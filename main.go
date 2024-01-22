// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/tomb.v2"
)

type DBOperationDef struct {
	opName string
	op     DBOperation
	freq   time.Duration
}

type BenchmarkOpts struct {
	provider DBProvider
	wrapper  DBWrapper
	runInTx  bool
}

const (
	// Control the number of models created in the test and the frequency at
	// which they are added.
	AddDBRate            = 400
	DatabaseAddFrequency = time.Second
	MaxNumberOfDatabases = 400
)

const (
	schema = `
CREATE TABLE agent (
    uuid TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE INDEX idx_agent_model_name ON agent (model_name);
CREATE INDEX idx_agent_status ON agent (status);

CREATE TABLE agent_events (
 	agent_uuid TEXT NOT NULL,   
 	event TEXT NOT NULL,
 	CONSTRAINT fk_agent_uuid
    	FOREIGN KEY (agent_uuid)
        REFERENCES agent(uuid)
);

CREATE INDEX idx_agent_events_event ON agent_events (event);
`
)

var (
	dbCreationTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "db_creation_time",
		Buckets: []float64{
			0.001,
			0.01,
			0.1,
			1.0,
			10.0,
		},
	})

	dbTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "db_total",
		Help: "The total number of dbs",
	})

	dbAgentGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "db_agents",
	}, []string{"db"})

	dbAgentEventsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "db_agent_events",
	}, []string{"db"})

	// Set the operations to be performed per db and the frequency.
	perDBOperations = []DBOperationDef{
		{
			opName: "db-init",
			op:     seedModelAgents(60),
			freq:   time.Duration(0),
		},
		{
			opName: "agent-status-active",
			op:     updateModelAgentStatus(10, "active"),
			freq:   time.Second * 5,
		},
		{
			opName: "agent-status-inactive",
			op:     updateModelAgentStatus(10, "inactive"),
			freq:   time.Second * 8,
		},
		{
			opName: "agent-events",
			op:     generateAgentEvents(10),
			freq:   time.Second * 15,
		},
		{
			opName: "cull-agent-events",
			op:     cullAgentEvents(30),
			freq:   time.Second * 30,
		},
		{
			opName: "agents-count",
			op:     agentModelCount(dbAgentGauge),
			freq:   time.Second * 30,
		},
		{
			opName: "agent-events-count",
			op:     agentEventModelCount(dbAgentEventsGauge),
			freq:   time.Second * 30,
		},
	}
)

func start(t tomb.Tomb, opts *BenchmarkOpts) {
	dbCh := dbRamper(&t, opts, DatabaseAddFrequency, AddDBRate, MaxNumberOfDatabases)
	dbSpawner(&t, opts, dbCh, perDBOperations)
}

func dbSpawner(
	t *tomb.Tomb,
	opts *BenchmarkOpts,
	ch <-chan DB,
	perDBOperations []DBOperationDef,
) {
	startPerDBOperations := func(opTomb *tomb.Tomb, dbs []DB) {
		for _, op := range perDBOperations {
			opHistogram := promauto.NewHistogram(prometheus.HistogramOpts{
				Name: "db_operation_time",
				ConstLabels: prometheus.Labels{
					"wrapper":   opts.wrapper.Name(),
					"operation": op.opName,
				},
				Buckets: timeBucketSplits,
			})
			opErrCount := promauto.NewCounter(prometheus.CounterOpts{
				Name: "db_operation_errors",
				ConstLabels: prometheus.Labels{
					"wrapper":   opts.wrapper.Name(),
					"operation": op.opName,
				},
			})
			for _, db := range dbs {
				RunDBOperation(opTomb, op.opName, op.freq, opHistogram, opErrCount, op.op, db)
			}
		}
	}

	t.Go(func() error {
		opTomb := tomb.Tomb{}
		allDBs := []DB{}
		dbs := []DB{}

		for {
			select {
			case db, ok := <-ch:
				if !ok {
					ch = nil
					break
				}
				dbs = append(dbs, db)
			case <-t.Dying():
				opTomb.Kill(nil)
				return opTomb.Wait()
			case <-opTomb.Dead():
				err := opTomb.Wait()
				fmt.Printf("operation tomb is dead: %v", err)
				return err
			default:
				if len(dbs) == 0 {
					break
				}
				allDBs = append(allDBs, dbs...)
				dbs = []DB{}
				opTomb.Kill(nil)
				if opTomb.Alive() {
					if err := opTomb.Wait(); err != nil {
						fmt.Println("Tomb error", err)
						return err
					}
				}
				opTomb = tomb.Tomb{}
				fmt.Printf("Spawning model %d operations\n", AddDBRate)
				startPerDBOperations(&opTomb, allDBs)
			}
		}
	})
}

// creates DBs. DBs are sent down the channel once they are ready.
func dbRamper(
	t *tomb.Tomb,
	opts *BenchmarkOpts,
	freq time.Duration,
	inc,
	max int,
) <-chan DB {
	newDBCh := make(chan DB, inc)
	t.Go(func() error {
		defer close(newDBCh)
		ticker := time.NewTicker(freq)
		numDBS := 0
		for numDBS < max {
			select {
			case <-t.Dying():
				return nil
			case <-ticker.C:
			}
			dbs, makeErr := makeDBs(opts, inc)
			numDBS += len(dbs)
			dbTotal.Add(float64(len(dbs)))

			for _, db := range dbs {
				newDBCh <- db
			}

			if makeErr != nil {
				return makeErr
			}
		}
		return nil
	})
	return newDBCh
}

func makeDBs(opts *BenchmarkOpts, x int) ([]DB, error) {
	dbs := make([]DB, 0, x)
	for i := 0; i < x; i++ {
		db, err := func() (DB, error) {
			timer := prometheus.NewTimer(dbCreationTime)
			defer timer.ObserveDuration()
			dbUUID := uuid.New()
			sqldb, err := opts.provider.NewDB(dbUUID.String())
			return opts.wrapper.Wrap(sqldb, dbUUID.String(), opts.runInTx), err
		}()

		if err != nil {
			return dbs, err
		}
		dbs = append(dbs, db)
	}

	return dbs, nil
}

func main() {
	opts1 := BenchmarkOpts{
		// Valid values for provider are:
		// - NewSQLiteDBProvider()
		// - NewDQLite1NodeDBProvider()
		// - NewDQLite3NodeDBProvider()
		// provider: NewDQLite3NodeDBProvider(),
		provider: NewSQLiteDBProvider(),
		// Valid values for wrapper are:
		// - SQLWrapper{}
		// - SQLairWrapper{}
		// - PreparedSQLairWrapper{}
		wrapper: SQLWrapper{},
		// runInTx indicates if queries will be applied in transactions or not.
		runInTx: true,
	}
	opts2 := BenchmarkOpts{
		// Valid values for provider are:
		// - NewSQLiteDBProvider()
		// - NewDQLite1NodeDBProvider()
		// - NewDQLite3NodeDBProvider()
		// provider: NewDQLite3NodeDBProvider(),
		provider: NewSQLiteDBProvider(),
		// Valid values for wrapper are:
		// - SQLWrapper{}
		// - SQLairWrapper{}
		// - PreparedSQLairWrapper{}
		wrapper: SQLairWrapper{},
		// runInTx indicates if queries will be applied in transactions or not.
		runInTx: true,
	}

	var err error
	if _, err = os.Stat("/tmp"); errors.Is(err, fs.ErrNotExist) {
		err = os.Mkdir("/tmp", 0750)
	}
	if err != nil {
		fmt.Printf("establishing tmp dir: %v\n", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	server := http.Server{
		Addr:         ":3333",
		Handler:      mux,
		WriteTimeout: 50 * time.Second,
	}
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	t := tomb.Tomb{}

	t.Go(func() error {
		return server.ListenAndServe()
	})

	go start(t, &opts1)

	go start(t, &opts2)

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-t.Dead():
	case <-sig:
		t.Kill(nil)
		server.Close()
	}

	err = t.Wait()
	fmt.Println(err)
}
