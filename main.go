// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"database/sql"
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

// DBProvider can be one of SQLiteDB, DQLite1NodeDB, DQLite3NodeDB
type DBProvider interface {
	NewDB(name string) DB
}

// DB can be SQLDB or SQLairDB
type DB struct {
	sqldb    *sql.DB
	sqlairDB *sqlair.DB
}

type QueryProvider interface {
	Init()
	// GetQuery will assume the db is a sqlairDB or sqlDB depending on which
	// provider it is.
	GetQuery(db DB) Query
}

type PreparedSQLairQueryProvider struct{}

type SQLairQueryProvider struct{}

type SQLQueryProvider struct{}

type ModelProvider interface {
	Init() error
	NewModel(string) (Model, error)
}

type Model struct {
	DB                  DB
	Name                string
	ModelTableName      string
	ModelEventTableName string
	TxRunner            TxRunner
}

type ModelOperationDef struct {
	opName string
	op     ModelOperation
	freq   time.Duration
}

const (
	// Control the number of models created in the test and the frequency at
	// which they are added.
	AddModelRate         = 400
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
	modelCreationTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "model_creation_time",
		Buckets: []float64{
			0.001,
			0.01,
			0.1,
			1.0,
			10.0,
		},
	})

	modelTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "model_total",
		Help: "The total number of models",
	})

	modelAgentGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "model_agents",
	}, []string{"model"})

	modelAgentEventsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "model_agent_events",
	}, []string{"model"})

	// Valid values for queryProvider are:
	// - SQLQueryProvider
	// - SQLairQueryProvider
	// - PreparedSQLairQueryProvider
	queryProvider = SQLQueryProvider

	// Valid values for database are:
	// - SQLiteDB
	// - DQLite1NodeDB
	// - DQLite3NodeDB
	database = SQLiteDB

	// Set the operations to be performed per model and the frequency.
	perModelOperations = []ModelOperationDef{
		{
			opName: "model-init",
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
			op:     agentModelCount(modelAgentGauge),
			freq:   time.Second * 30,
		},
		{
			opName: "agent-events-count",
			op:     agentEventModelCount(modelAgentEventsGauge),
			freq:   time.Second * 30,
		},
	}
)

func main() {
	var err error
	if _, err = os.Stat("/tmp"); errors.Is(err, fs.ErrNotExist) {
		err = os.Mkdir("/tmp", 0750)
	}
	if err != nil {
		fmt.Printf("establishing tmp dir: %v\n", err)
		os.Exit(1)
	}

	provider := newModelProvider()

	if err := provider.Init(); err != nil {
		fmt.Printf("init model provider: %v\n", err)
		os.Exit(1)
	}

	t := tomb.Tomb{}

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

	t.Go(func() error {
		return server.ListenAndServe()
	})

	modelCh := modelRamper(&t, provider, DatabaseAddFrequency, AddModelRate, MaxNumberOfDatabases)
	modelSpawner(&t, modelCh, perModelOperations)

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

func modelSpawner(
	t *tomb.Tomb,
	ch <-chan Model,
	perModelOperations []ModelOperationDef,
) {
	startPerModelOperations := func(opTomb *tomb.Tomb, models []Model) {
		for _, model := range models {
			for _, op := range perModelOperations {
				RunModelOperation(opTomb, op.opName, op.freq, op.op, model)
			}
		}
	}

	t.Go(func() error {
		opTomb := tomb.Tomb{}
		allModels := []Model{}
		models := []Model{}

		for {
			select {
			case model, ok := <-ch:
				if !ok {
					ch = nil
					break
				}
				models = append(models, model)
			case <-t.Dying():
				opTomb.Kill(nil)
				return opTomb.Wait()
			case <-opTomb.Dead():
				err := opTomb.Wait()
				fmt.Printf("operation tomb is dead: %v", err)
				return err
			default:
				if len(models) == 0 {
					break
				}
				allModels = append(allModels, models...)
				models = []Model{}
				opTomb.Kill(nil)
				if opTomb.Alive() {
					if err := opTomb.Wait(); err != nil {
						fmt.Println("Tomb error", err)
						return err
					}
				}
				opTomb = tomb.Tomb{}
				fmt.Printf("Spawning model %d operations\n", AddModelRate)
				startPerModelOperations(&opTomb, allModels)
			}
		}
	})
}

func modelRamper(
	t *tomb.Tomb,
	provider ModelProvider,
	freq time.Duration,
	inc,
	max int,
) <-chan Model {
	newDBCh := make(chan Model, inc)
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
			dbs, makeErr := makeModels(provider, inc)
			numDBS += len(dbs)
			modelTotal.Add(float64(len(dbs)))

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

func makeModels(provider ModelProvider, x int) ([]Model, error) {
	models := make([]Model, 0, x)
	for i := 0; i < x; i++ {
		model, err := func() (Model, error) {
			timer := prometheus.NewTimer(modelCreationTime)
			defer timer.ObserveDuration()
			dbUUID := uuid.New()
			return provider.NewModel(dbUUID.String())
		}()

		if err != nil {
			return models, err
		}
		models = append(models, model)
	}

	return models, nil
}
