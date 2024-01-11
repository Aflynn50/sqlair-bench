// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gopkg.in/tomb.v2"
)

type DBOperation func(DB) error

func seedModelAgents(numAgents int) DBOperation {
	return func(db DB) error {
		fmt.Println("Seeding agents")

		agentUUIDS := make([]any, 0, numAgents*3)

		for i := 0; i < numAgents; i++ {
			uuid, err := uuid.NewUUID()
			if err != nil {
				return err
			}
			agentUUIDS = append(agentUUIDS, uuid.String(), db.Name(), "inactive")
		}
		return db.SeedModelAgents(agentUUIDS)
	}
}

func updateModelAgentStatus(agentUpdates int, status string) DBOperation {
	return func(db DB) error {
		fmt.Println("Updating agent status")
		return db.UpdateModelAgentStatus(agentUpdates, status)
	}
}

func generateAgentEvents(agents int) DBOperation {
	return func(db DB) error {
		fmt.Println("Generating agent events")
		return db.GenerateAgentEvents(agents)
	}
}

func cullAgentEvents(maxEvents int) DBOperation {
	return func(db DB) error {
		fmt.Println("Culling agent events")
		return db.CullAgentEvents(maxEvents)
	}
}

func agentModelCount(gaugeVec *prometheus.GaugeVec) DBOperation {
	return func(db DB) error {
		fmt.Println("Agent model count")

		count, err := db.AgentModelCount()
		if err != nil || count == 0 {
			return err
		}

		gauge, err := gaugeVec.GetMetricWith(prometheus.Labels{
			"db": db.Name(),
		})
		if err != nil {
			return err
		}

		gauge.Set(float64(count))
		return nil
	}
}

func agentEventModelCount(gaugeVec *prometheus.GaugeVec) DBOperation {
	return func(db DB) error {
		fmt.Println("Agent event model count")

		count, err := db.AgentEventModelCount()
		if err != nil || count == 0 {
			return err
		}

		gauge, err := gaugeVec.GetMetricWith(prometheus.Labels{
			"db": db.Name(),
		})
		if err != nil {
			return err
		}

		gauge.Set(float64(count))
		return nil
	}
}

var (
	timeBucketSplits = []float64{
		0.001,
		0.01,
		0.02,
		0.03,
		0.04,
		0.05,
		0.06,
		0.07,
		0.08,
		0.09,
		0.1,
		1.0,
		10.0,
	}
)

func runDBOp(
	op DBOperation,
	db DB,
	obs prometheus.Observer,
) error {
	timer := prometheus.NewTimer(obs)
	defer timer.ObserveDuration()
	return op(db)
}

func RunDBOperation(
	t *tomb.Tomb,
	opName string,
	freq time.Duration,
	op DBOperation,
	db DB,
) {
	t.Go(func() error {
		opHistogram := promauto.NewHistogram(prometheus.HistogramOpts{
			Name: "db_operation_time",
			ConstLabels: prometheus.Labels{
				"db":        db.Name(),
				"operation": opName,
			},
			Buckets: timeBucketSplits,
		})
		opErrCount := promauto.NewCounter(prometheus.CounterOpts{
			Name: "db_operation_errors",
			ConstLabels: prometheus.Labels{
				"db":        db.Name(),
				"operation": opName,
			},
		})

		if freq == time.Duration(0) {
			if err := runDBOp(op, db, opHistogram); err != nil {
				panic(err)
				opErrCount.Inc()
				fmt.Printf("operation %s died for db %s: %v\n", opName, db.Name(), err)
			}
			return nil
		}

		initalDelay := time.Duration(rand.Int63n(int64(freq)))
		time.Sleep(initalDelay)

		ticker := time.NewTicker(freq)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := runDBOp(op, db, opHistogram); err != nil {
					opErrCount.Inc()
					fmt.Printf("operation %s died for db %s: %v\n", opName, db.Name(), err)
				}
			case <-t.Dying():
				return nil
			}
		}
	})
}
