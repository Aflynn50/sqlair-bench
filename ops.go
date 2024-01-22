// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
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
		0.0001,
		0.0002,
		0.0003,
		0.0004,
		0.0005,
		0.0006,
		0.0007,
		0.0008,
		0.0009,
		0.001,
		0.01,
		0.1,
		1.0,
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
	opHistogram prometheus.Histogram,
	opErrCount prometheus.Counter,
	op DBOperation,
	db DB,
) {
	t.Go(func() error {

		if freq == time.Duration(0) {
			if err := runDBOp(op, db, opHistogram); err != nil {
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
