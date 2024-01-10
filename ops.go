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

/*
	func updateModelAgentStatus(agentUpdates int, status string) ModelOperation {
		return func(model Model, tx *sql.Tx) error {
			fmt.Println("Updating agent status")

			rows, err := tx.Query(`

SELECT uuid
FROM agent
WHERE model_name = ?
ORDER BY RANDOM()
LIMIT ?
`,

				model.Name, agentUpdates)

			if err != nil {
				return err
			}

			agentUUIDS := make([]any, 0, agentUpdates)

			for rows.Next() {
				var agentUUID string
				if err := rows.Scan(&agentUUID); err != nil {
					return err
				}
				agentUUIDS = append(agentUUIDS, agentUUID)
			}

			_, err = tx.Exec("UPDATE agent SET status = '"+status+"' WHERE uuid IN ("+SliceToPlaceholder(agentUUIDS)+")",
				agentUUIDS...)
			return err
		}
	}

	func generateAgentEvents(agents int) ModelOperation {
		return func(model Model, tx *sql.Tx) error {
			fmt.Println("Generating agent events")

			rows, err := tx.Query(`

SELECT uuid
FROM agent
WHERE model_name = ?
ORDER BY RANDOM()
LIMIT ?
`,

				model.Name, agents)

			if err != nil {
				return err
			}

			agentUUIDS := make([]any, 0, agents*2)
			insertStrings := make([]string, 0, agents)

			for rows.Next() {
				var agentUUID string
				if err := rows.Scan(&agentUUID); err != nil {
					return err
				}
				agentUUIDS = append(agentUUIDS, agentUUID, "event")
				insertStrings = append(insertStrings, "(?, ?)")
			}

			_, err = tx.Exec("INSERT INTO agent_events VALUES "+strings.Join(insertStrings, ","),
				agentUUIDS...)
			return err
		}
	}

	func cullAgentEvents(maxEvents int) ModelOperation {
		return func(model Model, tx *sql.Tx) error {
			fmt.Println("Culling agent events")

			// delete from agent_events where agent_uuid in (select agent_uuid from agent_events group by agent_uuid having count(*) > 1
			_, err := tx.Exec("DELETE FROM agent_events WHERE agent_uuid IN (SELECT agent_uuid from agent_events INNER JOIN agent ON agent.uuid = agent_events.agent_uuid WHERE agent.model_name = ? GROUP BY agent_uuid HAVING COUNT(*) > ?)",
				model.Name, maxEvents)
			return err
		}
	}

	func agentModelCount(gaugeVec *prometheus.GaugeVec) ModelOperation {
		return func(model Model, tx *sql.Tx) error {
			fmt.Println("Agent model count")

			rows, err := tx.Query(`

SELECT count(*)
FROM agent
WHERE model_name = ?
`, model.Name)

			if err != nil {
				return err
			}

			if !rows.Next() {
				return nil
			}

			var count int
			err = rows.Scan(&count)
			if err != nil {
				return err
			}

			gauge, err := gaugeVec.GetMetricWith(prometheus.Labels{
				"model": model.Name,
			})
			if err != nil {
				return err
			}

			gauge.Set(float64(count))
			return nil
		}
	}

	func agentEventModelCount(gaugeVec *prometheus.GaugeVec) ModelOperation {
		return func(model Model, tx *sql.Tx) error {
			fmt.Println("Agent event model count")

			rows, err := tx.Query(`
			SELECT count(*)
			FROM agent_events
			INNER JOIN agent ON agent.uuid = agent_events.agent_uuid
			WHERE agent.model_name = ?
			`, model.Name)

			if err != nil {
				return err
			}

			if !rows.Next() {
				return nil
			}

			var count int
			err = rows.Scan(&count)
			if err != nil {
				return err
			}

			gauge, err := gaugeVec.GetMetricWith(prometheus.Labels{
				"model": model.Name,
			})
			if err != nil {
				return err
			}

			gauge.Set(float64(count))
			return nil
		}
	}
*/
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
