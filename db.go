package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/canonical/sqlair"
	"github.com/juju/collections/transform"
)

type DB interface {
	Name() string
	SeedModelAgents(agentUUIDs []any) error
	UpdateModelAgentStatus(agentUpdates int, status string) error
	GenerateAgentEvents(agents int) error
	CullAgentEvents(maxEvents int) error
	AgentModelCount() (int, error)
	AgentEventModelCount() (int, error)
}

// SQLQuerySubstate can be a transaction or a db.
type SQLQuerySubstrate interface {
	Query(string, ...any) (*sql.Rows, error)
	Exec(string, ...any) (sql.Result, error)
}

type SQLDB struct {
	db     *sql.DB
	name   string
	runner SQLRunner
}

func (db *SQLDB) Name() string {
	return db.name
}

func (db *SQLDB) SeedModelAgents(agentUUIDs []any) error {
	return db.runner(db.db, func(qs SQLQuerySubstrate) error {
		var insertStrings []string
		for i := 0; i < len(agentUUIDs)/3; i++ {
			insertStrings = append(insertStrings, "(?, ?, ?)")
		}
		_, err := qs.Exec("INSERT INTO agent VALUES "+strings.Join(insertStrings, ","),
			agentUUIDs...)
		return err
	})
}

func (db *SQLDB) UpdateModelAgentStatus(agentUpdates int, status string) error {
	return db.runner(db.db, func(qs SQLQuerySubstrate) error {
		rows, err := qs.Query(`
			SELECT uuid
			FROM agent
			WHERE model_name = ?
			ORDER BY RANDOM()
			LIMIT ?
			`,
			db.Name(),
			agentUpdates,
		)
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

		_, err = qs.Exec("UPDATE agent SET status = '"+status+"' WHERE uuid IN ("+SliceToPlaceholder(agentUUIDS)+")",
			agentUUIDS...)
		return err
	})
}

func (db *SQLDB) GenerateAgentEvents(agents int) error {
	return db.runner(db.db, func(qs SQLQuerySubstrate) error {
		rows, err := qs.Query(`
			SELECT uuid
			FROM agent
			WHERE model_name = ?
			ORDER BY RANDOM()
			LIMIT ?
			`, db.Name(),
			agents,
		)
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

		_, err = qs.Exec("INSERT INTO agent_events VALUES "+strings.Join(insertStrings, ","),
			agentUUIDS...)
		return err
	})
}

func (db *SQLDB) CullAgentEvents(maxEvents int) error {
	return db.runner(db.db, func(qs SQLQuerySubstrate) error {
		// delete from agent_events where agent_uuid in (select agent_uuid from agent_events group by agent_uuid having count(*) > 1
		_, err := qs.Exec("DELETE FROM agent_events WHERE agent_uuid IN (SELECT agent_uuid from agent_events INNER JOIN agent ON agent.uuid = agent_events.agent_uuid WHERE agent.model_name = ? GROUP BY agent_uuid HAVING COUNT(*) > ?)",
			db.Name(), maxEvents)
		return err
	})
}

func (db *SQLDB) AgentModelCount() (int, error) {
	var count int
	err := db.runner(db.db, func(qs SQLQuerySubstrate) error {
		rows, err := qs.Query(`

		SELECT count(*)
		FROM agent
		WHERE model_name = ?
		`, db.Name())

		if err != nil {
			return err
		}

		if !rows.Next() {
			return nil
		}

		err = rows.Scan(&count)
		if err != nil {
			return err
		}
		return nil
	})
	return count, err
}

func (db *SQLDB) AgentEventModelCount() (int, error) {
	var count int
	err := db.runner(db.db, func(qs SQLQuerySubstrate) error {
		rows, err := qs.Query(`
		SELECT count(*)
		FROM agent_events
		INNER JOIN agent ON agent.uuid = agent_events.agent_uuid
		WHERE agent.model_name = ?
		`, db.Name())

		if err != nil {
			return err
		}

		if !rows.Next() {
			return nil
		}

		err = rows.Scan(&count)
		if err != nil {
			return err
		}

		return nil
	})
	return count, err
}

func SliceToPlaceholder[T any](in []T) string {
	return strings.Join(transform.Slice(in, func(item T) string { return "?" }), ",")
}

// SQLairQuerySubstate can be a transaction or a db.
type SQLairQuerySubstrate interface {
	Query(context.Context, *sqlair.Statement, ...any) *sqlair.Query
}

type SQLairDB struct {
	db     *sqlair.DB
	name   string
	runner SQLairRunner
}

func (db *SQLairDB) Name() string {
	return db.name
}

func (db *SQLairDB) SeedModelAgents(agentUUIDs []any) error {
	return db.runner(db.db, func(qs SQLairQuerySubstrate) error {
		m := sqlair.M{}
		var insertStrings []string
		for i := 0; i < len(agentUUIDs)/3; i++ {
			s := fmt.Sprintf("($M.id%d, $M.id%d, $M.id%d)", i*3, i*3+1, i*3+2)
			insertStrings = append(insertStrings, s)
			m["id"+strconv.Itoa(i*3)] = agentUUIDs[i*3]
			m["id"+strconv.Itoa(i*3+1)] = agentUUIDs[i*3+1]
			m["id"+strconv.Itoa(i*3+2)] = agentUUIDs[i*3+2]
		}
		stmt, err := sqlair.Prepare("INSERT INTO agent VALUES "+strings.Join(insertStrings, ","), sqlair.M{})
		if err != nil {
			return err
		}
		err = qs.Query(nil, stmt, m).Run()
		if err != nil {
			return err
		}
		return nil
	})
}

func (db *SQLairDB) UpdateModelAgentStatus(agentUpdates int, status string) error {
	return db.runner(db.db, func(qs SQLairQuerySubstrate) error {
		var selectUUID = sqlair.MustPrepare(`SELECT &M.uuid FROM agent WHERE model_name = $M.name ORDER BY RANDOM() LIMIT $M.agentUpdates`, sqlair.M{})
		ms := []sqlair.M{}
		err := qs.Query(nil, selectUUID, sqlair.M{"agentUpdates": agentUpdates, "name": db.Name()}).GetAll(&ms)
		if err != nil {
			return err
		}

		createTable := sqlair.MustPrepare("CREATE TEMPORARY TABLE temp_agent_uuids ( uuid INT )")
		err = qs.Query(nil, createTable).Run()
		if err != nil {
			return nil
		}

		insertUUID := sqlair.MustPrepare("INSERT INTO temp_agent_uuids VALUES ($M.uuid)", sqlair.M{})
		for _, m := range ms {
			// INSERT m["uuid"] into temp table.
			err = qs.Query(nil, insertUUID, m).Run()
			if err != nil {
				return nil
			}
		}

		updateStatus := sqlair.MustPrepare("UPDATE agent SET status = $M.status WHERE uuid IN (SELECT uuid FROM temp_agent_uuids)", sqlair.M{})
		err = qs.Query(nil, updateStatus, sqlair.M{"status": status}).Run()
		if err != nil {
			return err
		}

		dropTable := sqlair.MustPrepare("DROP TABLE temp.temp_agent_uuids")
		return qs.Query(nil, dropTable).Run()
	})
}

func (db *SQLairDB) GenerateAgentEvents(agents int) error {
	return db.runner(db.db, func(qs SQLairQuerySubstrate) error {
		var insertAgentStrings = sqlair.MustPrepare("INSERT INTO agent_events VALUES ($M.uuid, $M.event)", sqlair.M{})
		var selectUUID = sqlair.MustPrepare(`SELECT &M.uuid FROM agent WHERE model_name = $M.name ORDER BY RANDOM() LIMIT $M.agentUpdates`, sqlair.M{})

		ms := []sqlair.M{}
		err := qs.Query(nil, selectUUID, sqlair.M{"agentUpdates": agents, "name": db.Name()}).GetAll(&ms)
		if err != nil {
			return err
		}

		for _, m := range ms {
			m["event"] = "event"
			err = qs.Query(nil, insertAgentStrings, m).Run()
			if err != nil {
				return err
			}
		}

		return err
	})
}

func (db *SQLairDB) CullAgentEvents(maxEvents int) error {
	return db.runner(db.db, func(qs SQLairQuerySubstrate) error {
		cullAgents := sqlair.MustPrepare("DELETE FROM agent_events WHERE agent_uuid IN (SELECT agent_uuid from agent_events INNER JOIN agent ON agent.uuid = agent_events.agent_uuid WHERE agent.model_name = $M.name GROUP BY agent_uuid HAVING COUNT(*) > $M.maxEvents)", sqlair.M{})
		err := qs.Query(nil, cullAgents, sqlair.M{"maxEvents": maxEvents, "name": db.Name()}).Run()
		return err
	})
}

func (db *SQLairDB) AgentModelCount() (int, error) {
	var count int
	err := db.runner(db.db, func(qs SQLairQuerySubstrate) error {
		getCount := sqlair.MustPrepare(`
			SELECT &M.c FROM (
			SELECT count(*) AS c
			FROM agent
			WHERE model_name = $M.name)
		`, sqlair.M{})
		m := sqlair.M{}
		err := qs.Query(nil, getCount, sqlair.M{"name": db.Name()}).Get(m)
		if errors.Is(err, sqlair.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		count = int(m["c"].(int64))
		return nil
	})
	return count, err
}

func (db *SQLairDB) AgentEventModelCount() (int, error) {
	var count int
	err := db.runner(db.db, func(qs SQLairQuerySubstrate) error {
		eventModelCount := sqlair.MustPrepare(`
			SELECT &M.c FROM (
			SELECT count(*) AS c
			FROM agent_events
			INNER JOIN agent ON agent.uuid = agent_events.agent_uuid
			WHERE agent.model_name = $M.name)
			`, sqlair.M{})

		m := sqlair.M{}
		err := qs.Query(nil, eventModelCount, sqlair.M{"name": db.Name()}).Get(m)
		if errors.Is(err, sqlair.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		count = int(m["c"].(int64))
		return nil
	})
	return count, err
}

type SQLairPreparedDB struct {
	DB     sqlair.DB
	Name   string
	Runner SQLairRunner
}
