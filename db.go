package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/canonical/sqlair"
	"github.com/juju/collections/transform"
)

type DB interface {
	Name() string
	SeedModelAgents([]any) error
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

func SliceToPlaceholder[T any](in []T) string {
	return strings.Join(transform.Slice(in, func(item T) string { return "?" }), ",")
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
			s := fmt.Sprintf("($M.%d, $M.%d, $M.%d)", i*3, i*3+1, i*3+2)
			insertStrings = append(insertStrings, s)
			m[strconv.Itoa(i*3)] = agentUUIDs[i*3]
			m[strconv.Itoa(i*3+1)] = agentUUIDs[i*3+1]
			m[strconv.Itoa(i*3+2)] = agentUUIDs[i*3+2]
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

type SQLairPreparedDB struct {
	DB     sqlair.DB
	Name   string
	Runner SQLairRunner
}
