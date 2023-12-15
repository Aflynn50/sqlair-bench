package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/canonical/sqlair"
)

func main() {
	fmt.Println("vim-go")
}

type SQLQuerySubstrate interface {
	Query(string, ...any)
	Exec(string, ...any)
}

type QP interface {
	Init(SQLRunner)
	// GetQuery will assume the db is a sqlairDB or sqlDB depending on which
	// provider it is.
	SeedModelAgents(DB, []string) error
}

// The runner can be global
type SQLRunner func(*sql.DB, func(SQLQuerySubstrate) error) error

var TxRunner = func(db *sql.DB, fn func(qs SQLQuerySubstrate) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

var PlainRunner = func(db *sql.DB, fn func(qs SQLQuerySubstrate) error) error {
	err = fn(db)
	if err != nil {
		return err
	}
	return nil
}

type SQLQP struct {
	sqlRunner SQLRunner // Set this when you create it.
}

func (sqp *SQLQP) Init() {}

func (sqp *SQLQP) SeedModelAgents(db DB, agentUUIDs []string) error {
	db := db.sqldb
	err := sqlRunner(db, func(qs SQLQuerySubstrate) error {
		var insertStrings []string
		for i = 0; i < agentUUIDs/3; i++ {
			insertStrings = append(insertStrings, "(?, ?, ?)")
		}
		_, err := qs.Exec("INSERT INTO agent VALUES "+strings.Join(insertStrings, ","),
			agentUUIDS...)
		return err
	})
	return err
}

type SQLairRunner func(*sqlair.DB, func(SQLairQuerySubstrate) error) error

var TxRunner = func(db *sqlair.DB, fn func(qs SQLairQuerySubstrate) error) error {
	tx, err := db.Begin(nil, nil)
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

var PlainRunner = func(db *sqlair.DB, fn func(qs SQLairQuerySubstrate) error) error {
	err = fn(db)
	if err != nil {
		return err
	}
	return nil
}

// SQLair Query Provider
type SQLairQP struct {
	sqlairRunner SQLairRunner
}

func (sqp *SQLairQP) Init() {}

func (sqp *SQLairQP) SeedModelAgents(db DB, agentUUIDs []string) error {
	db := db.sqldb
	m := sqlair.M{}
	err := sqlairRunner(db, func(qs SQLairQuerySubstrate) error {
		var insertStrings []string
		for i = 0; i < agentUUIDs/3; i++ {
			s := fmt.Sprintf("($M.%d, $M.%d, $M.%d)", i*3, i*3+1, i*3+2)
			insertStrings = append(insertStrings, s)
			m[strconv.Atoi(i*3)] = agentUUIds[i*3]
			m[strconv.Atoi(i*3+1)] = agentUUIds[i*3+1]
			m[strconv.Atoi(i*3+2)] = agentUUIds[i*3+2]
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
	return err
}

// Prepared SQLair Query Provider
type PreparedSQLairQP struct {
	sqlairRunner SQLairRunner
}

func (sqp *PreparedSQLairQP) Init() {}

func (sqp *PreparedSQLairQP) SeedModelAgents(db DB, agentUUIDs []string) error {
	db := db.sqldb
	m := sqlair.M{}
	err := sqlairRunner(db, func(qs SQLairQuerySubstrate) error {
		var insertStrings []string
		for i = 0; i < agentUUIDs/3; i++ {
			s := fmt.Sprintf("($M.%d, $M.%d, $M.%d)", i*3, i*3+1, i*3+2)
			insertStrings = append(insertStrings, s)
			m[strconv.Atoi(i*3)] = agentUUIds[i*3]
			m[strconv.Atoi(i*3+1)] = agentUUIds[i*3+1]
			m[strconv.Atoi(i*3+2)] = agentUUIds[i*3+2]
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
	return err
}
