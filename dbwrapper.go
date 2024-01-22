package main

import (
	"database/sql"

	"github.com/canonical/sqlair"
)

type DBWrapper interface {
	Wrap(db *sql.DB, name string, runInTX bool) DB
	Name() string
}

type SQLWrapper struct{}

func (SQLWrapper) Name() string {
	return "sql"
}

func (SQLWrapper) Wrap(db *sql.DB, name string, runInTX bool) DB {
	runner := SQLPlainRunner
	if runInTX {
		runner = SQLTxRunner
	}
	return &SQLDB{
		db:     db,
		name:   name,
		runner: runner,
	}
}

type SQLairWrapper struct{}

func (SQLairWrapper) Name() string {
	return "sqlair"
}

func (SQLairWrapper) Wrap(db *sql.DB, name string, runInTx bool) DB {
	runner := SQLairPlainRunner
	if runInTx {
		runner = SQLairTxRunner
	}
	return &SQLairDB{
		db:     sqlair.NewDB(db),
		name:   name,
		runner: runner,
	}
}
