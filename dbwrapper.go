package main

import "database/sql"

type DBWrapper interface {
	Wrap(db *sql.DB, name string, runInTX bool) DB
}

type SQLWrapper struct{}

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

/*
type SQLairWrapper struct{}

func (SQLairWrapper) Wrap(db sql.DB, name string, runInTx bool) DB {
	runner := SQLairPlainRunner
	if runInTx {
		runner = SQLairTxRunner
	}
	return &SQLairDB{
		DB:     sqlair.New(db),
		Name:   name,
		Runner: runner,
	}
}
*/
