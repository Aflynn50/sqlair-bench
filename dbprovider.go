// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"database/sql"
	"os"
)

type DBProvider interface {
	NewDB(name string) (*sql.DB, error)
}

type SQLiteDBProvider struct {
	dbDir   string
	dbCount uint64
}

func NewSQLiteDBProvider() *SQLiteDBProvider {
	dbDir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	return &SQLiteDBProvider{dbDir: dbDir, dbCount: 0}
}

func (sbp *SQLiteDBProvider) NewDB(name string) (*sql.DB, error) {

	sqldb, err := sql.Open("sqlite3", "file:"+name+".db?cache=shared&mode=memory")
	if err != nil {
		return nil, err
	}

	tx, err := sqldb.Begin()
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(schema); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	return sqldb, tx.Commit()
}
