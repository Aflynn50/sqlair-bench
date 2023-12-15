// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"context"
	"database/sql"
	"os"

	"github.com/canonical/sqlair"
)

type SQLiteDBModelProvider struct {
	dbDir   string
	dbCount uint64
}

type SQLiteTableModelProvider struct {
	db *sql.DB
}

func NewSQLiteDBModelProvider() ModelProvider {
	return &SQLiteDBModelProvider{}
}

func NewSQLiteTableModelProvider() ModelProvider {
	return &SQLiteTableModelProvider{}
}

func (d *SQLiteDBModelProvider) Init() error {
	dbDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	d.dbDir = dbDir
	return nil
}

func (d *SQLiteTableModelProvider) Init() error {
	db, err := db.Open(context.Background(), "file:test.db?cache=shared&mode=memory")
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(schema); err != nil {
		_ = tx.Rollback()
		return err
	}

	d.a = app
	d.db = db
	return tx.Commit()
}

func (d *SQLiteDBModelProvider) NewModel(name string) (Model, error) {
	sqldb, err := d.a.Open(context.Background(), name)
	if err != nil {
		return Model{}, err
	}

	db := sqlair.NewDB(sqldb)

	tx, err := db.Begin(nil, nil)
	if err != nil {
		return Model{}, err
	}

	if _, err := tx.Exec(schema); err != nil {
		_ = tx.Rollback()
		return Model{}, err
	}

	return Model{
		DB:                  db,
		Name:                name,
		ModelTableName:      "agent",
		ModelEventTableName: "agent_events",
		TxRunner:            transactionRunner(db),
	}, tx.Commit()
}

func (d *SQLiteDBModelShardProvider) NewModel(name string) (Model, error) {
	shard, err := d.getShard()
	if err != nil {
		return Model{}, err
	}

	db, err := shard.app.Open(context.Background(), name)
	if err != nil {
		return Model{}, err
	}
	shard.dbs++

	tx, err := db.Begin()
	if err != nil {
		return Model{}, err
	}
	if _, err := tx.Exec(schema); err != nil {
		_ = tx.Rollback()
		return Model{}, err
	}

	return Model{
		DB:                  db,
		Name:                name,
		ModelTableName:      "agent",
		ModelEventTableName: "agent_events",
		TxRunner:            transactionRunner(db),
	}, tx.Commit()
}

func (d *SQLiteTableModelProvider) NewModel(name string) (Model, error) {
	return Model{
		DB:                  d.db,
		Name:                name,
		ModelTableName:      "agent",
		ModelEventTableName: "agent_events",
		TxRunner:            transactionRunner(d.db),
	}, nil
}
