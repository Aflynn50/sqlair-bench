// Copyright 2023 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/canonical/go-dqlite/app"
	_ "github.com/mattn/go-sqlite3"
)

type DBProvider interface {
	NewDB(name string) (*sql.DB, error)
}

type SQLiteDBProvider struct {
}

func NewSQLiteDBProvider() *SQLiteDBProvider {
	return &SQLiteDBProvider{}
}

func (*SQLiteDBProvider) NewDB(name string) (*sql.DB, error) {

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

type DQLite1NodeDBProvider struct {
	a *app.App
}

func NewDQLite1NodeDBProvider() *DQLite1NodeDBProvider {
	appDir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}

	app, err := app.New(appDir)
	if err != nil {
		panic(err)
	}
	if err := app.Ready(context.Background()); err != nil {
		panic(err)
	}

	return &DQLite1NodeDBProvider{a: app}
}

func (dbp *DQLite1NodeDBProvider) NewDB(name string) (*sql.DB, error) {
	db, err := dbp.a.Open(context.Background(), name)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(schema); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return db, tx.Commit()
}

type DQLite3NodeDBProvider struct {
	a *app.App
}

func NewDQLite3NodeDBProvider() *DQLite3NodeDBProvider {
	addrs := []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"}
	appDirs := make([]string, len(addrs))
	for i := 0; i < 3; i++ {
		appDir, err := os.MkdirTemp("", "")
		if err != nil {
			panic(err)
		}
		appDirs[i] = appDir
	}

	node1, err := app.New(appDirs[0], app.WithAddress(addrs[0]))
	if err != nil {
		panic(err)
	}
	if err := node1.Ready(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(node1.Address())
	node2, err := app.New(appDirs[1], app.WithAddress(addrs[1]), app.WithCluster(addrs[0:1]))
	if err != nil {
		panic(err)
	}
	if err := node2.Ready(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(node2.Address())
	node3, err := app.New(appDirs[2], app.WithAddress(addrs[2]), app.WithCluster(addrs[0:2]))
	if err != nil {
		panic(err)
	}
	/*
		// This hangs forever.
		fmt.Println("waiting for 3")
		if err := node3.Ready(context.Background()); err != nil {
			panic(err)
		}
	*/
	fmt.Println(node3.Address())

	fmt.Printf("1: %d, 2: %d, 3: %d\n", node1.ID(), node2.ID(), node3.ID())

	return &DQLite3NodeDBProvider{a: node1}
}

func (dbp *DQLite3NodeDBProvider) NewDB(name string) (*sql.DB, error) {
	db, err := dbp.a.Open(context.Background(), name)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(schema); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return db, tx.Commit()
}
