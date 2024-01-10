package main

import (
	"database/sql"

	"github.com/canonical/sqlair"
)

// The runner can be global
type SQLRunner func(*sql.DB, func(SQLQuerySubstrate) error) error

var SQLTxRunner = func(db *sql.DB, fn func(SQLQuerySubstrate) error) error {
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

var SQLPlainRunner = func(db *sql.DB, fn func(qs SQLQuerySubstrate) error) error {
	err := fn(db)
	if err != nil {
		return err
	}
	return nil
}

type SQLairRunner func(*sqlair.DB, func(SQLairQuerySubstrate) error) error

var SQLairTxRunner = func(db *sqlair.DB, fn func(SQLairQuerySubstrate) error) error {
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

var SQLairPlainRunner = func(db *sqlair.DB, fn func(SQLairQuerySubstrate) error) error {
	err := fn(db)
	if err != nil {
		return err
	}
	return nil
}
