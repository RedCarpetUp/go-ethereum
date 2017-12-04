package ethdb

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "psql_eth"
)


//check if database exists, if not create it
func EnsureDatabaseExists() error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s sslmode=disable",
		host, port, user, password)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return fmt.Errorf("mysql: could not get a connection: %v", err)
	}

	defer db.Close()

	err = db.Ping()
	if err!= nil {
		return err
	}

	//database exists if res.RowsAffected() returns 1, does not exists if returns 0
	res, err := db.Exec("SELECT 1 FROM pg_database WHERE datname = 'psql_eth';")
	if err!= nil {
		return err
	}
	exists,err := res.RowsAffected()
	if err!= nil {
		return err
	}
	if exists==0 {
		db.Exec("CREATE DATABASE psql_eth")
		return nil
	} else {
		return nil
	}

}

//check if table exists, if not create it
func EnsureTableExists() error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return fmt.Errorf("mysql: could not get a connection: %v", err)
	}

	defer db.Close()

	err = db.Ping()
	if err!= nil {
		return err
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS psql_eth_table(data jsonb);")
	if err!= nil {
		return err
	}
	return nil
}

