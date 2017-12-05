package ethdb

import (

	"fmt"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
	"bytes"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "psql_eth"
)

type PGSQLDatabase struct {
	db *sqlx.DB
} 

func NewPostgreSQLDb() (*PGSQLDatabase, func(), error) {
	//func returned as closure to close database
	EnsureDatabaseExists()
	EnsureTableExists()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sqlx.Open("postgres", psqlInfo)
	if err != nil {
		return nil,nil, err
	}
	return &PGSQLDatabase{
		db: db,
	},func() {
		db.Close()
	},nil
}

//check if database exists, if not create it
func EnsureDatabaseExists() error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s sslmode=disable",
		host, port, user, password)
	db, err := sqlx.Open("postgres", psqlInfo)
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
	db, err := sqlx.Open("postgres", psqlInfo)
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

func (db *PGSQLDatabase) Put (key []byte, value []byte) error {
	//PostgreSQL doesn't support '\x00' so trimmed them out
	key = bytes.Trim(key, "\x00")
	value = bytes.Trim(value, "\x00")
	sqlStatement := `INSERT INTO psql_eth_table VALUES ($1)`
	_, err := db.db.Exec(sqlStatement, "{\""+string(key)+"\":\""+string(value)+"\"}")
	return err
}

func (db *PGSQLDatabase) Get (key []byte) ([]byte, error) {
	//PostgreSQL doesn't support '\x00' so trimmed them out
	key = bytes.Trim(key, "\x00")
	sqlStatement := `SELECT data->>$1 FROM psql_eth_table WHERE data ->> $1 is not null;`
	var data string
	err := db.db.QueryRowx(sqlStatement, string(key)).Scan(&data)
	if err != nil {
		return nil, err
	}
	return []byte(data), nil
}
