package ethdb

import (

	"fmt"

	_ "github.com/lib/pq"
	"database/sql"
	"encoding/base64"
)

const (
	host     = "35.200.194.52"
	port     = 5432
	user     = "postgres"
	password = "vvkaExD1rCerkG4F"
	dbname   = "psql_eth"
)

type PgSQLDatabase struct {
	db *sql.DB
} 

func NewPostgreSQLDb() (*PgSQLDatabase, error) {
	//func returned as closure to close database
	EnsureDatabaseExists()
	EnsureTableExists()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	return &PgSQLDatabase{
		db: db,
	},nil
}

//check if database exists, if not create it
func EnsureDatabaseExists(){
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s sslmode=disable",
		host, port, user, password)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic("could not get a connection:"+err.Error())
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic("could not get a connection:"+err.Error())
	}

	//database exists if res.RowsAffected() returns 1, does not exists if returns 0
	res, err := db.Exec("SELECT 1 FROM pg_database WHERE datname = 'psql_eth';")
	if err != nil {
		panic(err.Error())
	}
	exists,err := res.RowsAffected()
	if err != nil {
		panic(err.Error())
	}
	if exists==0 {
		db.Exec("CREATE DATABASE psql_eth")
	}

}

//check if table exists, if not create it
func EnsureTableExists(){
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic("Could not get a connection:"+err.Error())
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic("could not get a connection:"+err.Error())
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS psql_eth_table(data jsonb);")
	if err != nil {
		panic("Create table failed :"+err.Error())
	}
}

func (db *PgSQLDatabase) Put (key []byte, value []byte) error {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)
	hasKey, err := db.Has(key)
	if err!= nil {
		return err
	}
	if hasKey {
		sqlStatement := `UPDATE psql_eth_table SET data = $1
where data ->> $2 is not null;`
		_, err := db.db.Exec(sqlStatement,
			"{\""+keyBase64+"\":\""+valueBase64+"\"}", keyBase64)
		return err
	}else {
		sqlStatement := `INSERT INTO psql_eth_table VALUES ($1)`
		_, err := db.db.Exec(sqlStatement,
			"{\""+keyBase64+"\":\""+valueBase64+"\"}")
		return err
	}
}

func (db *PgSQLDatabase) Get (key []byte) ([]byte, error) {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := `SELECT data->>$1 FROM psql_eth_table
WHERE data ->> $1 is not null;`
	var data string
	err := db.db.QueryRow(sqlStatement, keyBase64).Scan(&data)
	if err != nil {
		return nil, err
	}
	value, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (db *PgSQLDatabase) Has (key []byte) (bool, error){
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := `SELECT count(data->>$1) FROM psql_eth_table
WHERE data ->> $1 is not null;`
	var numRows int
	hasKey := false
	err := db.db.QueryRow(sqlStatement, keyBase64).Scan(&numRows)
	if numRows!=0{
		hasKey = true
	}
	return hasKey, err

}

func (db *PgSQLDatabase) Delete(key []byte) error{
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := `DELETE FROM psql_eth_table WHERE data ->> $1 is not null;`
	_, err := db.db.Exec(sqlStatement,keyBase64)
	return err
}

func (db *PgSQLDatabase) Close() {
	err := db.db.Close()
	if err != nil{
		panic(err)
	}
}

func (db *PgSQLDatabase) NewBatch() Batch {
	tx, err := db.db.Begin()
	if err != nil{
		panic(err)
	}
	return &psqlBatch{tx:tx}
}


type psqlBatch struct {
	db   *PgSQLDatabase
	tx   *sql.Tx
	size int
}

func (b *psqlBatch) Put(key []byte, value []byte) error  {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)
	hasKey, err := b.db.Has(key)
	if err!= nil {
		return err
	}
	if hasKey {
		sqlStatement := `UPDATE psql_eth_table SET data = $1
where data ->> $2 is not null;`
		_, err := b.tx.Exec(sqlStatement,
			"{\""+keyBase64+"\":\""+valueBase64+"\"}", keyBase64)
		b.size += len(value)
		return err
	}else {
		sqlStatement := `INSERT INTO psql_eth_table VALUES ($1)`
		_, err := b.tx.Exec(sqlStatement,
			"{\""+keyBase64+"\":\""+valueBase64+"\"}")
		b.size += len(value)
		return err
	}
}

func (b *psqlBatch) Write() error  {
	err := b.tx.Commit()
	return err
}

func (b *psqlBatch) ValueSize() int  {
	return b.size
}

