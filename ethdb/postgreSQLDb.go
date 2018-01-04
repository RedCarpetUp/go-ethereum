package ethdb

//TODO database not SQL injection secure

import (

	"fmt"

	_ "github.com/lib/pq"
	"database/sql"
	"encoding/base64"
	"github.com/ethereum/go-ethereum/log"
	"strings"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strconv"
	"github.com/lib/pq"
	"time"
	//"runtime"
)

const (
	host     = "35.200.194.52"
	port     = 5432
	user     = "postgres"
	password = "vvkaExD1rCerkG4F"
	dbname   = "psql_eth"
)

type PgSQLDatabase struct {
	db         *sql.DB
	tableName  string
	stmtHas    *sql.Stmt
	stmtGet    *sql.Stmt
	stmtPut    *sql.Stmt
	stmtUpdate *sql.Stmt

} 

func NewPostgreSQLDb(tableName string) (*PgSQLDatabase, error) {
	now := time.Now()
	//this removes '/', '-' from string
	tableName = strings.Replace(tableName,"/","",-1)
	tableName = strings.Replace(tableName,"-","",-1)
	tableName = strings.Replace(tableName,".","",-1)
	EnsureDatabaseExists()
	EnsureTableExists(tableName)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}


	stmtHas, err := db.Prepare(`SELECT count(*) FROM `+tableName+` WHERE key = $1;`)
	if err != nil{
		log.Error(err.Error())
	}
	stmtGet, err := db.Prepare(`SELECT value FROM `+tableName+` WHERE key = $1;`)
	if err != nil{
		log.Error(err.Error())
	}
	stmtPut, err := db.Prepare(`INSERT INTO `+tableName+` VALUES ($1, $2);`)
	if err != nil{
		log.Error(err.Error())
	}
	stmtUpdate, err := db.Prepare(`UPDATE `+tableName+` SET value = $2 where key = $1;`)
	if err != nil{
		log.Error(err.Error())
	}

	elapsed := time.Since(now)
	log.Info("time newdb = "+elapsed.String())

	return &PgSQLDatabase{
		db: db,
		tableName:tableName,
		stmtHas:stmtHas,
		stmtGet: stmtGet,
		stmtPut:stmtPut,
		stmtUpdate: stmtUpdate,
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
		panic(err)
	}
	exists,err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	if exists==0 {
		_, err := db.Exec("CREATE DATABASE psql_eth")
		if err != nil{
			panic(err)
		}
		log.Info("created db")
	}

}

//check if table exists, if not create it
func EnsureTableExists(tableName string){
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

	//_, err = db.Exec(`CREATE TABLE IF NOT EXISTS `+tableName+`(data jsonb)`)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS `+tableName+
		`(key TEXT, value TEXT)`)
	if err != nil {
		panic("Create table failed :"+err.Error())
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS `+tableName+`_index ON `+tableName+
		`(key)`)
	if err != nil {
		panic("Create index failed :"+err.Error())
	}
}

func (db *PgSQLDatabase) Put (key []byte, value []byte) error {
	now := time.Now()
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)
	fmt.Printf("-----")
	hasKey, err := db.Has(key)
	if err!= nil {
		return err
	}
	if hasKey {
		_, err := db.stmtUpdate.Exec(keyBase64,valueBase64)
		elapsed := time.Since(now)
		log.Info("time put = "+elapsed.String())
		return err
	} else {
		_, err := db.stmtPut.Exec(keyBase64,valueBase64)
		elapsed := time.Since(now)
		log.Info("time put = "+elapsed.String())
		return err
	}
}

func (db *PgSQLDatabase) Get (key []byte) ([]byte, error) {
	now := time.Now()
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	var data string

	err := db.stmtGet.QueryRow(keyBase64).Scan(&data)
	if err != nil {
		return nil, err
	}
	value, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(now)
	log.Info("time get = "+elapsed.String())
	return value, nil
}

func (db *PgSQLDatabase) Has (key []byte) (bool, error){
	now := time.Now()
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	//sqlStatement := `SELECT count(data->>$1) FROM `+db.tableName+`WHERE data ->> $1 is not null;`
	var numRows int
	err := db.stmtHas.QueryRow(keyBase64).Scan(&numRows)
	hasKey := false
	//err := db.db.QueryRow(sqlStatement, keyBase64).Scan(&numRows)
	if numRows!=0{
		hasKey = true
	}
	elapsed := time.Since(now)
	log.Info("time has = "+elapsed.String())
	return hasKey, err

}

func (db *PgSQLDatabase) Delete(key []byte) error{
	now := time.Now()
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := `DELETE FROM `+db.tableName+` WHERE key = $1;`
	_, err := db.db.Exec(sqlStatement,keyBase64)
	elapsed := time.Since(now)
	log.Info("time delete = "+elapsed.String())
	return err
}

func (db *PgSQLDatabase) Close() {
	err := db.db.Close()
	if err != nil{
		panic(err)
	}
}

func (self *PgSQLDatabase) Write(batch *PsqlBatch) error {
	return batch.Write()
}

func (db *PgSQLDatabase) NewBatch() Batch {
	now := time.Now()
	tx, err := db.db.Begin()
	if err != nil{
		panic(err)
	}
	stmtPut, err := tx.Prepare(pq.CopyIn(db.tableName, "key", "value"))
	//stmtPut, err := tx.Prepare(`INSERT INTO `+db.tableName+` VALUES ($1)`)
	if err != nil{
		log.Error(err.Error())
	}
	elapsed := time.Since(now)
	log.Info("time newbatch = "+elapsed.String())
	return &PsqlBatch{
		db: db,
		tx:tx,
		stmtPut:stmtPut,
		}
}


type PsqlBatch struct {
	db   *PgSQLDatabase
	tx   *sql.Tx
	stmtPut *sql.Stmt
	size int
}

func (b *PsqlBatch) Put(key []byte, value []byte) error  {
	now := time.Now()
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)
	//log.Info("batch put thing")

	_, err := b.stmtPut.Exec(keyBase64,valueBase64)
	//sqlState	ment := `INSERT INTO `+b.db.tableName+` VALUES ($1)`
	////_, err := b.tx.Exec(sqlStatement,
	////	"{\""+keyBase64+"\":\""+valueBase64+"\"}")
	b.size += len(value)
	elapsed := time.Since(now)
	log.Info("time batch put = "+elapsed.String())
	return err

}

func (b *PsqlBatch) Delete(key []byte) error{
	now := time.Now()
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	//_, err := b.stmtDel.Exec(keyBase64)
	sqlStatement := `DELETE FROM `+b.db.tableName+` WHERE key = $1;`
	_, err := b.tx.Exec(sqlStatement,keyBase64)
	b.size += 1
	elapsed := time.Since(now)
	log.Info("time batch delete = "+elapsed.String())
	return err
}

func (b *PsqlBatch) Write() error  {
	now := time.Now()

	//b.stmtPut.Close()
	//b.stmtDel.Close()
	_, err := b.stmtPut.Exec()
	if err != nil {
		log.Error(err.Error())
	}
	err = b.stmtPut.Close()
	if err != nil {
		log.Error(err.Error())
	}
	err = b.tx.Commit()
	elapsed := time.Since(now)
	log.Info("time batch write = "+elapsed.String())
	return err
}

func (b *PsqlBatch) ValueSize() int  {
	return b.size
}

func (db *PgSQLDatabase) NewIterator() iterator.Iterator {
	return &PgSQLIterator{
		offset:0,
		key:    make([]byte, 0),
		value:  make([]byte, 0),
		db: db,
	}
}

type PgSQLIterator struct {
	offset int
	db *PgSQLDatabase
	//CommonIterator
	key         []byte
	value       []byte
	err         error
}

//type CommonIterator struct {
//
//}
func (i *PgSQLIterator) Error() error {
	return i.err
}

func (i *PgSQLIterator) First() bool {
	var key string
	var value string
	sqlStatement := "SELECT * FROM " + i.db.tableName + " ORDER BY key ASC LIMIT 1 OFFSET 0"
	err := i.db.db.QueryRow(sqlStatement).Scan(&key, &value)
	if err != nil{
		i.err = err
		return false
	}

	keyDecoded, err := base64.StdEncoding.DecodeString(key)
	if err!= nil{
		i.err = err
		return false
	}
	valueDecoded, err := base64.StdEncoding.DecodeString(value)
	if err!= nil{
		i.err = err
		return false
	}
	i.key = []byte(keyDecoded)
	i.value = []byte(valueDecoded)
	i.offset=0
	return true
}

func (i *PgSQLIterator) Last() bool {
	var totalString string
	var totalInt int

	sqlStatementLast := "SELECT count(*) FROM " + i.db.tableName
	err := i.db.db.QueryRow(sqlStatementLast).Scan(&totalString)
	if err != nil{
		if err == sql.ErrNoRows{
			return false
		}
		i.err = err
		return false
	}

	totalInt, err = strconv.Atoi(totalString)
	if err != nil{
		i.err = err
		return false
	}

	var key string
	var value string

	sqlStatement2 := "SELECT * FROM " + i.db.tableName + " ORDER BY key ASC LIMIT 1 OFFSET " + strconv.Itoa(totalInt-1)
	err = i.db.db.QueryRow(sqlStatement2).Scan(&key, &value)
	if err != nil{
		i.err = err
		return false
	}


	keyDecoded, err := base64.StdEncoding.DecodeString(key)
	if err!= nil{
		i.err = err
		return false
	}
	valueDecoded, err := base64.StdEncoding.DecodeString(value)
	if err!= nil{
		i.err = err
		return false
	}
	i.key = []byte(keyDecoded)
	i.value = []byte(valueDecoded)

	i.offset,err = strconv.Atoi(strconv.Itoa(totalInt-1))
	if err != nil{
		i.err = err
		return false
	}
	return true
}

func (i *PgSQLIterator) Next() bool {
	now := time.Now()
	var key string
	var value string

	sqlStatement := "SELECT * FROM " + i.db.tableName + " ORDER BY key ASC LIMIT 1 OFFSET " + strconv.Itoa(i.offset+1)
	err := i.db.db.QueryRow(sqlStatement).Scan(&key, &value)
	if err != nil{
		i.err = err
		return false
	}

	keyDecoded, err := base64.StdEncoding.DecodeString(key)
	if err!= nil{
		i.err = err
		return false
	}
	valueDecoded, err := base64.StdEncoding.DecodeString(value)
	if err!= nil{
		i.err = err
		return false
	}
	i.key = []byte(keyDecoded)
	i.value = []byte(valueDecoded)
	i.offset += 1
	elapsed := time.Since(now)
	log.Info("time iterator next = "+elapsed.String())
	return true
}

func (i *PgSQLIterator) Prev() bool {
	var key string
	var value string

	sqlStatement := "SELECT * FROM " + i.db.tableName + " ORDER BY key ASC LIMIT 1 OFFSET " + strconv.Itoa(i.offset-1)
	err := i.db.db.QueryRow(sqlStatement).Scan(&key, &value)
	if err != nil{
		i.err = err
		return false
	}

	keyDecoded, err := base64.StdEncoding.DecodeString(key)
	if err!= nil{
		i.err = err
		return false
	}
	valueDecoded, err := base64.StdEncoding.DecodeString(value)
	if err!= nil{
		i.err = err
		return false
	}
	i.key = []byte(keyDecoded)
	i.value = []byte(valueDecoded)

	i.offset -= 1
	return true
}

func (i *PgSQLIterator) Seek(key []byte) bool {
	var value string
	var indexString string

	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := "SELECT value, index FROM (SELECT key, value, row_number() OVER(ORDER BY key ASC) AS INDEX FROM "+i.db.tableName+") As data WHERE key = '"+keyBase64+"';"
	err := i.db.db.QueryRow(sqlStatement).Scan(&value,&indexString)

	if err!=nil{
		if err.Error() == "sql: no rows in result set"{
			return false
		}
		i.err = err
		return false
	}

	valueDecoded, err := base64.StdEncoding.DecodeString(value)
	if err!= nil{
		i.err = err
		return false
	}
	i.key = key
	i.value = []byte(valueDecoded)
	indexInt, err := strconv.Atoi(indexString)
	i.offset = indexInt-1
	if err!= nil{
		i.err = err
		return false
	}

	return true
}

func (i *PgSQLIterator) Key() []byte {
	return i.key
}

func (i *PgSQLIterator) Value() []byte {
	return i.value
}

func (i *PgSQLIterator) Release() {
	//doesn't do anything
}

func (i *PgSQLIterator) SetReleaser(releaser util.Releaser) {
	//doesn't do anything
}


func (i *PgSQLIterator) Valid() bool {
	if i.err != nil{
		return false
	}
	return true
}
