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
	"encoding/json"
	"github.com/lib/pq"
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
	//this removes '/', '-' from string
	tableName = strings.Replace(tableName, "/", "", -1)
	tableName = strings.Replace(tableName, "-", "", -1)
	tableName = strings.Replace(tableName, ".", "", -1)
	EnsureDatabaseExists(dbname)
	EnsureTableExists(tableName)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	stmtHas, err := db.Prepare(`SELECT COUNT(data) FROM ` + tableName + ` WHERE data->>'key'=$1 AND row_status = TRUE;`)
	if err != nil {
		log.Error(err.Error())
	}
	stmtGet, err := db.Prepare(`SELECT data->>'value' FROM ` + tableName + ` WHERE data->>'key'=$1 AND row_status = TRUE;`)
	if err != nil {
		log.Error(err.Error())
	}
	stmtPut, err := db.Prepare(`INSERT INTO ` + tableName + ` VALUES ($1, True);`)
	if err != nil {
		log.Error(err.Error())
	}
	stmtUpdate, err := db.Prepare(`UPDATE ` + tableName + ` SET data = $1 where data->>'key' = $2;`)
	if err != nil {
		log.Error(err.Error())
	}

	return &PgSQLDatabase{
		db:         db,
		tableName:  tableName,
		stmtHas:    stmtHas,
		stmtGet:    stmtGet,
		stmtPut:    stmtPut,
		stmtUpdate: stmtUpdate,
	}, nil
}

//check if database exists, if not create it
func EnsureDatabaseExists(dbname string) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s sslmode=disable",
		host, port, user, password)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Error("could not get a connection:" + err.Error())
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Error("could not get a connection:" + err.Error())
	}

	//database exists if res.RowsAffected() returns 1, does not exists if returns 0
	res, err := db.Exec("SELECT 1 FROM pg_database WHERE datname = '" + dbname + "';")
	if err != nil {
		log.Error(err.Error())
	}
	exists, err := res.RowsAffected()
	if err != nil {
		log.Error(err.Error())
	}
	if exists == 0 {
		_, err := db.Exec("CREATE DATABASE " + dbname)
		if err != nil {
			log.Error(err.Error())
		}
		log.Info("created db")
	}

}

//check if table exists, if not create it
func EnsureTableExists(tableName string) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Error("Could not get a connection:" + err.Error())
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Error("could not get a connection:" + err.Error())
	}
	//row_status represent if the row is soft-deleted
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS ` + tableName + `(data jsonb, row_status BOOLEAN DEFAULT TRUE)`)
	if err != nil {
		log.Error("Create table failed :" + err.Error())
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS ` + tableName + `_index ON ` + tableName + `((data->>'key'));`)
	if err != nil {
		log.Error("Create index failed :" + err.Error())
	}
}

func (db *PgSQLDatabase) Put(key []byte, value []byte) error {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)
	hasKey, err := db.Has(key)
	if err != nil {
		return err
	}
	if hasKey {
		_, err := db.stmtUpdate.Exec("{\"key\": \""+keyBase64+"\", \"value\" :\""+valueBase64+"\"}", keyBase64)
		return err
	} else {
		_, err := db.stmtPut.Exec("{\"key\": \"" + keyBase64 + "\", \"value\" :\"" + valueBase64 + "\"}")
		return err
	}
}

func (db *PgSQLDatabase) Get(key []byte) ([]byte, error) {
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
	return value, nil
}

func (db *PgSQLDatabase) Has(key []byte) (bool, error) {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	var numRows int
	err := db.stmtHas.QueryRow(keyBase64).Scan(&numRows)
	hasKey := false
	if numRows != 0 {
		hasKey = true
	}
	return hasKey, err

}

func (db *PgSQLDatabase) Delete(key []byte) error {
	//used soft-delete(set row_status to false)
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := `UPDATE ` + db.tableName + ` SET row_status = FALSE WHERE data->>'key'=$1;`
	_, err := db.db.Exec(sqlStatement, keyBase64)
	return err
}

func (db *PgSQLDatabase) Close() {
	err := db.db.Close()
	if err != nil {
		log.Error(err.Error())
	}
}

func (self *PgSQLDatabase) Write(batch *PsqlBatch) error {
	return batch.Write()
}

func (db *PgSQLDatabase) NewBatch() Batch {
	tx, err := db.db.Begin()
	if err != nil {
		log.Error(err.Error())
	}
	stmtPut, err := tx.Prepare(pq.CopyIn(db.tableName, "data", "row_status"))
	if err != nil {
		log.Error(err.Error())
	}
	return &PsqlBatch{
		db:      db,
		tx:      tx,
		stmtPut: stmtPut,
	}
}

type PsqlBatch struct {
	db      *PgSQLDatabase
	tx      *sql.Tx
	stmtPut *sql.Stmt
	size    int
}

func (b *PsqlBatch) Put(key []byte, value []byte) error {
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	valueBase64 := base64.StdEncoding.EncodeToString(value)
	_, err := b.stmtPut.Exec("{\"key\": \"" + keyBase64 + "\", \"value\" :\"" + valueBase64 + "\"}", true)
	b.size += len(value)
	return err

}

func (b *PsqlBatch) Delete(key []byte) error {
	//used soft-delete(set row_status to false)
	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := `UPDATE ` + b.db.tableName + ` SET row_status = FALSE WHERE data->>'key' = $1;`
	_, err := b.tx.Exec(sqlStatement, keyBase64)
	b.size += 1
	return err
}

func (b *PsqlBatch) Write() error {
	_, err := b.stmtPut.Exec()
	if err != nil {
		log.Error(err.Error())
	}
	err = b.stmtPut.Close()
	if err != nil {
		log.Error(err.Error())
	}
	err = b.tx.Commit()
	return err
}

func (b *PsqlBatch) ValueSize() int {
	return b.size
}

func (db *PgSQLDatabase) NewIterator() iterator.Iterator {
	return &PgSQLIterator{
		offset: 0,
		key:    make([]byte, 0),
		value:  make([]byte, 0),
		db:     db,
	}
}

type PgSQLIterator struct {
	offset int
	db     *PgSQLDatabase
	key    []byte
	value  []byte
	err    error
}

func (i *PgSQLIterator) Error() error {
	return i.err
}

func (i *PgSQLIterator) First() bool {
	var rowString string
	var jsonMap map[string]string
	sqlStatement := "SELECT * FROM " + i.db.tableName + "WHERE row_status = TRUE ORDER BY data ASC LIMIT 1 OFFSET 0"
	err := i.db.db.QueryRow(sqlStatement).Scan(&rowString)
	if err != nil {
		i.err = err
		return false
	}
	err = json.Unmarshal([]byte(rowString), &jsonMap)
	if err != nil {
		i.err = err
		return false
	}
	for key, value := range jsonMap {
		keyDecoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			i.err = err
			return false
		}
		valueDecoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			i.err = err
			return false
		}
		i.key = []byte(keyDecoded)
		i.value = []byte(valueDecoded)
	}
	i.offset = 0
	return true
}

func (i *PgSQLIterator) Last() bool {
	var totalString string
	var totalInt int

	sqlStatementLast := "SELECT count(*) FROM " + i.db.tableName + " WHERE row_status = TRUE;"
	err := i.db.db.QueryRow(sqlStatementLast).Scan(&totalString)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		i.err = err
		return false
	}

	totalInt, err = strconv.Atoi(totalString)
	if err != nil {
		i.err = err
		return false
	}

	var rowString string
	var jsonMap map[string]string
	sqlStatement2 := "SELECT * FROM " + i.db.tableName + " WHERE row_status = TRUE ORDER BY data ASC LIMIT 1 OFFSET " + strconv.Itoa(totalInt-1)
	err = i.db.db.QueryRow(sqlStatement2).Scan(&rowString)
	if err != nil {
		i.err = err
		return false
	}

	err = json.Unmarshal([]byte(rowString), &jsonMap)
	if err != nil {
		i.err = err
		return false
	}
	for key, value := range jsonMap {
		keyDecoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			i.err = err
			return false
		}
		valueDecoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			i.err = err
			return false
		}
		i.key = []byte(keyDecoded)
		i.value = []byte(valueDecoded)
	}

	i.offset, err = strconv.Atoi(strconv.Itoa(totalInt - 1))
	if err != nil {
		i.err = err
		return false
	}
	return true
}

func (i *PgSQLIterator) Next() bool {
	var rowString string
	var jsonMap map[string]string

	sqlStatement := "SELECT * FROM " + i.db.tableName + " WHERE row_status = TRUE ORDER BY data ASC LIMIT 1 OFFSET " + strconv.Itoa(i.offset+1)
	err := i.db.db.QueryRow(sqlStatement).Scan(&rowString)
	if err != nil {
		i.err = err
		return false
	}
	err = json.Unmarshal([]byte(rowString), &jsonMap)
	if err != nil {
		i.err = err
		return false
	}
	for key, value := range jsonMap {
		keyDecoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			i.err = err
			return false
		}
		valueDecoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			i.err = err
			return false
		}
		i.key = []byte(keyDecoded)
		i.value = []byte(valueDecoded)
	}
	i.offset += 1
	return true
}

func (i *PgSQLIterator) Prev() bool {
	var rowString string
	var jsonMap map[string]string

	sqlStatement := "SELECT * FROM " + i.db.tableName + "  WHERE row_status = TRUE ORDER BY data ASC LIMIT 1 OFFSET " + strconv.Itoa(i.offset-1)
	err := i.db.db.QueryRow(sqlStatement).Scan(&rowString)
	if err != nil {
		i.err = err
		return false
	}
	err = json.Unmarshal([]byte(rowString), &jsonMap)
	if err != nil {
		i.err = err
		return false
	}
	for key, value := range jsonMap {
		keyDecoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			i.err = err
			return false
		}
		valueDecoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			i.err = err
			return false
		}
		i.key = []byte(keyDecoded)
		i.value = []byte(valueDecoded)
	}
	i.offset -= 1
	return true
}

func (i *PgSQLIterator) Seek(key []byte) bool {
	var dataString string
	var indexString string
	var jsonMap map[string]string

	keyBase64 := base64.StdEncoding.EncodeToString(key)
	sqlStatement := "SELECT data, index FROM (SELECT data, row_number() OVER(ORDER BY data ASC) AS INDEX FROM " + i.db.tableName + ") As data WHERE data ->> '" + keyBase64 + "' is not null;"
	err := i.db.db.QueryRow(sqlStatement).Scan(&dataString, &indexString)

	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return false
		}
		i.err = err
		return false
	}

	err = json.Unmarshal([]byte(dataString), &jsonMap)
	if err != nil {
		i.err = err
		return false
	}

	for key, value := range jsonMap {
		// loop will run exactly once, as jsonMap contains only one item
		keyDecoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			i.err = err
			return false
		}
		valueDecoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			i.err = err
			return false
		}
		i.key = []byte(keyDecoded)
		i.value = []byte(valueDecoded)
	}
	indexInt, err := strconv.Atoi(indexString)
	i.offset = indexInt - 1
	if err != nil {
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
	if i.err != nil {
		return false
	}
	return true
}
