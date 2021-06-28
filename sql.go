package gossql

import (
	"database/sql"
	"errors"
	"reflect"
)

type GoSql struct {
	db *sql.DB
}

//InitEnv 初始化
func New(db *sql.DB) *GoSql {
	if db == nil {
		panic("db is a must")
	}
	err := db.Ping()
	if err != nil {
		panic(err)
	}
	return &GoSql{
		db: db,
	}
}

//--------------------------SQL--------------------------------------Start
func (sqlDB *GoSql) getResult(sql string, parameters ...interface{}) (sql.Result, error) {
	stmt, err := sqlDB.db.Prepare(sql)
	if err != nil || stmt == nil {
		return nil, err
	}
	defer stmt.Close()
	rs, err := stmt.Exec(parameters...)
	return rs, err
}

//Insert
func (sqlDB *GoSql) Insert(sql string, parameters ...interface{}) (int64, error) {
	rs, err := sqlDB.getResult(sql, parameters...)
	if err != nil {
		return 0, err
	}
	return rs.LastInsertId()
}

//Update
func (sqlDB *GoSql) Update(sql string, parameters ...interface{}) (int64, error) {
	rs, err := sqlDB.getResult(sql, parameters...)
	if err != nil {
		return 0, err
	}
	return rs.RowsAffected()
}

//Delete
func (sqlDB *GoSql) Delete(sql string, parameters ...interface{}) (int64, error) {
	rs, err := sqlDB.getResult(sql, parameters...)
	if err != nil {
		return 0, err
	}
	return rs.RowsAffected()
}

//Tx func
func (sqlDB *GoSql) TransactionFunc(fn func(transaction *Transaction) error) (bool, error) {
	tx := sqlDB.BeginTransaction()
	if tx.err != nil {
		return false, tx.err
	}
	defer tx.Rollback()
	err := fn(tx)
	if err != nil {
		return false, err
	}
	err = tx.Commit()
	if err != nil {
		return false, err
	}
	return true, nil
}

type query struct {
	db          *sql.DB
	sql         string
	parameter   []interface{}
	data        []map[string]string
	err         error
	transaction *Transaction
}

//Query
func (sqlDB *GoSql) Query(sql string, parameter ...interface{}) *query {
	query := &query{
		db:          sqlDB.db,
		sql:         sql,
		parameter:   parameter,
		data:        nil,
		err:         nil,
		transaction: nil,
	}
	return query
}

func (q *query) getRows() (*sql.Rows, error) {
	if q.db != nil {
		rows, err := q.db.Query(q.sql, q.parameter...)
		return rows, err
	} else if q.transaction != nil {
		rows, err := q.transaction.tx.Query(q.sql, q.parameter...)
		return rows, err
	}
	return nil, nil
}

func (q *query) getRow() *sql.Row {
	if q.db != nil {
		row := q.db.QueryRow(q.sql, q.parameter...)
		return row
	} else if q.transaction != nil {
		row := q.transaction.tx.QueryRow(q.sql, q.parameter...)
		return row
	}
	return nil
}

//handle
func (q *query) handleQuery() {
	rows, err := q.getRows()
	if err != nil {
		q.err = err
		return
	}
	defer rows.Close()
	column, err := rows.Columns()
	if err != nil {
		q.err = err
		return
	}
	values := make([][]byte, len(column)) //行
	scans := make([]interface{}, len(column))
	for i := range values {
		scans[i] = &values[i]
	}
	results := make([]map[string]string, 0)
	for rows.Next() {
		//一行一行的取数据
		if err := rows.Scan(scans...); err != nil {
			q.err = err
			return
		}
		row := make(map[string]string)
		for k, v := range values {
			row[column[k]] = string(v)
		}
		results = append(results, row)
	}
	q.data = results
}

//one
func (q *query) Unique(model interface{}) error {
	if q.err != nil {
		return q.err
	}
	q.handleQuery()
	if q.err != nil {
		return q.err
	}
	if len(q.data) > 0 {
		return Mapping(q.data[0], reflect.ValueOf(model))
	}
	return nil
}

//list
func (q *query) ToList(list interface{}) error {
	if q.err != nil {
		return q.err
	}
	v := reflect.ValueOf(list)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return errors.New("list need slice kind")
	}
	q.handleQuery()
	if q.err != nil {
		return q.err
	}
	length := len(q.data)
	if length > 0 {
		newv := reflect.MakeSlice(v.Type(), 0, length)
		v.Set(newv)
		v.SetLen(length)
		for i := 0; i < length; i++ {
			k := v.Type().Elem()
			newObj := reflect.New(k)
			err := Mapping(q.data[i], newObj)
			if err != nil {
				return err
			}
			if newObj.Kind() == reflect.Ptr {
				newObj = newObj.Elem()
			}
			v.Index(i).Set(newObj)
		}
	}
	return nil
}

//Count
func (q *query) Count(size *int) error {
	if q.err != nil {
		return q.err
	}
	err := q.getRow().Scan(size)
	if err != nil {
		return err
	}
	return nil
}

//--------------------------SQL--------------------------------------End

//--------------------------Tx---------------------------------------Start
//Tx
type Transaction struct {
	tx  *sql.Tx
	err error
}

//Tx begin
func (sqlDB *GoSql) BeginTransaction() *Transaction {
	tx, err := sqlDB.db.Begin()
	return &Transaction{
		tx:  tx,
		err: err,
	}
}
func (transaction *Transaction) getResult(sql string, parameters ...interface{}) (sql.Result, error) {
	stmt, err := transaction.tx.Prepare(sql)
	if err != nil || stmt == nil {
		return nil, err
	}
	defer stmt.Close()
	rs, err := stmt.Exec(parameters...)
	return rs, err
}

//Insert
func (transaction *Transaction) Insert(sql string, parameters ...interface{}) (int64, error) {
	if transaction.err != nil {
		return 0, transaction.err
	}
	rs, err := transaction.getResult(sql, parameters...)
	if err != nil {
		return 0, err
	}
	return rs.LastInsertId()
}

//Update
func (transaction *Transaction) Update(sql string, parameters ...interface{}) (int64, error) {
	if transaction.err != nil {
		return 0, transaction.err
	}
	rs, err := transaction.getResult(sql, parameters...)
	if err != nil {
		return 0, err
	}
	return rs.RowsAffected()
}

//Delete
func (transaction *Transaction) Delete(sql string, parameters ...interface{}) (int64, error) {
	if transaction.err != nil {
		return 0, transaction.err
	}
	rs, err := transaction.getResult(sql, parameters...)
	if err != nil {
		return 0, err
	}
	return rs.RowsAffected()
}

//Tx Query
func (transaction *Transaction) Query(sql string, parameter ...interface{}) *query {
	query := &query{
		db:          nil,
		sql:         sql,
		parameter:   parameter,
		data:        nil,
		err:         transaction.err,
		transaction: transaction,
	}
	return query
}

//TX Commit
func (transaction *Transaction) Commit() error {
	return transaction.tx.Commit()
}

//Tx Rollback
func (transaction *Transaction) Rollback() error {
	return transaction.tx.Rollback()
}

//--------------------------Tx---------------------------------------End
