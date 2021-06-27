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

//--------------------------SQL--------------------------------------
//Insert
func (sqlDB *GoSql) Insert(sql string, parameters ...interface{}) (int64, error) {
	stmt, err := sqlDB.db.Prepare(sql)
	if err != nil || stmt == nil {
		return 0, err
	}
	defer stmt.Close()
	rs, err := stmt.Exec(parameters...)
	if err != nil {
		return 0, err
	}
	return rs.LastInsertId()
}

//Update
func (sqlDB *GoSql) Update(sql string, parameters ...interface{}) (int64, error) {
	stmt, err := sqlDB.db.Prepare(sql)
	if err != nil || stmt == nil {
		return 0, err
	}
	defer stmt.Close()
	rs, err := stmt.Exec(parameters...)
	if err != nil {
		return 0, err
	}
	return rs.RowsAffected()
}

//Delete
func (sqlDB *GoSql) Delete(sql string, parameters ...interface{}) (int64, error) {
	stmt, err := sqlDB.db.Prepare(sql)
	if err != nil || stmt == nil {
		return 0, err
	}
	defer stmt.Close()
	rs, err := stmt.Exec(parameters...)
	if err != nil {
		return 0, err
	}
	return rs.RowsAffected()
}

func (sqlDB *GoSql) TransactionFunc(fn func() error) (bool, error) {
	tx, err := sqlDB.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	err = fn()
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
	db        *sql.DB
	sql       string
	parameter []interface{}
	data      []map[string]string
	err       error
}

func (sqlDB *GoSql) Query(sql string, parameter ...interface{}) *query {
	query := &query{
		db:        sqlDB.db,
		sql:       sql,
		parameter: parameter,
		data:      nil,
		err:       nil,
	}
	return query
}

func (q *query) handleQuery() {
	rows, err := q.db.Query(q.sql, q.parameter...)
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

func (q *query) Unique(model interface{}) error {
	q.handleQuery()
	if q.err != nil {
		return q.err
	}
	if len(q.data) > 0 {
		return Mapping(q.data[0], reflect.ValueOf(model))
	}
	return nil
}

func (q *query) ToList(list interface{}) error {
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

func (q *query) Count(size *int) error {
	if q.err != nil {
		return q.err
	}
	err := q.db.QueryRow(q.sql, q.parameter...).Scan(size)
	if err != nil {
		return err
	}
	return nil
}

//--------------------------SQL--------------------------------------

//--------------------------NOSQL--------------------------------------

//--------------------------NOSQL--------------------------------------
