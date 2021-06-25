package gossql

import (
	"database/sql"
)

type GoSql struct {
	db *sql.DB
}

//InitEnv 初始化
func New(db *sql.DB) *GoSql {
	if db == nil {
		panic("db is a must")
	}
	return &GoSql{
		db: db,
	}
}

//Insert
func (sqlDB *GoSql) Insert(sql string, parameters ...interface{}) (int64, error) {
	stmt, err := sqlDB.db.Prepare(sql)
	if err != nil || stmt == nil {
		return 0, err
	}
	rs, err := stmt.Exec(parameters...)
	if err != nil {
		return 0, err
	}
	Id, err := rs.LastInsertId()
	if err != nil {
		return 0, err
	}
	return Id, nil
}

//Update
func (sqlDB *GoSql) Update(sql string, parameters ...interface{}) (int64, error) {
	stmt, err := sqlDB.db.Prepare(sql)
	if err != nil || stmt == nil {
		return 0, err
	}
	rs, err := stmt.Exec(parameters...)
	if err != nil {
		return 0, err
	}
	affected, err := rs.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}
