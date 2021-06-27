package gossql

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

/* sql := "CREATE TABLE `userinfo` (
    `uid` INTEGER PRIMARY KEY AUTOINCREMENT,
    `username` VARCHAR(64) NULL,
    `departname` VARCHAR(64) NULL,
    `created` DATE NULL
);" */

type Userinfo struct {
	Uid        int    `col:"uid"`
	UserName   string `col:"username"`
	DepartName string `col:"departname"`
	Created    string `col:"created"`
}

func TestInsert(t *testing.T) {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	gosql := New(db)
	//插入数据
	a, err := gosql.Insert("INSERT INTO userinfo(username, departname, created) values(?,?,?)", "astaxie", "研发部门", "2012-12-09")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(a)
}

func TestTag(t *testing.T) {
	fmt.Println(ColTag.String())
}

func TestQuery(t *testing.T) {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	gosql := New(db)
	userinfo := Userinfo{}
	err = gosql.Query("SELECT * FROM userinfo").Unique(&userinfo)
	if err != nil {
		fmt.Println("query failed")
	} else {
		fmt.Println(userinfo)
	}
}

func TestQueryList(t *testing.T) {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	gosql := New(db)
	list := []Userinfo{}
	err = gosql.Query("SELECT * FROM userinfo").ToList(&list)
	if err != nil {
		fmt.Println("query failed")
	} else {
		fmt.Println(list)
	}
}

func TestCount(t *testing.T) {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	gosql := New(db)
	var size int = 0
	err = gosql.Query("SELECT count(1) FROM userinfo").Count(&size)
	if err != nil {
		fmt.Println("query failed")
	} else {
		fmt.Println(size)
	}
}
