package db

/**
 * @Description: 数据库连接池
 */

import (
	"database/sql"
	"file-storage-linhe/config"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

func InitDB() error {
	var err error
	DB, err = sql.Open("mysql", config.MySQLDNS)
	if err != nil {
		return err
	}

	DB.SetMaxOpenConns(20)                   // 最大连接数
	DB.SetMaxIdleConns(10)                   // 最大空闲连接数
	DB.SetConnMaxLifetime(300 * time.Second) // 连接最大生命周期

	if err := DB.Ping(); err != nil {
		_ = DB.Close()
		return err
	}
	log.Println("数据库连接成功！")
	return nil
}
