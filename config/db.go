package config

import (
	"fmt"
)

var (
	MySQLHost     = getEnv("MYSQL_HOST", "127.0.0.1")
	MySQLPort     = getEnv("MYSQL_PORT", "3306")
	MySQLUser     = getEnv("MYSQL_USER", "root")
	MySQLPassword = getEnv("MYSQL_PASSWORD", "Zsq0331@.")
	MySQLDatabase = getEnv("MYSQL_DATABASE", "filestore")
)

var MySQLDNS = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
	MySQLUser, MySQLPassword, MySQLHost, MySQLPort, MySQLDatabase)
