package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"strconv"
)

type MySQLDataAccess struct {
	SqlDataAccess
}

func openMySQL(host string, port int, user, password, databaseName string, ctx context.Context, sslEnabled bool) (*sql.DB, error) {
	c := mysql.NewConfig()
	c.User = user
	c.Passwd = password
	c.Net = "tcp"
	c.Addr = fmt.Sprintf("%s:%d", host, port)
	c.DBName = databaseName
	c.TLSConfig = strconv.FormatBool(sslEnabled)

	return sql.Open(MySQLDriverName, c.FormatDSN())
}
