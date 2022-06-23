package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/getgort/gort/dataaccess/errs"
	gerr "github.com/getgort/gort/errors"
	"github.com/go-sql-driver/mysql"
	"strconv"
)

type mySQLManager struct{}

func newMySQLManager() mySQLManager {
	return mySQLManager{}
}

func (mySQLManager) openInternal(host string, port int, user, password, databaseName string, ctx context.Context, sslEnabled bool) (*sql.DB, error) {
	c := mysql.NewConfig()
	c.User = user
	c.Passwd = password
	c.Net = "tcp"
	c.Addr = fmt.Sprintf("%s:%d", host, port)
	c.DBName = databaseName
	c.TLSConfig = strconv.FormatBool(sslEnabled)

	return sql.Open(MySQLDriverName, c.FormatDSN())
}

func (mySQLManager) tableExists(ctx context.Context, table string, conn *sql.Conn) (bool, error) {
	const query = `SHOW TABLES LIKE $1`

	rows, err := conn.QueryContext(ctx, query, table)
	if err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}

	if err := rows.Err(); err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return false, nil
}

func (mySQLManager) databaseExists(ctx context.Context, conn *sql.Conn, dbName string) (bool, error) {
	const query = `SHOW DATABASES LIKE $1`

	rows, err := conn.QueryContext(ctx, query, dbName)
	if err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}

	if err := rows.Err(); err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return false, nil
}
