package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/getgort/gort/dataaccess/errs"
	gerr "github.com/getgort/gort/errors"
)

type postgresManager struct{}

func newPostgresManager() postgresManager {
	return postgresManager{}
}

func (postgresManager) databaseExists(ctx context.Context, conn *sql.Conn, dbName string) (bool, error) {
	const query = `SELECT datname
		FROM pg_database
		WHERE datistemplate = false AND datname = $1`

	rows, err := conn.QueryContext(ctx, query, dbName)
	if err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}
	defer rows.Close()

	datname := ""

	for rows.Next() {
		rows.Scan(&datname)

		if datname == dbName {
			return true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return false, nil
}

func (postgresManager) tableExists(ctx context.Context, table string, conn *sql.Conn) (bool, error) {
	var result string

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT to_regclass('public.%s');", table))
	if err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&result)

		if result == table {
			return true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return false, nil
}

func (postgresManager) openInternal(host string, port int, user, password, databaseName string, ctx context.Context, sslEnabled bool) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s database=%s",
		host, port, user,
		password, databaseName)

	if !sslEnabled {
		psqlInfo = psqlInfo + " sslmode=disable"
	}

	return sql.Open(PostgresDriverName, psqlInfo)
}
