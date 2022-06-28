/*
 * Copyright 2021 The Gort Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
	"sync"

	"github.com/getgort/gort/data"
	"github.com/getgort/gort/dataaccess/errs"
	gerr "github.com/getgort/gort/errors"
	"github.com/getgort/gort/telemetry"

	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel"
)

const (
	DatabaseGort = "gort"
	DriverName   = "mysql"
)

func execMultiQuery(ctx context.Context, multiQuery string, conn *sql.Conn) error {
	for _, query := range strings.Split(multiQuery, ";\n") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}
		_, err := conn.ExecContext(ctx, query+";")
		if err != nil {
			return gerr.Wrap(errs.ErrDataAccess, err)
		}
	}

	return nil
}

// MySqlDataAccess is a data access implementation backed by a database.
type MySqlDataAccess struct {
	configs data.DatabaseConfigs
	dbs     map[string]*sql.DB
	mutex   *sync.Mutex
}

// NewMySQLDataAccess returns a new MySqlDataAccess based on the
// supplied config.
func NewMySQLDataAccess(configs data.DatabaseConfigs) MySqlDataAccess {
	return MySqlDataAccess{
		configs: configs,
		dbs:     map[string]*sql.DB{},
		mutex:   &sync.Mutex{},
	}
}

// Initialize sets up the database.
func (da MySqlDataAccess) Initialize(ctx context.Context) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.Initialize")
	defer sp.End()

	if err := da.initializeGortData(ctx); err != nil {
		return gerr.Wrap(fmt.Errorf("failed to initialize gort data"), err)
	}
	if err := da.initializeAuditData(ctx); err != nil {
		return gerr.Wrap(fmt.Errorf("failed to initialize audit data"), err)
	}

	return nil
}

func (da MySqlDataAccess) initializeAuditData(ctx context.Context) error {
	// Does the database exist? If not, create it.
	err := da.ensureDatabaseExists(ctx, DatabaseGort)
	if err != nil {
		return gerr.Wrap(fmt.Errorf("cannot ensure gort database exists"), err)
	}

	// Establish a connection to the "gort" database
	conn, err := da.connect(ctx)
	if err != nil {
		return gerr.Wrap(fmt.Errorf("cannot connect to gort database"), err)
	}
	defer conn.Close()

	// Check whether the users table exists
	exists, err := da.tableExists(ctx, "commands", conn)
	if err != nil {
		return err
	}

	// If not, assume none of them do. Create them all.
	if !exists {
		err = da.createCommandsTable(ctx, conn)
		if err != nil {
			return gerr.Wrap(fmt.Errorf("failed to create commands table"), err)
		}
	}

	return nil
}

func (da MySqlDataAccess) initializeGortData(ctx context.Context) error {
	// Does the database exist? If not, create it.
	err := da.ensureDatabaseExists(ctx, DatabaseGort)
	if err != nil {
		return err
	}

	// Establish a connection to the "gort" database
	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Check whether the users table exists
	exists, err := da.tableExists(ctx, "users", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createUsersTable(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Check whether the user adapter ids table exists
	if exists, err = da.tableExists(ctx, "user_adapter_ids", conn); err != nil {
		return err
	} else if !exists {
		err = da.createUsersAdapterIDsTable(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Check whether the groups table exists
	exists, err = da.tableExists(ctx, "groups", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createGroupsTable(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Check whether the groupusers table exists
	exists, err = da.tableExists(ctx, "groupusers", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createGroupUsersTable(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Check whether the tokens table exists
	exists, err = da.tableExists(ctx, "tokens", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createTokensTable(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Upsert bundles tables to make sure it and related tables exist with appropriate columns
	err = da.createBundlesTables(ctx, conn)
	if err != nil {
		return err
	}

	// Check whether the bundles_kubernetes table exists
	exists, err = da.tableExists(ctx, "bundle_kubernetes", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createBundleKubernetesTables(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Check whether the roles table exists
	exists, err = da.tableExists(ctx, "roles", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createRolesTables(ctx, conn)
		if err != nil {
			return err
		}
	}

	// Check whether the configs table exists
	exists, err = da.tableExists(ctx, "configs", conn)
	if err != nil {
		return err
	}
	if !exists {
		err = da.createConfigsTable(ctx, conn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (da MySqlDataAccess) connect(ctx context.Context) (*sql.Conn, error) {
	db, err := da.open(ctx, DatabaseGort)
	if err != nil {
		return nil, gerr.WrapStr("failed to open Gort database", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (da MySqlDataAccess) createBundlesTables(ctx context.Context, conn *sql.Conn) error {
	createBundlesQuery := `CREATE TABLE IF NOT EXISTS bundles (
		gort_bundle_version INT NOT NULL CHECK(gort_bundle_version > 0),
		name				VARCHAR(255) NOT NULL CHECK(name <> ''),
		version				VARCHAR(255) NOT NULL CHECK(version <> ''),
		author				VARCHAR(255),
		homepage			VARCHAR(255),
		description			VARCHAR(255) NOT NULL CHECK(description <> ''),
		long_description	VARCHAR(255),
		image_repository	VARCHAR(255),
		image_tag			VARCHAR(255),
		install_timestamp	TIMESTAMP DEFAULT now(),
		install_user		VARCHAR(255),
		CONSTRAINT 			unq_bundle UNIQUE(name, version),
		PRIMARY KEY 		(name, version)
	);

	CREATE TABLE IF NOT EXISTS bundle_enabled (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		CONSTRAINT			unq_bundle_enabled UNIQUE(bundle_name),
		PRIMARY KEY			(bundle_name),
		FOREIGN KEY 		(bundle_name, bundle_version) REFERENCES bundles(name, version)
		ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS bundle_permissions (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		theindex			INT NOT NULL CHECK(theindex >= 0),
		permission			VARCHAR(255),
		CONSTRAINT			unq_bundle_permission UNIQUE(bundle_name, bundle_version, theindex),
		PRIMARY KEY			(bundle_name, bundle_version, theindex),
		FOREIGN KEY 		(bundle_name, bundle_version) REFERENCES bundles(name, version)
		ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS bundle_templates (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		command				VARCHAR(255),
		command_error		VARCHAR(255),
		message				VARCHAR(255),
		message_error		VARCHAR(255),
		CONSTRAINT			unq_bundle_template UNIQUE(bundle_name, bundle_version),
		PRIMARY KEY			(bundle_name, bundle_version),
		FOREIGN KEY 		(bundle_name, bundle_version) REFERENCES bundles(name, version)
		ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS bundle_commands (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		name				VARCHAR(255) NOT NULL CHECK(name <> ''),
		description			VARCHAR(255) NOT NULL,
		executable			VARCHAR(255) NOT NULL,
		long_description	VARCHAR(255),
		CONSTRAINT			unq_bundle_command UNIQUE(bundle_name, bundle_version, name),
		PRIMARY KEY			(bundle_name, bundle_version, name),
		FOREIGN KEY 		(bundle_name, bundle_version) REFERENCES bundles(name, version)
		ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS bundle_command_triggers (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		command_name		VARCHAR(255) NOT NULL,
		thematch 		 	VARCHAR(255) NOT NULL,
		id					INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		FOREIGN KEY 		(bundle_name, bundle_version, command_name)
		REFERENCES 			bundle_commands(bundle_name, bundle_version, name)
		ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS bundle_command_rules (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		command_name		VARCHAR(255) NOT NULL,
		rule				VARCHAR(255) NOT NULL CHECK(rule <> ''),
		id					INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		FOREIGN KEY 		(bundle_name, bundle_version, command_name)
		REFERENCES 			bundle_commands(bundle_name, bundle_version, name)
		ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS bundle_command_templates (
		bundle_name			VARCHAR(255) NOT NULL,
		bundle_version		VARCHAR(255) NOT NULL,
		command_name		VARCHAR(255) NOT NULL,
		command				VARCHAR(255),
		command_error		VARCHAR(255),
		message				VARCHAR(255),
		message_error		VARCHAR(255),
		CONSTRAINT			unq_bundle_command_templates UNIQUE(bundle_name, bundle_version, command_name),
		PRIMARY KEY			(bundle_name, bundle_version, command_name),
		FOREIGN KEY 		(bundle_name, bundle_version, command_name)
		REFERENCES 			bundle_commands(bundle_name, bundle_version, name)
		ON DELETE CASCADE
	);
	`

	return execMultiQuery(ctx, createBundlesQuery, conn)
}

func (da MySqlDataAccess) createBundleKubernetesTables(ctx context.Context, conn *sql.Conn) error {
	var err error

	createBundlesQuery := `CREATE TABLE bundle_kubernetes (
		bundle_version 			VARCHAR(255) NOT NULL,
		bundle_name				VARCHAR(255) NOT NULL,
		service_account_name 	VARCHAR(255) NOT NULL,
		env_secret            VARCHAR(255) NOT NULL
	);
	`

	_, err = conn.ExecContext(ctx, createBundlesQuery)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) createConfigsTable(ctx context.Context, conn *sql.Conn) error {
	var err error

	createConfigsQuery := `CREATE TABLE configs (
		bundle_name		VARCHAR(255) NOT NULL CHECK(bundle_name <> ''),
		layer			VARCHAR(255) NOT NULL,
		owner			VARCHAR(255) NOT NULL,
		thekey			VARCHAR(255) NOT NULL,
		value			VARCHAR(255) NOT NULL,
		secret			BOOLEAN NOT NULL
	);`

	_, err = conn.ExecContext(ctx, createConfigsQuery)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) createGroupsTable(ctx context.Context, conn *sql.Conn) error {
	var err error

	createGroupQuery := `CREATE TABLE groups (
		groupname VARCHAR(255) PRIMARY KEY
	  );`

	_, err = conn.ExecContext(ctx, createGroupQuery)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) createGroupUsersTable(ctx context.Context, conn *sql.Conn) error {
	var err error

	createGroupUsersQuery := `CREATE TABLE groupusers (
		groupname VARCHAR(255) REFERENCES groups,
		username  VARCHAR(255) REFERENCES users,
		PRIMARY KEY (groupname, username)
	);`

	_, err = conn.ExecContext(ctx, createGroupUsersQuery)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) createRolesTables(ctx context.Context, conn *sql.Conn) error {
	var err error

	createRolesQuery := `CREATE TABLE roles (
		role_name		VARCHAR(255) NOT NULL,
		PRIMARY KEY 	(role_name)
	);

	CREATE TABLE group_roles (
		group_name		VARCHAR(255) NOT NULL,
		role_name		VARCHAR(255) NOT NULL,
		CONSTRAINT		unq_group_role UNIQUE(group_name, role_name),
		PRIMARY KEY		(group_name, role_name),
		FOREIGN KEY 	(group_name) REFERENCES groups(groupname)
		ON DELETE CASCADE
	);

	CREATE TABLE role_permissions (
		role_name			VARCHAR(255) NOT NULL,
		bundle_name			VARCHAR(255) NOT NULL,
		permission			VARCHAR(255) NOT NULL,
		CONSTRAINT			unq_role_permission UNIQUE(role_name, bundle_name, permission),
		PRIMARY KEY			(role_name, bundle_name, permission),
		FOREIGN KEY 		(role_name) REFERENCES roles(role_name)
		ON DELETE CASCADE
	);
	`
	err = execMultiQuery(ctx, createRolesQuery, conn)
	if err != nil {
		return err
	}

	return nil
}

func (da MySqlDataAccess) createTokensTable(ctx context.Context, conn *sql.Conn) error {
	createTokensQuery := `CREATE TABLE tokens (
		token       VARCHAR(255),
		username    VARCHAR(255) REFERENCES users,
		valid_from  TIMESTAMP,
		valid_until TIMESTAMP,
		PRIMARY KEY (username)
	);

	CREATE UNIQUE INDEX tokens_token ON tokens (token);
	`

	return execMultiQuery(ctx, createTokensQuery, conn)
}

func (da MySqlDataAccess) createUsersTable(ctx context.Context, conn *sql.Conn) error {
	var err error

	createUserQuery := `CREATE TABLE users (
		email         	VARCHAR(255),
		full_name     	VARCHAR(255),
		password_hash 	VARCHAR(255),
		username 		VARCHAR(255) PRIMARY KEY
	  );`

	_, err = conn.ExecContext(ctx, createUserQuery)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) createUsersAdapterIDsTable(ctx context.Context, conn *sql.Conn) error {
	var err error

	createTableQuery := `CREATE TABLE user_adapter_ids (
		username            VARCHAR(255) NOT NULL,
		adapter             VARCHAR(255) NOT NULL,
		id                  VARCHAR(255) NOT NULL,
		CONSTRAINT          unq_adapter_id UNIQUE(username, adapter),
		PRIMARY KEY         (adapter, id),
		FOREIGN KEY         (username) REFERENCES users(username)
		ON DELETE CASCADE
	);
	`

	_, err = conn.ExecContext(ctx, createTableQuery)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) databaseExists(ctx context.Context, conn *sql.Conn, dbName string) (bool, error) {
	const query = `SHOW DATABASES;`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}
	defer rows.Close()

	var db string
	for rows.Next() {
		rows.Scan(&db)

		if db == dbName {
			return true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return false, nil
}

// ensureGortDatabaseExists simply checks whether the dbname database exists,
// and creates the empty database if it doesn't.
func (da MySqlDataAccess) ensureDatabaseExists(ctx context.Context, dbName string) error {
	db, err := da.open(ctx, "gort")
	if err != nil {
		return gerr.WrapStr("failed to open mysql database", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	exists, err := da.databaseExists(ctx, conn, dbName)
	if err != nil {
		return err
	}

	if !exists {
		_, err := conn.ExecContext(ctx, "CREATE DATABASE ?;", dbName)
		if err != nil {
			return gerr.Wrap(errs.ErrDataAccess,
				gerr.Wrap(fmt.Errorf("failed to create database"),
					err))
		}
	}

	return nil
}

func (da MySqlDataAccess) open(ctx context.Context, databaseName string) (*sql.DB, error) {
	da.mutex.Lock()
	defer da.mutex.Unlock()

	if db, exists := da.dbs[databaseName]; exists {
		return db, nil
	}

	config := mysql.NewConfig()

	config.Addr = fmt.Sprintf("%s:%d", da.configs.Host, da.configs.Port)
	config.User = da.configs.User
	config.Passwd = da.configs.Password
	config.Net = "tcp"
	config.DBName = databaseName
	config.TLSConfig = strconv.FormatBool(da.configs.SSLEnabled)

	db, err := sql.Open(DriverName, config.FormatDSN())
	if err != nil {
		return nil, gerr.WrapStr(fmt.Sprintf("failed to open database %q", databaseName), err)
	}

	if da.configs.MaxIdleConnections != 0 {
		db.SetMaxIdleConns(da.configs.MaxIdleConnections)
	}
	if da.configs.MaxOpenConnections != 0 {
		db.SetMaxOpenConns(da.configs.MaxOpenConnections)
	}
	if da.configs.ConnectionMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(da.configs.ConnectionMaxIdleTime)
	}
	if da.configs.ConnectionMaxLifetime != 0 {
		db.SetConnMaxLifetime(da.configs.ConnectionMaxLifetime)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, gerr.Wrap(errs.ErrDataAccess, err)
	}

	da.dbs[databaseName] = db

	return db, nil
}

func (da MySqlDataAccess) tableExists(ctx context.Context, table string, conn *sql.Conn) (bool, error) {
	const query = `SHOW TABLES;`

	fmt.Println(table)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}
	defer rows.Close()

	var t string
	for rows.Next() {
		rows.Scan(&t)
		fmt.Println(t)
		fmt.Println(table)
		if t == table {
			return true, nil
		}
	}

	fmt.Println("hello")

	if err := rows.Err(); err != nil {
		return false, gerr.Wrap(errs.ErrDataAccess, err)
	}

	fmt.Println("???????")
	return false, nil
}
