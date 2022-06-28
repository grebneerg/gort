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
	"sort"

	"go.opentelemetry.io/otel"

	"github.com/getgort/gort/data/rest"
	"github.com/getgort/gort/dataaccess/errs"
	gerr "github.com/getgort/gort/errors"
	"github.com/getgort/gort/telemetry"
)

// GroupCreate creates a new user group.
func (da MySqlDataAccess) GroupCreate(ctx context.Context, group rest.Group) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupCreate")
	defer sp.End()

	if group.Name == "" {
		return errs.ErrEmptyGroupName
	}

	exists, err := da.GroupExists(ctx, group.Name)
	if err != nil {
		return err
	}
	if exists {
		return errs.ErrGroupExists
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := `INSERT INTO groups (groupname) VALUES (?);`
	_, err = conn.ExecContext(ctx, query, group.Name)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

// GroupDelete deletes a group.
func (da MySqlDataAccess) GroupDelete(ctx context.Context, groupname string) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupDelete")
	defer sp.End()

	if groupname == "" {
		return errs.ErrEmptyGroupName
	}

	// Thou Shalt Not Delete Admin
	if groupname == "admin" {
		return errs.ErrAdminUndeletable
	}

	exists, err := da.GroupExists(ctx, groupname)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchGroup
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := `DELETE FROM groupusers WHERE groupname=?;`
	_, err = conn.ExecContext(ctx, query, groupname)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	query = `DELETE FROM groups WHERE groupname=?;`
	_, err = conn.ExecContext(ctx, query, groupname)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

// GroupExists is used to determine whether a group exists in the data store.
func (da MySqlDataAccess) GroupExists(ctx context.Context, groupname string) (bool, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupExists")
	defer sp.End()

	conn, err := da.connect(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	query := "SELECT EXISTS(SELECT 1 FROM groups WHERE groupname=?);"
	exists := false

	err = conn.QueryRowContext(ctx, query, groupname).Scan(&exists)
	if err != nil {
		return false, gerr.Wrap(errs.ErrNoSuchGroup, err)
	}

	return exists, nil
}

// GroupGet gets a specific group.
func (da MySqlDataAccess) GroupGet(ctx context.Context, groupname string) (rest.Group, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupGet")
	defer sp.End()

	if groupname == "" {
		return rest.Group{}, errs.ErrEmptyGroupName
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return rest.Group{}, err
	}
	defer conn.Close()

	// There will be more fields here eventually
	query := `SELECT groupname
		FROM groups
		WHERE groupname=?;`

	group := rest.Group{}
	err = conn.QueryRowContext(ctx, query, groupname).Scan(&group.Name)
	if err == sql.ErrNoRows {
		return group, errs.ErrNoSuchGroup
	} else if err != nil {
		return group, gerr.Wrap(errs.ErrDataAccess, err)
	}

	users, err := da.GroupUserList(ctx, groupname)
	if err != nil {
		return group, err
	}

	group.Users = users

	return group, nil
}

// GroupList returns a list of all known groups in the datastore.
// Passwords are not included. Nice try.
func (da MySqlDataAccess) GroupList(ctx context.Context) ([]rest.Group, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupList")
	defer sp.End()

	groups := make([]rest.Group, 0)

	conn, err := da.connect(ctx)
	if err != nil {
		return groups, err
	}
	defer conn.Close()

	query := `SELECT groupname FROM groups`
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return groups, gerr.Wrap(errs.ErrDataAccess, err)
	}

	for rows.Next() {
		group := rest.Group{}

		err = rows.Scan(&group.Name)
		if err != nil {
			return groups, gerr.Wrap(errs.ErrNoSuchGroup, err)
		}

		groups = append(groups, group)
	}

	if rows.Err(); err != nil {
		return nil, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return groups, nil
}

func (da MySqlDataAccess) GroupPermissionList(ctx context.Context, groupname string) (rest.RolePermissionList, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupPermissionList")
	defer sp.End()

	roles, err := da.GroupRoleList(ctx, groupname)
	if err != nil {
		return rest.RolePermissionList{}, err
	}

	mp := map[string]rest.RolePermission{}

	for _, r := range roles {
		rpl, err := da.RolePermissionList(ctx, r.Name)
		if err != nil {
			return rest.RolePermissionList{}, err
		}

		for _, rp := range rpl {
			mp[rp.String()] = rp
		}
	}

	pp := []rest.RolePermission{}

	for _, p := range mp {
		pp = append(pp, p)
	}

	sort.Slice(pp, func(i, j int) bool { return pp[i].String() < pp[j].String() })

	return pp, nil
}

// GroupRoleAdd grants one or more roles to a group.
func (da MySqlDataAccess) GroupRoleAdd(ctx context.Context, groupname, rolename string) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupRoleAdd")
	defer sp.End()

	if rolename == "" {
		return errs.ErrEmptyRoleName
	}

	exists, err := da.GroupExists(ctx, groupname)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchGroup
	}

	exists, err = da.RoleExists(ctx, rolename)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchRole
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := `INSERT INTO group_roles (group_name, role_name)
		VALUES (?, ?);`
	_, err = conn.ExecContext(ctx, query, groupname, rolename)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

// GroupRoleDelete revokes a role from a group.
func (da MySqlDataAccess) GroupRoleDelete(ctx context.Context, groupname, rolename string) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupRoleDelete")
	defer sp.End()

	if groupname == "" {
		return errs.ErrEmptyGroupName
	}

	if rolename == "" {
		return errs.ErrEmptyRoleName
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := `DELETE FROM group_roles
		WHERE group_name=? AND role_name=?;`
	_, err = conn.ExecContext(ctx, query, groupname, rolename)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

func (da MySqlDataAccess) GroupRoleList(ctx context.Context, groupname string) ([]rest.Role, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupRoleList")
	defer sp.End()

	if groupname == "" {
		return nil, errs.ErrEmptyGroupName
	}

	exists, err := da.GroupExists(ctx, groupname)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errs.ErrNoSuchGroup
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	query := `SELECT role_name
		FROM group_roles
		WHERE group_name = ?
		ORDER BY role_name;`

	rows, err := conn.QueryContext(ctx, query, groupname)
	if err != nil {
		return nil, gerr.Wrap(errs.ErrDataAccess, err)
	}

	roles := []rest.Role{}

	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return nil, gerr.Wrap(errs.ErrDataAccess, err)
		}

		role, err := da.RoleGet(ctx, name)
		if err != nil {
			return nil, err
		}

		roles = append(roles, role)
	}

	return roles, nil
}

// GroupUpdate is used to update an existing group. An error is returned if the
// groupname is empty or if the group doesn't exist.
// TODO Should we let this create groups that don't exist?
func (da MySqlDataAccess) GroupUpdate(ctx context.Context, group rest.Group) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupUpdate")
	defer sp.End()

	if group.Name == "" {
		return errs.ErrEmptyGroupName
	}

	exists, err := da.UserExists(ctx, group.Name)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchGroup
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// There will be more eventually
	query := `UPDATE groupname
	SET groupname=?
	WHERE groupname=?;`

	_, err = conn.ExecContext(ctx, query, group.Name)
	if err != nil {
		err = gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

// GroupUserAdd adds a user to a group
func (da MySqlDataAccess) GroupUserAdd(ctx context.Context, groupname string, username string) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupUserAdd")
	defer sp.End()

	if groupname == "" {
		return errs.ErrEmptyGroupName
	}

	exists, err := da.GroupExists(ctx, groupname)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchGroup
	}

	if username == "" {
		return errs.ErrEmptyUserName
	}

	exists, err = da.UserExists(ctx, username)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchUser
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := `INSERT INTO groupusers (groupname, username) VALUES (?, ?);`
	_, err = conn.ExecContext(ctx, query, groupname, username)
	if err != nil {
		err = gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

// GroupUserDelete removes a user from a group.
func (da MySqlDataAccess) GroupUserDelete(ctx context.Context, groupname string, username string) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupUserDelete")
	defer sp.End()

	if groupname == "" {
		return errs.ErrEmptyGroupName
	}

	exists, err := da.GroupExists(ctx, groupname)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrNoSuchGroup
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := "DELETE FROM groupusers WHERE groupname=? AND username=?;"
	_, err = conn.ExecContext(ctx, query, groupname, username)
	if err != nil {
		err = gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

// GroupUserList returns a list of all known users in a group.
func (da MySqlDataAccess) GroupUserList(ctx context.Context, groupname string) ([]rest.User, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.GroupUserList")
	defer sp.End()

	users := make([]rest.User, 0)

	if groupname == "" {
		return users, errs.ErrEmptyGroupName
	}

	exists, err := da.GroupExists(ctx, groupname)
	if err != nil {
		return users, err
	}
	if !exists {
		return users, errs.ErrNoSuchGroup
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return users, err
	}
	defer conn.Close()

	query := `SELECT email, full_name, username
	FROM users
	WHERE username IN (
		SELECT username
		FROM groupusers
		WHERE groupname = ?
	)`

	rows, err := conn.QueryContext(ctx, query, groupname)
	if err != nil {
		return users, gerr.Wrap(errs.ErrDataAccess, err)
	}

	for rows.Next() {
		user := rest.User{}

		err = rows.Scan(&user.Email, &user.FullName, &user.Username)
		if err != nil {
			return users, gerr.Wrap(errs.ErrNoSuchUser, err)
		}

		user.Mappings, err = da.doUserGetAdapterIDs(ctx, user.Username)
		if err != nil {
			return users, err
		}

		users = append(users, user)
	}

	if rows.Err(); err != nil {
		return nil, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return users, nil
}
