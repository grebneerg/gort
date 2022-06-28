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

	"go.opentelemetry.io/otel"

	"github.com/getgort/gort/data"
	"github.com/getgort/gort/dataaccess/errs"
	gerr "github.com/getgort/gort/errors"
	"github.com/getgort/gort/telemetry"
)

func (da MySqlDataAccess) DynamicConfigurationCreate(ctx context.Context, dc data.DynamicConfiguration) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.DynamicConfigurationCreate")
	defer sp.End()

	if err := validate(dc.Layer, dc.Bundle, dc.Owner, dc.Key); err != nil {
		return err
	}

	if exists, err := da.DynamicConfigurationExists(ctx, dc.Layer, dc.Bundle, dc.Owner, dc.Key); err != nil {
		return err
	} else if exists {
		return errs.ErrConfigExists
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := `INSERT INTO configs
		(bundle_name, layer, owner, thekey, secret, value)
		VALUES (?, ?, ?, ?, ?, ?);`
	_, err = conn.ExecContext(ctx, query, dc.Bundle, dc.Layer, dc.Owner, dc.Key, dc.Secret, dc.Value)
	if err != nil {
		return gerr.Wrap(errs.ErrDataAccess, err)
	}

	return nil
}

func (da MySqlDataAccess) DynamicConfigurationDelete(ctx context.Context, layer data.ConfigurationLayer, bundle, owner, thekey string) error {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.DynamicConfigurationDelete")
	defer sp.End()

	if err := validate(layer, bundle, owner, thekey); err != nil {
		return err
	}

	if exists, err := da.DynamicConfigurationExists(ctx, layer, bundle, owner, thekey); err != nil {
		return err
	} else if !exists {
		return errs.ErrNoSuchConfig
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := "DELETE FROM configs WHERE bundle_name=? AND layer=? AND owner=? AND thekey=?;"
	_, err = conn.ExecContext(ctx, query, bundle, layer, owner, thekey)
	if err != nil {
		err = gerr.Wrap(errs.ErrDataAccess, err)
	}

	return err
}

func (da MySqlDataAccess) DynamicConfigurationExists(ctx context.Context, layer data.ConfigurationLayer, bundle, owner, thekey string) (bool, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.DynamicConfigurationExists")
	defer sp.End()

	if err := validate(layer, bundle, owner, thekey); err != nil {
		return false, err
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	query := "SELECT EXISTS(SELECT 1 FROM configs WHERE bundle_name=? AND layer=? AND owner=? AND thekey=?);"
	exists := false

	err = conn.QueryRowContext(ctx, query, bundle, layer, owner, thekey).Scan(&exists)
	if err != nil {
		return false, gerr.Wrap(errs.ErrNoSuchGroup, err)
	}

	return exists, nil
}

func (da MySqlDataAccess) DynamicConfigurationGet(ctx context.Context, layer data.ConfigurationLayer, bundle, owner, thekey string) (data.DynamicConfiguration, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.DynamicConfigurationGet")
	defer sp.End()

	if err := validate(layer, bundle, owner, thekey); err != nil {
		return data.DynamicConfiguration{}, err
	}

	if exists, err := da.DynamicConfigurationExists(ctx, layer, bundle, owner, thekey); err != nil {
		return data.DynamicConfiguration{}, err
	} else if !exists {
		return data.DynamicConfiguration{}, errs.ErrNoSuchConfig
	}

	conn, err := da.connect(ctx)
	if err != nil {
		return data.DynamicConfiguration{}, err
	}
	defer conn.Close()

	query := `SELECT bundle_name, layer, owner, thekey, value, secret
		FROM configs
		WHERE bundle_name=? AND layer=? AND owner=? AND thekey=?;`
	dc := data.DynamicConfiguration{}

	err = conn.QueryRowContext(ctx, query, bundle, layer, owner, thekey).
		Scan(&dc.Bundle, &dc.Layer, &dc.Owner, &dc.Key, &dc.Value, &dc.Secret)

	if err == sql.ErrNoRows {
		return dc, errs.ErrNoSuchGroup
	} else if err != nil {
		return dc, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return dc, nil
}

func (da MySqlDataAccess) DynamicConfigurationList(ctx context.Context, layer data.ConfigurationLayer, bundle, owner, thekey string) ([]data.DynamicConfiguration, error) {
	tr := otel.GetTracerProvider().Tracer(telemetry.ServiceName)
	ctx, sp := tr.Start(ctx, "mysql.DynamicConfigurationList")
	defer sp.End()

	if bundle == "" {
		return nil, errs.ErrEmptyConfigBundle
	}
	if layer == "" {
		layer = "%"
	}
	if owner == "" {
		owner = "%"
	}
	if thekey == "" {
		thekey = "%"
	}

	var dcs = make([]data.DynamicConfiguration, 0)

	conn, err := da.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	query := `SELECT bundle_name, layer, owner, thekey, value, secret
		FROM configs
		WHERE bundle_name LIKE ? AND layer LIKE ? AND owner LIKE ? AND thekey LIKE ?`

	rows, err := conn.QueryContext(ctx, query, bundle, layer, owner, thekey)
	if err != nil {
		return nil, gerr.Wrap(errs.ErrDataAccess, err)
	}

	for rows.Next() {
		dc := data.DynamicConfiguration{}

		err = rows.Scan(&dc.Bundle, &dc.Layer, &dc.Owner, &dc.Key, &dc.Value, &dc.Secret)
		if err != nil {
			return nil, gerr.Wrap(errs.ErrNoSuchGroup, err)
		}

		dcs = append(dcs, dc)
	}

	if rows.Err(); err != nil {
		return nil, gerr.Wrap(errs.ErrDataAccess, err)
	}

	return dcs, nil
}

func validate(layer data.ConfigurationLayer, bundle, owner, thekey string) error {
	switch {
	case bundle == "":
		return errs.ErrEmptyConfigBundle
	case layer == "":
		return errs.ErrEmptyConfigLayer
	case layer.Validate() != nil:
		return layer.Validate()
	case owner == "" && layer != data.LayerBundle:
		return errs.ErrEmptyConfigOwner
	case thekey == "":
		return errs.ErrEmptyConfigKey
	}

	return nil
}
