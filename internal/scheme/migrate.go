package scheme

import (
	"database/sql"

	"github.com/GuiaBolso/darwin"
)

var migrations = []darwin.Migration{
	{
		Version:     1,
		Description: "Create uuid extension",
		Script:      `CREATE EXTENSION "uuid-ossp";`,
	},
	{
		Version:     2,
		Description: "Create storages table",
		Script: `CREATE TABLE storages (
			id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
			file_name VARCHAR(255) NOT NULL,
			file_type VARCHAR(10) NOT NULL,
			file_size BIGINT NOT NULL,
			bucket VARCHAR(100) NOT NULL,
			path VARCHAR(500) NOT NULL,
			url VARCHAR(500),
			is_used BOOLEAN DEFAULT FALSE,
			updated_by UUID,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', NOW()),
			created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', NOW()),
			deleted_at TIMESTAMPTZ
		);`,
	},
}

// Migrate attempts to bring the schema for db up to date with the migrations
// defined in this package.
func Migrate(db *sql.DB) error {
	driver := darwin.NewGenericDriver(db, darwin.PostgresDialect{})

	d := darwin.New(driver, migrations, nil)

	return d.Migrate()
}
