package db

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgdriver "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema/migrations/*.sql
var migrations embed.FS

type Postgres struct {
	DB *sql.DB
}

func New(dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Postgres{DB: db}, nil
}

func (p *Postgres) Migrate() error {
	src, err := iofs.New(migrations, "schema/migrations")
	if err != nil {
		return fmt.Errorf("create migration source: %w", err)
	}

	driver, err := pgdriver.WithInstance(p.DB, &pgdriver.Config{})
	if err != nil {
		return fmt.Errorf("create migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("run migrations: %w", err)
	}

	return nil
}

func (p *Postgres) Close() error {
	return p.DB.Close()
}
