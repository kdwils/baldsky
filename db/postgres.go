package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgdriver "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/kdwils/baldsky/logger"
)

//go:embed schema/migrations/*.sql
var migrations embed.FS

type Postgres struct {
	DB *sql.DB
}

func New(ctx context.Context, dsn string, reconnectDelay time.Duration) (*Postgres, error) {
	log := logger.FromContext(ctx)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	ticker := time.NewTicker(reconnectDelay)
	defer ticker.Stop()

	if err := db.Ping(); err != nil {
		log.Error("failed to connect to database, retrying", "err", err, "delay", reconnectDelay)
		for {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("connect to database: %w", ctx.Err())
			case <-ticker.C:
				if err := db.Ping(); err != nil {
					log.Error("failed to connect to database, retrying", "err", err, "delay", reconnectDelay)
					continue
				}
				return &Postgres{DB: db}, nil
			}
		}
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
