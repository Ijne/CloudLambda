package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"api/internal/vars"
)

var ErrNotFound = errors.New("function not found")

type Function struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Runtime     vars.EnvType `json:"runtime"`
	CodePath    string       `json:"code_path"`
	MemoryLimit int64        `json:"memory_limit"`
	CPUQuota    int64        `json:"cpu_quota"`
	TimeoutSec  int          `json:"timeout_sec"`
	CreatedAt   time.Time    `json:"created_at"`
}

func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS functions (
			id           TEXT        PRIMARY KEY,
			name         TEXT        NOT NULL,
			runtime      TEXT        NOT NULL,
			code_path    TEXT        NOT NULL,
			memory_limit BIGINT      NOT NULL DEFAULT 536870912,
			cpu_quota    BIGINT      NOT NULL DEFAULT 50000,
			timeout_sec  INT         NOT NULL DEFAULT 10,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

type FunctionRepo struct {
	pool *pgxpool.Pool
}

func NewFunctionRepo(pool *pgxpool.Pool) *FunctionRepo {
	return &FunctionRepo{pool: pool}
}

func (r *FunctionRepo) Save(ctx context.Context, f *Function) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO functions (id, name, runtime, code_path, memory_limit, cpu_quota, timeout_sec, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, f.ID, f.Name, f.Runtime, f.CodePath, f.MemoryLimit, f.CPUQuota, f.TimeoutSec, time.Now())
	if err != nil {
		return fmt.Errorf("save function: %w", err)
	}
	return nil
}

func (r *FunctionRepo) Get(ctx context.Context, id string) (*Function, error) {
	f := &Function{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, runtime, code_path, memory_limit, cpu_quota, timeout_sec, created_at
		FROM functions WHERE id = $1
	`, id).Scan(&f.ID, &f.Name, &f.Runtime, &f.CodePath, &f.MemoryLimit, &f.CPUQuota, &f.TimeoutSec, &f.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get function: %w", err)
	}
	return f, nil
}

func (r *FunctionRepo) List(ctx context.Context) ([]*Function, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, runtime, code_path, memory_limit, cpu_quota, timeout_sec, created_at
		FROM functions ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list functions: %w", err)
	}
	defer rows.Close()

	var out []*Function
	for rows.Next() {
		f := &Function{}
		if err := rows.Scan(&f.ID, &f.Name, &f.Runtime, &f.CodePath, &f.MemoryLimit, &f.CPUQuota, &f.TimeoutSec, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan function: %w", err)
		}
		out = append(out, f)
	}
	return out, nil
}

func (r *FunctionRepo) Delete(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx, `DELETE FROM functions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete function: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
