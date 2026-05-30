package storages

import (
	"context"
	"database/sql"

	storagesPb "lms-storage-service/pb/storages"
)

// StorageRepository struct
type StorageRepository struct {
	Db *sql.DB
}

// Create inserts a new storage record
func (r *StorageRepository) Create(ctx context.Context, storage *storagesPb.Storage) error {
	query := `
		INSERT INTO storages (file_name, file_type, file_size, bucket, path, url, updated_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, updated_at, created_at
	`

	var updatedBy *string
	if storage.UpdatedBy != "" {
		updatedBy = &storage.UpdatedBy
	}

	return r.Db.QueryRowContext(ctx, query,
		storage.FileName, storage.FileType, storage.FileSize,
		storage.Bucket, storage.Path, storage.Url,
		updatedBy,
	).Scan(&storage.Id, &storage.UpdatedAt, &storage.CreatedAt)
}

// Get retrieves a storage record by ID (excluding soft-deleted)
func (r *StorageRepository) Get(ctx context.Context, id string) (*storagesPb.Storage, error) {
	query := `
		SELECT id, file_name, file_type, file_size, bucket, path,
			COALESCE(url, ''), is_used, COALESCE(updated_by::text, ''),
			updated_at, created_at
		FROM storages
		WHERE id = $1 AND deleted_at IS NULL
	`

	var storage storagesPb.Storage
	err := r.Db.QueryRowContext(ctx, query, id).Scan(
		&storage.Id, &storage.FileName, &storage.FileType,
		&storage.FileSize, &storage.Bucket, &storage.Path,
		&storage.Url, &storage.IsUsed, &storage.UpdatedBy,
		&storage.UpdatedAt, &storage.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &storage, nil
}

// SoftDelete marks a storage record as deleted
func (r *StorageRepository) SoftDelete(ctx context.Context, id string) error {
	query := `
		UPDATE storages SET deleted_at = timezone('utc', NOW())
		WHERE id = $1 AND deleted_at IS NULL
	`

	result, err := r.Db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// ConfirmIsUsed marks a storage record as used
func (r *StorageRepository) ConfirmIsUsed(ctx context.Context, id string) error {
	query := `
		UPDATE storages SET is_used = TRUE, updated_at = timezone('utc', NOW())
		WHERE id = $1 AND deleted_at IS NULL
	`

	_, err := r.Db.ExecContext(ctx, query, id)
	return err
}

// GetUnusedFiles retrieves storage records that are not marked as used
func (r *StorageRepository) GetUnusedFiles(ctx context.Context) ([]*storagesPb.Storage, error) {
	query := `
		SELECT id, file_name, file_type, file_size, bucket, path,
			COALESCE(url, ''), is_used, COALESCE(updated_by::text, ''),
			updated_at, created_at
		FROM storages
		WHERE is_used = FALSE AND deleted_at IS NULL
			AND created_at < timezone('utc', NOW()) - INTERVAL '24 hours'
	`

	rows, err := r.Db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var storages []*storagesPb.Storage
	for rows.Next() {
		var s storagesPb.Storage
		err := rows.Scan(
			&s.Id, &s.FileName, &s.FileType,
			&s.FileSize, &s.Bucket, &s.Path,
			&s.Url, &s.IsUsed, &s.UpdatedBy,
			&s.UpdatedAt, &s.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		storages = append(storages, &s)
	}

	return storages, nil
}
