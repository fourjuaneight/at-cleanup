package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the Postgres connection pool
type Store struct {
	DB            *pgxpool.Pool
	encryptionKey []byte // 32 bytes for AES-256-GCM; nil means no encryption
}

// NewStore creates a new Postgres store and ensures the schema exists.
// encryptionKey must be exactly 32 bytes (AES-256) or nil to skip encryption.
func NewStore(dsn string, encryptionKey []byte) (*Store, error) {
	if encryptionKey != nil && len(encryptionKey) != 32 {
		return nil, fmt.Errorf("encryptionKey must be exactly 32 bytes, got %d", len(encryptionKey))
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	s := &Store{DB: pool, encryptionKey: encryptionKey}
	if err := s.ensureSchema(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure schema: %w", err)
	}

	return s, nil
}

// Close closes the connection pool
func (s *Store) Close() {
	s.DB.Close()
}

// encryptToken encrypts plaintext with AES-256-GCM. Returns plaintext unchanged if no key is set.
func (s *Store) encryptToken(plaintext string) (string, error) {
	if plaintext == "" || s.encryptionKey == nil {
		return plaintext, nil
	}
	block, err := aes.NewCipher(s.encryptionKey)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// decryptToken decrypts a token encrypted by encryptToken. Falls back to returning
// the raw value if decryption fails, to allow migration from pre-encryption data.
// Once all legacy plaintext tokens have been overwritten by the daemon, the
// fallback branches can be removed.
func (s *Store) decryptToken(ciphertext string) (string, error) {
	if ciphertext == "" || s.encryptionKey == nil {
		return ciphertext, nil
	}
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		// Not base64 — plaintext token written before encryption was introduced.
		return ciphertext, nil
	}
	block, err := aes.NewCipher(s.encryptionKey)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		// Too short to be ciphertext — treat as plaintext.
		return ciphertext, nil
	}
	plain, err := gcm.Open(nil, data[:nonceSize], data[nonceSize:], nil)
	if err != nil {
		// Decryption failed — treat as plaintext for the same migration reason above.
		return ciphertext, nil
	}
	return string(plain), nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	_, err := s.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS repo_cleanup_jobs (
			job_id              TEXT PRIMARY KEY,
			repo                TEXT NOT NULL,
			refresh_token       TEXT NOT NULL DEFAULT '',
			cleanup_types       TEXT[] NOT NULL DEFAULT '{}',
			delete_older_than   TIMESTAMPTZ NOT NULL,
			num_deleted         BIGINT NOT NULL DEFAULT 0,
			num_deleted_today   BIGINT NOT NULL DEFAULT 0,
			est_num_remaining   BIGINT NOT NULL DEFAULT 0,
			job_state           TEXT NOT NULL,
			created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_deleted_at     TIMESTAMPTZ
		);

		CREATE INDEX IF NOT EXISTS idx_repo_cleanup_jobs_repo
			ON repo_cleanup_jobs (repo);

		CREATE INDEX IF NOT EXISTS idx_repo_cleanup_jobs_job_state
			ON repo_cleanup_jobs (job_state);
	`)
	return err
}
