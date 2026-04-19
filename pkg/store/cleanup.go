package store

import (
	"context"
	"fmt"
	"time"
)

// RepoCleanupJob represents a repo cleanup job
type RepoCleanupJob struct {
	JobID           string     `json:"job_id"`
	Repo            string     `json:"repo"`
	RefreshToken    string     `json:"refresh_token"`
	CleanupTypes    []string   `json:"cleanup_types"`
	DeleteOlderThan time.Time  `json:"delete_older_than"`
	NumDeleted      int64      `json:"num_deleted"`
	NumDeletedToday int64      `json:"num_deleted_today"`
	EstNumRemaining int64      `json:"est_num_remaining"`
	JobState        string     `json:"job_state"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	LastDeletedAt   *time.Time `json:"last_deleted_at"`
}

// UpsertRepoCleanupJob creates or updates a repo cleanup job.
// Uses INSERT ... ON CONFLICT so updates are atomic — no ReplacingMergeTree workarounds needed.
func (s *Store) UpsertRepoCleanupJob(ctx context.Context, job RepoCleanupJob) (*RepoCleanupJob, error) {
	encToken, err := s.encryptToken(job.RefreshToken)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt refresh token: %w", err)
	}

	_, err = s.DB.Exec(ctx, `
		INSERT INTO repo_cleanup_jobs (
			job_id, repo, refresh_token, cleanup_types, delete_older_than,
			num_deleted, num_deleted_today, est_num_remaining, job_state,
			created_at, updated_at, last_deleted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (job_id) DO UPDATE SET
			refresh_token     = EXCLUDED.refresh_token,
			cleanup_types     = EXCLUDED.cleanup_types,
			delete_older_than = EXCLUDED.delete_older_than,
			num_deleted       = EXCLUDED.num_deleted,
			num_deleted_today = EXCLUDED.num_deleted_today,
			est_num_remaining = EXCLUDED.est_num_remaining,
			job_state         = EXCLUDED.job_state,
			updated_at        = EXCLUDED.updated_at,
			last_deleted_at   = EXCLUDED.last_deleted_at
	`,
		job.JobID,
		job.Repo,
		encToken,
		job.CleanupTypes,
		job.DeleteOlderThan,
		job.NumDeleted,
		job.NumDeletedToday,
		job.EstNumRemaining,
		job.JobState,
		job.CreatedAt,
		job.UpdatedAt,
		job.LastDeletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert repo cleanup job: %w", err)
	}

	return &job, nil
}

func (s *Store) scanJob(row interface {
	Scan(...any) error
}) (*RepoCleanupJob, error) {
	job := &RepoCleanupJob{}
	var encToken string
	if err := row.Scan(
		&job.JobID,
		&job.Repo,
		&encToken,
		&job.CleanupTypes,
		&job.DeleteOlderThan,
		&job.NumDeleted,
		&job.NumDeletedToday,
		&job.EstNumRemaining,
		&job.JobState,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.LastDeletedAt,
	); err != nil {
		return nil, err
	}
	var err error
	job.RefreshToken, err = s.decryptToken(encToken)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt refresh token: %w", err)
	}
	return job, nil
}

const selectCols = `
	job_id, repo, refresh_token, cleanup_types, delete_older_than,
	num_deleted, num_deleted_today, est_num_remaining, job_state,
	created_at, updated_at, last_deleted_at `

// GetRepoCleanupJob retrieves a specific cleanup job by ID
func (s *Store) GetRepoCleanupJob(ctx context.Context, jobID string) (*RepoCleanupJob, error) {
	row := s.DB.QueryRow(ctx,
		`SELECT`+selectCols+`FROM repo_cleanup_jobs WHERE job_id = $1`,
		jobID,
	)
	job, err := s.scanJob(row)
	if err != nil {
		return nil, fmt.Errorf("failed to get repo cleanup job: %w", err)
	}
	return job, nil
}

// GetCleanupJobsByRepo retrieves cleanup jobs for a specific repo
func (s *Store) GetCleanupJobsByRepo(ctx context.Context, repo string, limit int) ([]RepoCleanupJob, error) {
	rows, err := s.DB.Query(ctx,
		`SELECT`+selectCols+`FROM repo_cleanup_jobs
		 WHERE repo = $1
		 ORDER BY updated_at DESC
		 LIMIT $2`,
		repo, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query cleanup jobs by repo: %w", err)
	}
	defer rows.Close()

	var jobs []RepoCleanupJob
	for rows.Next() {
		job, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan cleanup job: %w", err)
		}
		jobs = append(jobs, *job)
	}
	return jobs, rows.Err()
}

// GetRunningCleanupJobsByRepo retrieves running cleanup jobs for a specific repo
func (s *Store) GetRunningCleanupJobsByRepo(ctx context.Context, repo string) ([]RepoCleanupJob, error) {
	rows, err := s.DB.Query(ctx,
		`SELECT`+selectCols+`FROM repo_cleanup_jobs
		 WHERE repo = $1 AND job_state = 'running'
		 ORDER BY updated_at DESC`,
		repo,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query running cleanup jobs: %w", err)
	}
	defer rows.Close()

	var jobs []RepoCleanupJob
	for rows.Next() {
		job, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan cleanup job: %w", err)
		}
		jobs = append(jobs, *job)
	}
	return jobs, rows.Err()
}

// GetRunningCleanupJobs retrieves all running cleanup jobs up to a limit
func (s *Store) GetRunningCleanupJobs(ctx context.Context, limit int) ([]RepoCleanupJob, error) {
	rows, err := s.DB.Query(ctx,
		`SELECT`+selectCols+`FROM repo_cleanup_jobs
		 WHERE job_state = 'running'
		 ORDER BY updated_at ASC
		 LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query running cleanup jobs: %w", err)
	}
	defer rows.Close()

	var jobs []RepoCleanupJob
	for rows.Next() {
		job, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan cleanup job: %w", err)
		}
		jobs = append(jobs, *job)
	}
	return jobs, rows.Err()
}

// CleanupStats represents aggregated cleanup statistics
type CleanupStats struct {
	TotalNumDeleted int64  `json:"total_num_deleted"`
	NumJobs         int64  `json:"num_jobs"`
	NumRepos        int64  `json:"num_repos"`
}

// GetCleanupStats retrieves aggregate cleanup statistics
func (s *Store) GetCleanupStats(ctx context.Context) (*CleanupStats, error) {
	stats := &CleanupStats{}
	err := s.DB.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(num_deleted), 0) AS total_num_deleted,
			COUNT(*)                       AS num_jobs,
			COUNT(DISTINCT repo)           AS num_repos
		FROM repo_cleanup_jobs
	`).Scan(&stats.TotalNumDeleted, &stats.NumJobs, &stats.NumRepos)
	if err != nil {
		return nil, fmt.Errorf("failed to get cleanup stats: %w", err)
	}
	return stats, nil
}
