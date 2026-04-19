package endpoints

import "log/slog"

// GetCleanupStatusRequest for GET /repo/cleanup
type GetCleanupStatusRequest struct {
	JobID string `query:"job_id"`
	DID   string `query:"did"`
}

// CancelCleanupJobRequest for DELETE /repo/cleanup
type CancelCleanupJobRequest struct {
	JobID string `query:"job_id"`
}

// CleanupOldRecordsRequest for POST /repo/cleanup
type CleanupOldRecordsRequest struct {
	Identifier          string   `json:"identifier"`
	AppPassword         string   `json:"app_password"`
	CleanupTypes        []string `json:"cleanup_types"`
	DeleteUntilDaysAgo  int      `json:"delete_until_days_ago"`
	ActuallyDeleteStuff bool     `json:"actually_delete_stuff"`
}

// LogValue redacts AppPassword so it is never written to structured logs.
func (r CleanupOldRecordsRequest) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("identifier", r.Identifier),
		slog.String("app_password", "[REDACTED]"),
		slog.Any("cleanup_types", r.CleanupTypes),
		slog.Int("delete_until_days_ago", r.DeleteUntilDaysAgo),
		slog.Bool("actually_delete_stuff", r.ActuallyDeleteStuff),
	)
}
