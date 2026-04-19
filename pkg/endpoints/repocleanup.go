package endpoints

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/fourjuaneight/at-cleanup/pkg/store"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

var cleanupUserAgent = "jaz-repo-cleanup-tool/0.0.1"

// validCleanupTypes is the exhaustive set of accepted cleanup type values.
var validCleanupTypes = []string{"post", "post_with_media", "repost", "like"}

// maxCARBytes caps the in-memory size of a repo CAR file. The xrpc client has no
// built-in streaming for SyncGetRepo, so we limit via the transport layer instead.
const maxCARBytes = 500 * 1024 * 1024

// limitedTransport wraps an http.RoundTripper and caps response bodies to maxBytes.
type limitedTransport struct {
	wrapped  http.RoundTripper
	maxBytes int64
}

func (t *limitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(resp.Body, t.maxBytes),
		Closer: resp.Body,
	}
	return resp, nil
}

func newLimitedClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &limitedTransport{
			wrapped:  http.DefaultTransport,
			maxBytes: maxCARBytes,
		},
	}
}

func (api *API) CleanupOldRecords(c echo.Context) error {
	ctx := c.Request().Context()

	var req CleanupOldRecordsRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": fmt.Errorf("error parsing request: %w", err).Error()})
	}

	res, err := api.enqueueCleanupJob(ctx, req)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Errorf("error cleaning up records: %w", err).Error()})
	}

	return c.JSON(http.StatusOK, res)
}

func (api *API) GetCleanupStatus(c echo.Context) error {
	ctx := c.Request().Context()

	var req GetCleanupStatusRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if req.JobID != "" {
		job, err := api.Store.GetRepoCleanupJob(ctx, req.JobID)
		if err != nil {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
		}
		job.RefreshToken = ""
		return c.JSON(http.StatusOK, job)
	}

	if req.DID != "" {
		// Validate the DID before hitting the database.
		if _, err := syntax.ParseDID(req.DID); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid DID format"})
		}

		jobs, err := api.Store.GetCleanupJobsByRepo(ctx, req.DID, 100)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Errorf("error getting jobs: %w", err).Error()})
		}
		if len(jobs) == 0 {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "no jobs found for the provided DID"})
		}
		for i := range jobs {
			jobs[i].RefreshToken = ""
		}
		return c.JSON(http.StatusOK, jobs)
	}

	return c.JSON(http.StatusBadRequest, map[string]string{"error": "must specify either job_id or did in query params"})
}

func (api *API) GetCleanupStats(c echo.Context) error {
	ctx := c.Request().Context()

	cleanupStats, err := api.Store.GetCleanupStats(ctx)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Errorf("error getting cleanup metadata: %w", err).Error()})
	}

	return c.JSON(http.StatusOK, cleanupStats)
}

func (api *API) CancelCleanupJob(c echo.Context) error {
	ctx := c.Request().Context()

	var req CancelCleanupJobRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if req.JobID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "must specify job_id in query params"})
	}

	job, err := api.Store.GetRepoCleanupJob(ctx, req.JobID)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}

	if job.JobState == "completed" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "job already completed"})
	}

	job.JobState = "cancelled"
	job.RefreshToken = ""
	job.UpdatedAt = time.Now().UTC()

	_, err = api.Store.UpsertRepoCleanupJob(ctx, *job)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Errorf("error updating job: %w", err).Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "job cancelled"})
}

type cleanupInfo struct {
	NumEnqueued int    `json:"num_enqueued"`
	DryRun      bool   `json:"dry_run"`
	Message     string `json:"message,omitempty"`
	JobID       string `json:"job_id,omitempty"`
}

func (api *API) enqueueCleanupJob(ctx context.Context, req CleanupOldRecordsRequest) (*cleanupInfo, error) {
	log := slog.With("source", "cleanup_old_records_handler")

	// Validate DeleteUntilDaysAgo — zero or negative would match all records.
	if req.DeleteUntilDaysAgo < 1 {
		return nil, fmt.Errorf("delete_until_days_ago must be at least 1")
	}

	// Validate CleanupTypes against the known allow-list.
	for _, ct := range req.CleanupTypes {
		if !slices.Contains(validCleanupTypes, ct) {
			return nil, fmt.Errorf("invalid cleanup_type %q: must be one of %v", ct, validCleanupTypes)
		}
	}
	if len(req.CleanupTypes) == 0 {
		return nil, fmt.Errorf("cleanup_types must not be empty")
	}

	var ident *identity.Identity

	atID, err := syntax.ParseAtIdentifier(req.Identifier)
	if err == nil && atID != nil {
		ident, err = api.Directory.Lookup(ctx, *atID)
		if err != nil {
			log.Error("Error looking up identity", "error", err)
			return nil, fmt.Errorf("error looking up identity: %w", err)
		}
	} else if emailAddr, err := mail.ParseAddress(req.Identifier); err == nil && emailAddr != nil {
		// Identifier is a valid email address
	} else {
		log.Error("Failed to parse identifier as at-identifier or email address", "identifier", req.Identifier, "error", err)
		return nil, fmt.Errorf("failed to parse identifier as at-identifier or email address: %w", err)
	}

	pdsHost := "https://bsky.social"
	if ident != nil &&
		ident.PDSEndpoint() != "" &&
		!strings.HasSuffix(ident.PDSEndpoint(), ".host.bsky.network") {
		pdsHost = ident.PDSEndpoint()
	}

	client := xrpc.Client{
		Client:    newLimitedClient(5 * time.Minute),
		Host:      pdsHost,
		UserAgent: &cleanupUserAgent,
	}

	if api.MagicHeaderVal != "" {
		client.Headers = map[string]string{"x-ratelimit-bypass": api.MagicHeaderVal}
	}

	log = log.With("identifier", req.Identifier)

	out, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: req.Identifier,
		Password:   req.AppPassword,
	})
	if err != nil {
		log.Error("Error logging in", "error", err)
		return nil, fmt.Errorf("error logging in: %w", err)
	}

	client.Auth = &xrpc.AuthInfo{
		Did:        out.Did,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	log = log.With("did", out.Did)
	log = log.With("handle", out.Handle)

	// Prevent concurrent enqueue for the same DID.
	if _, loaded := api.inProgress.LoadOrStore(out.Did, struct{}{}); loaded {
		return &cleanupInfo{
			Message: "A job for this account is currently being created, please try again shortly.",
		}, nil
	}
	defer api.inProgress.Delete(out.Did)

	existingJobs, err := api.Store.GetRunningCleanupJobsByRepo(ctx, out.Did)
	if err != nil {
		log.Error("Error getting existing jobs", "error", err)
	} else if len(existingJobs) > 0 {
		log.Info("Found existing job for DID, skipping")
		return &cleanupInfo{
			JobID:   existingJobs[0].JobID,
			Message: "Found existing active job for your account, you can only have one job running at a time.",
		}, nil
	}

	did, err := syntax.ParseDID(out.Did)
	if err != nil {
		log.Error("Error parsing DID", "error", err)
		return nil, fmt.Errorf("error parsing DID: %w", err)
	}

	if ident == nil {
		ident, err = api.Directory.LookupDID(ctx, did)
		if err != nil {
			log.Error("Error looking up DID", "error", err)
			return nil, fmt.Errorf("error looking up DID: %w", err)
		}
	}

	client.Host = ident.PDSEndpoint()

	if client.Host == "" {
		log.Error("No PDS endpoint found for DID")
		return nil, fmt.Errorf("no PDS endpoint found for DID")
	}

	log = log.With("pds", client.Host)

	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, did.String(), "")
	if err != nil {
		log.Error("Error getting repo from PDS", "error", err)
		return nil, fmt.Errorf("error getting repo from PDS: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		log.Error("Error reading repo CAR", "error", err)
		return nil, fmt.Errorf("error reading repo CAR: %w", err)
	}

	recordsToDelete := []string{}
	lk := sync.Mutex{}

	err = rr.ForEach(ctx, "app.bsky.feed.", func(path string, nodeCid cid.Cid) error {
		if !strings.HasPrefix(path, "app.bsky.feed.") {
			return nil
		}
		log := log.With("path", path)
		if strings.Contains(path, "threadgate") {
			return nil
		}
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Error("Error getting record", "error", err)
			return nil
		}

		switch rec := rec.(type) {
		case *bsky.FeedPost:
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}
			if createdAt.After(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				return nil
			}

			hasMedia := rec.Embed != nil && rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0

			if hasMedia {
				if slices.Contains(req.CleanupTypes, "post_with_media") {
					lk.Lock()
					recordsToDelete = append(recordsToDelete, path)
					lk.Unlock()
				}
				return nil
			}

			if slices.Contains(req.CleanupTypes, "post") {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedRepost:
			if !slices.Contains(req.CleanupTypes, "repost") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedLike:
			if !slices.Contains(req.CleanupTypes, "like") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		}

		return nil
	})
	if err != nil {
		log.Error("Error iterating over records", "error", err)
		return nil, fmt.Errorf("error iterating over records: %w", err)
	}

	log.Info("Found records to delete", "count", len(recordsToDelete))

	info := cleanupInfo{
		NumEnqueued: len(recordsToDelete),
		DryRun:      !req.ActuallyDeleteStuff,
		Message:     "Records will not be deleted",
	}
	if req.ActuallyDeleteStuff {
		now := time.Now().UTC()
		job := store.RepoCleanupJob{
			JobID:           uuid.New().String(),
			Repo:            did.String(),
			RefreshToken:    out.RefreshJwt,
			CleanupTypes:    req.CleanupTypes,
			DeleteOlderThan: time.Now().UTC().AddDate(0, 0, -req.DeleteUntilDaysAgo),
			NumDeleted:      0,
			NumDeletedToday: 0,
			EstNumRemaining: int64(len(recordsToDelete)),
			JobState:        "running",
			CreatedAt:       now,
			UpdatedAt:       now,
			LastDeletedAt:   nil,
		}

		createdJob, err := api.Store.UpsertRepoCleanupJob(ctx, job)
		if err != nil {
			log.Error("Error creating job", "error", err)
			return nil, fmt.Errorf("error creating job: %w", err)
		}

		info.Message = fmt.Sprintf("%d records enqueued for deletion", len(recordsToDelete))
		info.JobID = createdJob.JobID
		log.Info("Created cleanup job", "job_id", createdJob.JobID)
	}

	return &info, nil
}

// RunCleanupDaemon runs a daemon that periodically checks for cleanup jobs to run
func (api *API) RunCleanupDaemon(ctx context.Context) {
	log := slog.With("source", "cleanup_daemon")
	log.Info("Starting cleanup daemon")

	for {
		log.Info("Getting jobs to process")
		jobs, err := api.Store.GetRunningCleanupJobs(ctx, 100)
		if err != nil {
			log.Error("Error getting running jobs", "error", err)
			time.Sleep(30 * time.Second)
			continue
		}

		jobsToRun := []*store.RepoCleanupJob{}
		anHourAgo := time.Now().UTC().Add(-1 * time.Hour)
		aDayAgo := time.Now().UTC().Add(-24 * time.Hour)
		for i := range jobs {
			job := jobs[i]
			if job.LastDeletedAt == nil ||
				(job.LastDeletedAt.Before(anHourAgo) && job.NumDeletedToday < int64(maxDeletesPerDay)) ||
				(job.LastDeletedAt.Before(aDayAgo)) {
				jobsToRun = append(jobsToRun, &job)
			}
		}

		log.Info("Found jobs to run", "count", len(jobsToRun))

		if len(jobsToRun) == 0 {
			log.Info("No jobs to run, sleeping")
			time.Sleep(30 * time.Second)
			continue
		}

		wg := sync.WaitGroup{}
		sem := semaphore.NewWeighted(10)
		for i := range jobsToRun {
			job := jobsToRun[i]
			wg.Add(1)
			if err := sem.Acquire(ctx, 1); err != nil {
				log.Error("Error acquiring semaphore", "error", err)
				wg.Done()
				continue
			}
			go func(job store.RepoCleanupJob) {
				defer func() {
					sem.Release(1)
					wg.Done()
				}()
				log := log.With("job_id", job.JobID, "did", job.Repo)
				resJob, err := api.cleanupNextBatch(ctx, job)
				if err != nil {
					log.Error("Error cleaning up batch", "error", err)
					// Persist the updated timestamp so the daemon backs off rather
					// than immediately retrying the same job on the next tick.
					now := time.Now().UTC()
					resJob = &job
					resJob.LastDeletedAt = &now
					resJob.UpdatedAt = now
					if _, upsertErr := api.Store.UpsertRepoCleanupJob(ctx, *resJob); upsertErr != nil {
						log.Error("Error persisting job state after batch error", "error", upsertErr)
					}
					return
				}

				if resJob != nil {
					_, err = api.Store.UpsertRepoCleanupJob(ctx, *resJob)
					if err != nil {
						log.Error("Error updating job", "error", err)
					}
				}
			}(*job)
		}

		wg.Wait()
		log.Info("Finished running jobs, sleeping")
		time.Sleep(30 * time.Second)
	}
}

// maxDeletesPerHour / maxDeletesPerDay mirror the AT Protocol PDS rate limits.
// Staying under them avoids 429s that would stall the cleanup daemon.
var maxDeletesPerHour = 4000
var maxDeletesPerDay = 30_000

func (api *API) cleanupNextBatch(ctx context.Context, job store.RepoCleanupJob) (*store.RepoCleanupJob, error) {
	log := slog.With("source", "cleanup_next_batch", "job_id", job.JobID, "did", job.Repo)
	log.Info("Cleaning up next batch")

	if job.LastDeletedAt != nil && job.LastDeletedAt.UTC().Day() == time.Now().UTC().Day() && job.NumDeletedToday >= int64(maxDeletesPerDay) {
		log.Info("Already deleted max records today, skipping")
		return nil, nil
	} else if job.LastDeletedAt != nil && job.LastDeletedAt.UTC().Day() != time.Now().UTC().Day() {
		job.NumDeletedToday = 0
	}

	ident, err := api.Directory.LookupDID(ctx, syntax.DID(job.Repo))
	if err != nil {
		log.Error("Error looking up DID", "error", err)
		return nil, fmt.Errorf("error looking up DID: %w", err)
	}

	client := xrpc.Client{
		Client:    newLimitedClient(5 * time.Minute),
		Host:      "https://bsky.social",
		UserAgent: &cleanupUserAgent,
	}

	if api.MagicHeaderVal != "" {
		client.Headers = map[string]string{"x-ratelimit-bypass": api.MagicHeaderVal}
	}

	client.Auth = &xrpc.AuthInfo{
		Did:       job.Repo,
		AccessJwt: job.RefreshToken,
	}

	if !strings.HasSuffix(ident.PDSEndpoint(), ".host.bsky.network") {
		client.Host = ident.PDSEndpoint()
	}

	out, err := comatproto.ServerRefreshSession(ctx, &client)
	if err != nil {
		log.Error("Error refreshing session", "error", err)
		if strings.Contains(err.Error(), "ExpiredToken") {
			job.RefreshToken = ""
			job.JobState = "errored: ExpiredToken"
			job.UpdatedAt = time.Now().UTC()
			return &job, nil
		}
		if strings.Contains(err.Error(), "Could not find user info for account") {
			job.RefreshToken = ""
			job.JobState = "errored: Could not find user info for account (account may have been deleted)"
			job.UpdatedAt = time.Now().UTC()
			return &job, nil
		}
		return nil, fmt.Errorf("error refreshing session: %w", err)
	}

	client.Auth = &xrpc.AuthInfo{
		Did:        job.Repo,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	job.RefreshToken = out.RefreshJwt

	client.Host = ident.PDSEndpoint()
	log = log.With("pds", client.Host)

	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, job.Repo, "")
	if err != nil {
		log.Error("Error getting repo from PDS", "error", err)
		return nil, fmt.Errorf("error getting repo from PDS: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		log.Error("Error reading repo CAR", "error", err)
		return nil, fmt.Errorf("error reading repo CAR: %w", err)
	}

	recordsToDelete := []string{}
	lk := sync.Mutex{}

	err = rr.ForEach(ctx, "app.bsky.feed.", func(path string, nodeCid cid.Cid) error {
		if !strings.HasPrefix(path, "app.bsky.feed.") {
			return nil
		}
		log := log.With("path", path)
		if strings.Contains(path, "threadgate") {
			return nil
		}
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Error("Error getting record", "error", err)
			return nil
		}

		switch rec := rec.(type) {
		case *bsky.FeedPost:
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}
			if createdAt.After(job.DeleteOlderThan) {
				return nil
			}

			hasMedia := rec.Embed != nil && ((rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0) ||
				(rec.Embed.EmbedRecordWithMedia != nil && rec.Embed.EmbedRecordWithMedia.Media != nil))

			if hasMedia {
				if slices.Contains(job.CleanupTypes, "post_with_media") {
					lk.Lock()
					recordsToDelete = append(recordsToDelete, path)
					lk.Unlock()
				}
				return nil
			}

			if slices.Contains(job.CleanupTypes, "post") {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedRepost:
			if !slices.Contains(job.CleanupTypes, "repost") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(job.DeleteOlderThan) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedLike:
			if !slices.Contains(job.CleanupTypes, "like") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(job.DeleteOlderThan) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		}

		return nil
	})
	if err != nil {
		log.Error("Error iterating over records", "error", err)
		return nil, fmt.Errorf("error iterating over records: %w", err)
	}

	log.Info("Found records to delete", "count", len(recordsToDelete))

	// applyWrites accepts at most 200 operations per call; we use 10 to stay well
	// within that and keep individual requests fast.
	deleteBatches := []*comatproto.RepoApplyWrites_Input{}
	batchNum := 0
	for i, path := range recordsToDelete {
		if i >= maxDeletesPerHour || job.NumDeletedToday+int64(i) > int64(maxDeletesPerDay) {
			break
		}
		if i%10 == 0 {
			if i != 0 {
				batchNum++
			}
			nextBatch := comatproto.RepoApplyWrites_Input{
				Repo:   job.Repo,
				Writes: []*comatproto.RepoApplyWrites_Input_Writes_Elem{},
			}
			deleteBatches = append(deleteBatches, &nextBatch)
		}

		collection := strings.Split(path, "/")[0]
		rkey := strings.Split(path, "/")[1]

		deleteObj := comatproto.RepoApplyWrites_Delete{
			Collection: collection,
			Rkey:       rkey,
		}

		deleteBatch := deleteBatches[batchNum]
		deleteBatch.Writes = append(deleteBatch.Writes, &comatproto.RepoApplyWrites_Input_Writes_Elem{
			RepoApplyWrites_Delete: &deleteObj,
		})
	}

	numDeleted := 0

	// 4 req/s keeps us under the PDS write rate limit with a small safety margin.
	limiter := rate.NewLimiter(rate.Limit(4), 1)
	for _, batch := range deleteBatches {
		if err := limiter.Wait(ctx); err != nil {
			log.Error("Error waiting for rate limiter", "error", err)
			return nil, fmt.Errorf("error waiting for rate limiter: %w", err)
		}

		_, err := comatproto.RepoApplyWrites(ctx, &client, batch)
		if err != nil {
			log.Error("Error applying writes", "error", err)
			return nil, fmt.Errorf("errored out after deleting (%d) records: %w", numDeleted, err)
		}
		numDeleted += len(batch.Writes)
	}

	estRemaining := len(recordsToDelete) - numDeleted

	log.Info("Deleted records",
		"count", numDeleted,
		"total", len(recordsToDelete),
		"est_remaining", estRemaining,
	)

	now := time.Now().UTC()
	job.NumDeletedToday += int64(numDeleted)
	job.NumDeleted += int64(numDeleted)
	job.EstNumRemaining = int64(estRemaining)
	job.UpdatedAt = now
	job.LastDeletedAt = &now

	if job.EstNumRemaining <= 0 {
		job.RefreshToken = ""
		job.JobState = "completed"
	}

	return &job, nil
}
