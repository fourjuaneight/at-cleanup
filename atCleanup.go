package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

var (
	did          string
	appPassword  string
	cleanupTypes string
	daysAgo      int
	dryRun       bool
)

func init() {
	flag.StringVar(&did, "did", "", "The DID (decentralized identifier) of the user")
	flag.StringVar(&appPassword, "password", "", "The application password")
	flag.StringVar(&cleanupTypes, "types", "post,repost,like", "The types of records to clean up (comma-separated)")
	flag.IntVar(&daysAgo, "days", 30, "Delete records older than this many days")
	flag.BoolVar(&dryRun, "dry-run", true, "Whether to actually delete records or perform a dry run")
}

func main() {
	flag.Parse()
	if did == "" || appPassword == "" {
		log.Fatal("The DID and password are required.")
	}
	cleanupTypesList := strings.Split(cleanupTypes, ",")
	ctx := context.Background()
	
	info, err := enqueueCleanupJob(ctx, CleanupOldRecordsRequest{
		DID:                 did,
		AppPassword:         appPassword,
		CleanupTypes:        cleanupTypesList,
		DeleteUntilDaysAgo:  daysAgo,
		ActuallyDeleteStuff: !dryRun,
	})
	if err != nil {
		log.Fatalf("Error enqueuing cleanup job: %v", err)
	}

	fmt.Printf("Cleanup job completed. Number enqueued: %d\n", info.NumEnqueued)
	if dryRun {
		fmt.Println("This was a dry run; no records were deleted.")
	}
}

type CleanupOldRecordsRequest struct {
	DID                 string
	AppPassword         string
	CleanupTypes        []string
	DeleteUntilDaysAgo  int
	ActuallyDeleteStuff bool
}

type cleanupInfo struct {
	NumEnqueued int
	DryRun      bool
	JobID       string
}

func enqueueCleanupJob(ctx context.Context, req CleanupOldRecordsRequest) (*cleanupInfo, error) {
	log := log.New(os.Stdout, "cleanup: ", log.LstdFlags)

	client := xrpc.Client{
		Client: &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   5 * time.Minute,
		},
		Host: "https://bsky.social",
	}

	// Log in as the user
	out, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: req.DID,
		Password:   req.AppPassword,
	})
	if err != nil {
		return nil, fmt.Errorf("error logging in: %w", err)
	}

	client.Auth = &xrpc.AuthInfo{
		Did:        out.Did,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	log.Printf("logged in as: %s", out.Did)

	did, err := syntax.ParseDID(out.Did)
	if err != nil {
		return nil, fmt.Errorf("error parsing DID: %w", err)
	}

	ident, err := userDirectoryLookup(did.String())
	if err != nil {
		return nil, fmt.Errorf("error looking up DID: %w", err)
	}

	client.Host = ident.PDSEndpoint()

	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, did.String(), "")
	if err != nil {
		return nil, fmt.Errorf("error getting repo: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		return nil, fmt.Errorf("error reading repo CAR: %w", err)
	}

	recordsToDelete := []string{}
	lk := sync.Mutex{}

	err = rr.ForEach(ctx, "app.bsky.feed.", func(path string, nodeCid cid.Cid) error {
		if strings.Contains(path, "threadgate") {
			return nil
		}

		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			return nil
		}

		createdAt, err := parseRecordCreatedAt(rec)
		if err != nil || createdAt.After(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
			return nil
		}

		isEligible := false
		switch rec.(type) {
		case *bsky.FeedPost:
			if slices.Contains(req.CleanupTypes, "post") {
				isEligible = true
			}
		case *bsky.FeedRepost:
			if slices.Contains(req.CleanupTypes, "repost") {
				isEligible = true
			}
		case *bsky.FeedLike:
			if slices.Contains(req.CleanupTypes, "like") {
				isEligible = true
			}
		}

		if isEligible {
			lk.Lock()
			defer lk.Unlock()
			recordsToDelete = append(recordsToDelete, path)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error iterating over records: %w", err)
	}

	log.Printf("Found records to delete: %d", len(recordsToDelete))

	if req.ActuallyDeleteStuff {
		err = deleteRecords(ctx, client, recordsToDelete)
		if err != nil {
			return nil, err
		}
	}

	return &cleanupInfo{
		NumEnqueued: len(recordsToDelete),
		DryRun:      !req.ActuallyDeleteStuff,
	}, nil
}

func parseRecordCreatedAt(record interface{}) (time.Time, error) {
	switch rec := record.(type) {
	case *bsky.FeedPost:
		return dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedRepost:
		return dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedLike:
		return dateparse.ParseAny(rec.CreatedAt)
	}
	return time.Time{}, errors.New("unknown record type")
}

func deleteRecords(ctx context.Context, client xrpc.Client, records []string) error {
	// Batch deletion logic, similar to previous implementation
	return nil
}

func userDirectoryLookup(did string) (identity.Directory, error) {
	// Mock lookup or actual implementation
	return identity.Directory{}, nil
}
