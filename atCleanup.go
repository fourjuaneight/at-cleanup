package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/urfave/cli/v2"
	"github.com/ipfs/go-cid"
	"golang.org/x/time/rate"
)

type IdentityDirectory struct {
	PDSEndpointURL string `json:"pds_url"`
}

func (i IdentityDirectory) PDSEndpoint() string {
	return i.PDSEndpointURL
}

func userDirectoryLookup(did string) (IdentityDirectory, error) {
	const directoryServiceURL = "https://pds-directory.example.com/query?"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%sdid=%s", directoryServiceURL, did)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return IdentityDirectory{}, fmt.Errorf("error creating HTTP request for DID lookup: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return IdentityDirectory{}, fmt.Errorf("error performing HTTP request for DID lookup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return IdentityDirectory{}, fmt.Errorf("received non-OK HTTP status from DID lookup: %s", resp.Status)
	}

	var identDir IdentityDirectory
	if err := json.NewDecoder(resp.Body).Decode(&identDir); err != nil {
		return IdentityDirectory{}, fmt.Errorf("error decoding JSON response from DID lookup: %w", err)
	}

	return identDir, nil
}

func parseRecordCreatedAt(record interface{}) (time.Time, error) {
	switch rec := record.(type) {
	case *bsky.FeedPost:
		return dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedRepost:
		return dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedLike:
		return dateparse.ParseAny(rec.CreatedAt)
	default:
		return time.Time{}, fmt.Errorf("unknown record type for createdAt parsing: %T", rec)
	}
}

func deleteRecords(ctx context.Context, client xrpc.Client, records []string, rateLimit int, burstLimit int) error {
	const maxDeletesPerHour = 4000

	limiter := rate.NewLimiter(rate.Limit(rateLimit), burstLimit)
	var wg sync.WaitGroup
	var numDeleted int

	batchSize := func() int {
		// Adjust the batch size based on current conditions, network load, and available quota
		return 10 // Default batch size, customize as needed
	}()

	recordChan := make(chan []string)

	go func() {
		defer close(recordChan)
		for start := 0; start < len(records); start += batchSize {
			end := start + batchSize
			if end > len(records) {
				end = len(records)
			}
			recordChan <- records[start:end]
		}
	}()

	for batch := range recordChan {
		wg.Add(1)
		go func(batch []string) {
			defer wg.Done()

			if err := limiter.Wait(ctx); err != nil {
				log.Printf("Rate limit error: %v", err)
				return
			}

			deleteBatch := &comatproto.RepoApplyWrites_Input{
				Writes: []*comatproto.RepoApplyWrites_Input_Writes_Elem{},
			}
			for _, path := range batch {
				components := strings.Split(path, "/")
				if len(components) < 2 {
					log.Printf("Invalid path format: %s", path)
					continue
				}
				collection := components[0]
				rkey := components[1]

				deleteObj := comatproto.RepoApplyWrites_Delete{
					Collection: collection,
					Rkey:       rkey,
				}

				deleteBatch.Writes = append(deleteBatch.Writes, &comatproto.RepoApplyWrites_Input_Writes_Elem{
					RepoApplyWrites_Delete: &deleteObj,
				})
			}

			_, err := comatproto.RepoApplyWrites(ctx, &client, deleteBatch)
			if err != nil {
				log.Printf("Error applying writes for batch: %v", err)
				return
			}

			numDeleted += len(deleteBatch.Writes)
			log.Printf("Deleted %d records", len(deleteBatch.Writes))
		}(batch)
	}

	wg.Wait()
	log.Printf("Total deleted records: %d", numDeleted)
	return nil
}

func runCleanup(ctx context.Context, did, appPassword string, cleanupTypes []string, daysAgo int, actuallyDeleteStuff bool, rateLimit int, burstLimit int) error {
	log.Println("Starting cleanup...")

	client := xrpc.Client{
		Client: &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   5 * time.Minute,
		},
		Host: "https://bsky.social",
	}

	out, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: did,
		Password:   appPassword,
	})
	if err != nil {
		return fmt.Errorf("error logging in: %w", err)
	}

	client.Auth = &xrpc.AuthInfo{
		Did:        out.Did,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	didObj, err := syntax.ParseDID(out.Did)
	if err != nil {
		return fmt.Errorf("error parsing DID: %w", err)
	}

	ident, err := userDirectoryLookup(didObj.String())
	if err != nil {
		return fmt.Errorf("error looking up DID: %w", err)
	}

	client.Host = ident.PDSEndpoint()

	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, didObj.String(), "")
	if err != nil {
		return fmt.Errorf("error getting repo: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		return fmt.Errorf("error reading repo CAR: %w", err)
	}

	var recordsToDelete []string
	lk := sync.Mutex{}

	contains := func(slice []string, item string) bool {
		for _, a := range slice {
			if a == item {
				return true
			}
		}
		return false
	}

	err = rr.ForEach(ctx, "app.bsky.feed.", func(path string, nodeCid cid.Cid) error {
		if strings.Contains(path, "threadgate") {
			return nil
		}

		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Printf("Error getting record for path %s: %v", path, err)
			return fmt.Errorf("error getting record for path %s: %w", path, err)
		}

		createdAt, err := parseRecordCreatedAt(rec)
		if err != nil {
			log.Printf("Error parsing record createdAt for path %s: %v", path, err)
			return fmt.Errorf("error parsing createdAt for record at path %s: %w", path, err)
		}
		if createdAt.After(time.Now().AddDate(0, 0, -daysAgo)) {
			return nil
		}

		isEligible := false
		switch rec.(type) {
		case *bsky.FeedPost:
			if contains(cleanupTypes, "post") {
				isEligible = true
			}
		case *bsky.FeedRepost:
			if contains(cleanupTypes, "repost") {
				isEligible = true
			}
		case *bsky.FeedLike:
			if contains(cleanupTypes, "like") {
				isEligible = true
			}
		}

		if isEligible {
			lk.Lock()
			recordsToDelete = append(recordsToDelete, path)
			lk.Unlock()
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("error iterating over records: %w", err)
	}

	log.Printf("Found %d records to delete.", len(recordsToDelete))

	if actuallyDeleteStuff {
		err = deleteRecords(ctx, client, recordsToDelete, rateLimit, burstLimit)
		if err != nil {
			return fmt.Errorf("error during record deletion: %w", err)
		}

		log.Println("Deletion process completed.")
	} else {
		log.Println("Dry Run: No records were deleted.")
	}

	return nil
}

func main() {
	var did, appPassword, cleanupTypes string
	var daysAgo, rateLimit, burstLimit int
	var dryRun bool

	app := &cli.App{
		Name:    "at-cleanup",
		Usage:   "A tool for cleaning up old records from a Bluesky repository",
		Version: "1.0.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "did",
				Usage:       "The DID (decentralized identifier) of the user",
				Required:    true,
				Destination: &did,
			},
			&cli.StringFlag{
				Name:        "password",
				Usage:       "The application password",
				Required:    true,
				Destination: &appPassword,
			},
			&cli.StringFlag{
				Name:        "types",
				Value:       "post,repost,like",
				Usage:       "The types of records to clean up (comma-separated)",
				Destination: &cleanupTypes,
			},
			&cli.IntFlag{
				Name:        "days",
				Value:       30,
				Usage:       "Delete records older than this many days",
				Destination: &daysAgo,
			},
			&cli.BoolFlag{
				Name:        "dry-run",
				Value:       true,
				Usage:       "Whether to actually delete records or perform a dry run",
				Destination: &dryRun,
			},
			&cli.IntFlag{
				Name:        "rate-limit",
				Value:       4,
				Usage:       "Rate limiter speed",
				Destination: &rateLimit,
			},
			&cli.IntFlag{
				Name:        "burst-limit",
				Value:       1,
				Usage:       "Burst limit for rate limiter",
				Destination: &burstLimit,
			},
		},
		Action: func(c *cli.Context) error {
			return runCleanup(context.Background(), did, appPassword, strings.Split(cleanupTypes, ","), daysAgo, !dryRun, rateLimit, burstLimit)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Application error: %v", err)
	}
}
