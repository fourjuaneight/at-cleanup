package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/time/rate"
)

const (
	directoryServiceURL = "https://plc.directory/"
	defaultBatchSize    = 10
	clientTimeout       = 5 * time.Minute
	defaultAPIHost      = "https://bsky.social"
	lookupTimeout       = 10 * time.Second
)

// IdentityDirectory holds the PDS endpoint URL
type IdentityDirectory struct {
	PDSEndpointURL string `json:"pds_url"`
}

// PDSEndpoint returns the PDS endpoint URL
func (i IdentityDirectory) PDSEndpoint() string {
	return i.PDSEndpointURL
}

// RecordType represents supported record types for cleanup
type RecordType string

const (
	Post   RecordType = "post"
	Repost RecordType = "repost"
	Like   RecordType = "like"
	Block  RecordType = "block"
)

// CleanupConfig holds configuration for the cleanup operation
type CleanupConfig struct {
	DID         string
	AppPassword string
	Types       []string
	DaysAgo     int
	RateLimit   int
	BurstLimit  int
}

// userDirectoryLookup queries user directory for a given DID
func userDirectoryLookup(did string) (IdentityDirectory, error) {
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()

	// Construct URL for DID lookup
	url := directoryServiceURL + did

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return IdentityDirectory{}, fmt.Errorf("error creating HTTP request for DID lookup: %w", err)
	}

	req.Header.Set("Accept", "application/did+ld+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return IdentityDirectory{}, fmt.Errorf("error performing HTTP request for DID lookup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return IdentityDirectory{}, fmt.Errorf("received non-OK HTTP status from DID lookup: %s", resp.Status)
	}

	var plcResponse struct {
		Service []map[string]any `json:"service"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&plcResponse); err != nil {
		return IdentityDirectory{}, fmt.Errorf("error decoding JSON response from DID lookup: %w", err)
	}

	for _, service := range plcResponse.Service {
		if typeVal, ok := service["type"].(string); ok && typeVal == "AtprotoPersonalDataServer" {
			if endpoint, ok := service["serviceEndpoint"].(string); ok {
				return IdentityDirectory{PDSEndpointURL: endpoint}, nil
			}
		}
	}

	return IdentityDirectory{}, fmt.Errorf("could not find PDS endpoint in PLC directory response")
}

// parseRecordCreatedAt parses the creation date of a given record
func parseRecordCreatedAt(record interface{}) (time.Time, error) {
	var createdAt string

	switch rec := record.(type) {
	case *bsky.FeedPost:
		createdAt = rec.CreatedAt
	case *bsky.FeedRepost:
		createdAt = rec.CreatedAt
	case *bsky.FeedLike:
		createdAt = rec.CreatedAt
	case *bsky.GraphBlock:
		createdAt = rec.CreatedAt
	default:
		return time.Time{}, fmt.Errorf("unknown record type for createdAt parsing: %T", rec)
	}

	return dateparse.ParseAny(createdAt)
}

// deleteRecords deletes batches of records based on rate limitations
func deleteRecords(ctx context.Context, client xrpc.Client, repo string, records []string, rateLimit, burstLimit int) error {
	if len(records) == 0 {
		return nil
	}

	limiter := rate.NewLimiter(rate.Limit(rateLimit), burstLimit)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalDeleted int
	errChan := make(chan error, 1)
	
	batchSize := defaultBatchSize
	recordChan := make(chan []string)

	// Producer: Send record batches to channel
	go func() {
		defer close(recordChan)
		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}
			recordChan <- records[i:end]
		}
	}()

	// Consumer: Process batches from channel
	for i := 0; i < burstLimit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			for batch := range recordChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if err := limiter.Wait(ctx); err != nil {
					errChan <- fmt.Errorf("rate limit error: %w", err)
					return
				}

				deleteBatch := &comatproto.RepoApplyWrites_Input{
					Repo:   repo,
					Writes: make([]*comatproto.RepoApplyWrites_Input_Writes_Elem, 0, len(batch)),
				}

				for _, path := range batch {
					components := strings.Split(path, "/")
					if len(components) < 2 {
						log.Printf("Invalid path format: %s", path)
						continue
					}
					
					deleteObj := comatproto.RepoApplyWrites_Delete{
						Collection: components[0],
						Rkey:       components[1],
					}

					deleteBatch.Writes = append(deleteBatch.Writes, &comatproto.RepoApplyWrites_Input_Writes_Elem{
						RepoApplyWrites_Delete: &deleteObj,
					})
				}

				if len(deleteBatch.Writes) == 0 {
					continue
				}

				_, err := comatproto.RepoApplyWrites(ctx, &client, deleteBatch)
				if err != nil {
					log.Printf("Error applying writes for batch: %v", err)
					continue
				}

				mu.Lock()
				totalDeleted += len(deleteBatch.Writes)
				mu.Unlock()
				
				log.Printf("Deleted %d records", len(deleteBatch.Writes))
			}
		}()
	}

	// Wait for all workers to finish
	wg.Wait()
	log.Printf("Total deleted records: %d", totalDeleted)
	
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// createClient initializes and authenticates an XRPC client
func createClient(ctx context.Context, config CleanupConfig) (*xrpc.Client, error) {
	client := &xrpc.Client{
		Client: &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   clientTimeout,
		},
		Host: defaultAPIHost,
	}

	out, err := comatproto.ServerCreateSession(ctx, client, &comatproto.ServerCreateSession_Input{
		Identifier: config.DID,
		Password:   config.AppPassword,
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

	didObj, err := syntax.ParseDID(out.Did)
	if err != nil {
		return nil, fmt.Errorf("error parsing DID: %w", err)
	}

	ident, err := userDirectoryLookup(didObj.String())
	if err != nil {
		return nil, fmt.Errorf("error looking up DID: %w", err)
	}

	client.Host = ident.PDSEndpoint()
	return client, nil
}

// getRecordsToDelete identifies records that need to be deleted
func getRecordsToDelete(ctx context.Context, client *xrpc.Client, userDID string, config CleanupConfig) ([]string, error) {
	repoBytes, err := comatproto.SyncGetRepo(ctx, client, userDID, "")
	if err != nil {
		return nil, fmt.Errorf("error getting repo: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		return nil, fmt.Errorf("error reading repo CAR: %w", err)
	}

	var recordsToDelete []string
	var mu sync.Mutex
	cutoffTime := time.Now().AddDate(0, 0, -config.DaysAgo)

	err = rr.ForEach(ctx, "", func(path string, nodeCid cid.Cid) error {
		// Skip if not target record type or contains threadgate
		if strings.Contains(path, "threadgate") {
			return nil
		}

		if !strings.HasPrefix(path, "app.bsky.feed.") && !strings.HasPrefix(path, "app.bsky.graph.block") {
			return nil
		}

		// Get record content
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Printf("Error getting record for path %s: %v", path, err)
			return nil // Skip this record but continue processing
		}

		// Parse creation date
		createdAt, err := parseRecordCreatedAt(rec)
		if err != nil {
			log.Printf("Error parsing record createdAt for path %s: %v", path, err)
			return nil // Skip this record but continue processing
		}

		// Skip if newer than cutoff
		if createdAt.After(cutoffTime) {
			return nil
		}

		// Check if record type matches cleanup types
		var isEligible bool
		switch rec.(type) {
		case *bsky.FeedPost:
			isEligible = contains(config.Types, string(Post))
		case *bsky.FeedRepost:
			isEligible = contains(config.Types, string(Repost))
		case *bsky.FeedLike:
			isEligible = contains(config.Types, string(Like))
		case *bsky.GraphBlock:
			isEligible = contains(config.Types, string(Block))
		}

		if isEligible {
			mu.Lock()
			recordsToDelete = append(recordsToDelete, path)
			mu.Unlock()
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error iterating over records: %w", err)
	}

	return recordsToDelete, nil
}

// contains checks if a slice contains a specific string
func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

// runCleanup orchestrates the overall cleanup process
func runCleanup(ctx context.Context, config CleanupConfig) error {
	log.Println("Starting cleanup...")

	// Create and authenticate client
	client, err := createClient(ctx, config)
	if err != nil {
		return err
	}

	// Find records to delete
	recordsToDelete, err := getRecordsToDelete(ctx, client, client.Auth.Did, config)
	if err != nil {
		return err
	}

	log.Printf("Found %d records to delete", len(recordsToDelete))

	// Delete identified records
	err = deleteRecords(ctx, *client, client.Auth.Did, recordsToDelete, config.RateLimit, config.BurstLimit)
	if err != nil {
		return fmt.Errorf("error during record deletion: %w", err)
	}

	log.Println("Deletion process completed successfully")
	return nil
}

func main() {
	var config CleanupConfig
	var cleanupTypes string

	app := &cli.App{
		Name:    "at-cleanup",
		Usage:   "A tool for cleaning up old records from a Bluesky repository",
		Version: "1.0.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "did",
				Usage:       "The DID (decentralized identifier) of the user",
				Required:    true,
				Destination: &config.DID,
			},
			&cli.StringFlag{
				Name:        "password",
				Usage:       "The application password (create at bsky.app/settings/app-passwords)",
				Required:    true,
				Destination: &config.AppPassword,
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
				Destination: &config.DaysAgo,
			},
			&cli.IntFlag{
				Name:        "rate-limit",
				Value:       4,
				Usage:       "Rate limiter speed (requests per second)",
				Destination: &config.RateLimit,
			},
			&cli.IntFlag{
				Name:        "burst-limit",
				Value:       1,
				Usage:       "Burst limit for rate limiter (concurrent workers)",
				Destination: &config.BurstLimit,
			},
		},
		Action: func(c *cli.Context) error {
			config.Types = strings.Split(cleanupTypes, ",")
			return runCleanup(c.Context, config)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Application error: %v", err)
	}
}