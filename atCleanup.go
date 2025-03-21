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

// IdentityDirectory holds the PDS endpoint URL
type IdentityDirectory struct {
	PDSEndpointURL string `json:"pds_url"`
}

// PDSEndpoint returns the PDS endpoint URL
func (i IdentityDirectory) PDSEndpoint() string {
	return i.PDSEndpointURL
}

// userDirectoryLookup queries user directory for a given DID
func userDirectoryLookup(did string) (IdentityDirectory, error) {
	const directoryServiceURL = "https://pds-directory.example.com/query?"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Set up a context with a timeout
	defer cancel() // Ensure the context is canceled on function exit to free up resources

	// Construct URL for DID lookup
	url := fmt.Sprintf("%sdid=%s", directoryServiceURL, did)

	// Create an HTTP GET request with context
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return IdentityDirectory{}, fmt.Errorf("error creating HTTP request for DID lookup: %w", err)
	}

	// Perform the HTTP request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return IdentityDirectory{}, fmt.Errorf("error performing HTTP request for DID lookup: %w", err)
	}
	defer resp.Body.Close() // Ensure the response body is closed after reading

	// Check for non-OK HTTP status
	if resp.StatusCode != http.StatusOK {
		return IdentityDirectory{}, fmt.Errorf("received non-OK HTTP status from DID lookup: %s", resp.Status)
	}

	// Decode the JSON response to IdentityDirectory struct
	var identDir IdentityDirectory
	if err := json.NewDecoder(resp.Body).Decode(&identDir); err != nil {
		return IdentityDirectory{}, fmt.Errorf("error decoding JSON response from DID lookup: %w", err)
	}

	return identDir, nil
}

// parseRecordCreatedAt parses the creation date of a given record
func parseRecordCreatedAt(record interface{}) (time.Time, error) {
	// Handle different record types separately
	switch rec := record.(type) {
	case *bsky.FeedPost:
		return dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedRepost:
		return dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedLike:
		return dateparse.ParseAny(rec.CreatedAt)
	default:
		// Return an error for unknown record types
		return time.Time{}, fmt.Errorf("unknown record type for createdAt parsing: %T", rec)
	}
}

// deleteRecords deletes batches of records based on rate limitations
func deleteRecords(ctx context.Context, client xrpc.Client, records []string, rateLimit int, burstLimit int) error {
	const maxDeletesPerHour = 4000

	// Create a rate limiter with specified rate and burst limits
	limiter := rate.NewLimiter(rate.Limit(rateLimit), burstLimit)
	var wg sync.WaitGroup // WaitGroup to synchronize goroutines
	var numDeleted int

	// Function to define the batch size for deletes
	batchSize := func() int {
		// Define batch size
		return 10 // Customize as needed
	}()

	// Channel to distribute record batches
	recordChan := make(chan []string)

	go func() {
		defer close(recordChan) // Close the channel when done
		// Iterate over records and send batches to the channel
		for start := 0; start < len(records); start += batchSize {
			end := start + batchSize
			if end > len(records) {
				end = len(records)
			}
			recordChan <- records[start:end]
		}
	}()

	// Range over batches from the channel
	for batch := range recordChan {
		wg.Add(1) // Increment WaitGroup counter
		go func(batch []string) {
			defer wg.Done() // Decrement counter when done

			// Wait for the rate limiter to allow the next request
			if err := limiter.Wait(ctx); err != nil {
				log.Printf("Rate limit error: %v", err)
				return
			}

			deleteBatch := &comatproto.RepoApplyWrites_Input{
				Writes: []*comatproto.RepoApplyWrites_Input_Writes_Elem{},
			}

			// Prepare delete instructions for each path in the batch
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

			// Apply delete instructions
			_, err := comatproto.RepoApplyWrites(ctx, &client, deleteBatch)
			if err != nil {
				log.Printf("Error applying writes for batch: %v", err)
				return
			}

			numDeleted += len(deleteBatch.Writes) // Update deleted records count
			log.Printf("Deleted %d records", len(deleteBatch.Writes))
		}(batch)
	}

	wg.Wait() // Wait for all goroutines to finish
	log.Printf("Total deleted records: %d", numDeleted)
	return nil
}

// runCleanup orchestrates the overall cleanup process for the specified parameters
func runCleanup(ctx context.Context, did, appPassword string, cleanupTypes []string, daysAgo int, rateLimit int, burstLimit int) error {
	log.Println("Starting cleanup...")

	client := xrpc.Client{
		Client: &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   5 * time.Minute,
		},
		Host: "https://bsky.social",
	}

	// Create a session using DID and password
	out, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: did,
		Password:   appPassword,
	})
	if err != nil {
		return fmt.Errorf("error logging in: %w", err)
	}

	// Set authentication info on the client
	client.Auth = &xrpc.AuthInfo{
		Did:        out.Did,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	// Parse the DID and look up identity directory
	didObj, err := syntax.ParseDID(out.Did)
	if err != nil {
		return fmt.Errorf("error parsing DID: %w", err)
	}

	ident, err := userDirectoryLookup(didObj.String())
	if err != nil {
		return fmt.Errorf("error looking up DID: %w", err)
	}

	client.Host = ident.PDSEndpoint() // Update the client host to identity's PDS endpoint

	// Fetch and read the repository data
	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, didObj.String(), "")
	if err != nil {
		return fmt.Errorf("error getting repo: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		return fmt.Errorf("error reading repo CAR: %w", err)
	}

	var recordsToDelete []string // Slice to hold paths of records to delete
	lk := sync.Mutex{} // Mutex to handle concurrent access to recordsToDelete

	// Function to check if a slice contains a specific item
	contains := func(slice []string, item string) bool {
		for _, a := range slice {
			if a == item {
				return true
			}
		}
		return false
	}

	// Iterate over records in the repository
	err = rr.ForEach(ctx, "app.bsky.feed.", func(path string, nodeCid cid.Cid) error {
		// Skip paths containing "threadgate"
		if strings.Contains(path, "threadgate") {
			return nil
		}

		// Get the record for the current path
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Printf("Error getting record for path %s: %v", path, err)
			return fmt.Errorf("error getting record for path %s: %w", path, err)
		}

		// Parse the creation date of the record
		createdAt, err := parseRecordCreatedAt(rec)
		if err != nil {
			log.Printf("Error parsing record createdAt for path %s: %v", path, err)
			return fmt.Errorf("error parsing createdAt for record at path %s: %w", path, err)
		}

		// Skip records that are newer than the specified days ago
		if createdAt.After(time.Now().AddDate(0, 0, -daysAgo)) {
			return nil
		}

		// Check if record type matches the specified cleanup types
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

		// If eligible, add the path to recordsToDelete
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

	// Delete the identified records
	err = deleteRecords(ctx, client, recordsToDelete, rateLimit, burstLimit)
	if err != nil {
		return fmt.Errorf("error during record deletion: %w", err)
	}

	log.Println("Deletion process completed.")

	return nil
}

// main function sets up CLI app and starts cleanup based on user inputs
func main() {
	var did, appPassword, cleanupTypes string
	var daysAgo, rateLimit, burstLimit int

	// versioning
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"v"},
		Usage:   "print app version",
	}

	// help
	cli.AppHelpTemplate = `NAME:
	{{.Name}} - {{.Usage}}

VERSION:
	{{.Version}}

USAGE:
	{{.HelpName}} [optional options]

OPTIONS:
{{range .VisibleFlags}}	{{.}}{{ "\n" }}{{end}}	
`
	cli.HelpFlag = &cli.BoolFlag{
		Name:    "help",
		Aliases: []string{"h"},
		Usage:   "show help",
	}

	app := &cli.App{
		Name:    "at-cleanup",
		Usage:   "A tool for cleaning up old records from a Bluesky repository",
		Version: "1.0.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "did",
				Usage:       "The DID (decentralized identifier) of the user. (https://docs.bsky.app/docs/advanced-guides/resolving-identities)",
				Required:    true,
				Destination: &did,
			},
			&cli.StringFlag{
				Name:        "password",
				Usage:       "The application password. (https://bsky.app/settings/app-passwords)",
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
			// Execute cleanup with specified flags
			return runCleanup(context.Background(), did, appPassword, strings.Split(cleanupTypes, ","), daysAgo, rateLimit, burstLimit)
		},
	}

	// Run the CLI app
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Application error: %v", err)
	}
}
