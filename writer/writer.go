package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

// --- Operation Types ---

// RequestType defines the type of action for a single operation item.
type RequestType int

const (
	SetType    RequestType = iota // Set/update operation item
	DeleteType                    // Delete operation item
	GetType                       // Get operation item
	// Other item types can be added later
)

// RequestItem represents a single operation (Set, Delete, or Get).
// Used within the list sent to the writer.
type RequestItem struct {
	Type   RequestType // Type of this specific operation item
	Bucket string      // The bucket (Collection)
	Key    []byte      // The key
	Value  []byte      // The value (for SetType only)
}

// Result holds the outcome of an ExecutionRequest.
// It contains only an Error field as requested, assuming caller logic
// handles successful results (like Get values, although Get is awkward with this Result type) separately or doesn't need them returned here.
type Result struct {
	Error error // Error if the ExecutionRequest (single op or transaction) failed (nil if successful)
}

// --- Request Sent to Writer ---

// RequestForWriter is the unit of work sent to the sequential writer's channel.
// If len(Operations) == 1, it's a single operation request.
// If len(Operations) > 1, it's a transaction request.
type RequestForWriter struct {
	Operations []RequestItem // List of operations in this request
	ResultChan chan Result   // Channel to send the result back on for *this* request (Error only)
}

// --- Global Channel for Requests ---
// Used by all callers to send ExecutionRequests to the writer.
// Buffered channel allows API calls to return faster by queuing requests.
// Set a reasonable buffer size based on expected concurrency/load.
var writerInputChan chan RequestForWriter

// --- Public API Functions (Called by concurrent application code) ---
// These functions create RequestForWriter and send it to the writer, waiting for the error result.

// Set sends a Set request to the writer and waits for the error result.
func Set(bucket string, key, value []byte) error {
	// Create a result channel for this call
	resultChan := make(chan Result)
	req := RequestForWriter{
		Operations: []RequestItem{
			{Type: SetType, Bucket: bucket, Key: key, Value: value},
		},
		ResultChan: resultChan, // Include the result channel
	}

	// Send the request to the writer's input channel
	writerInputChan <- req

	// Wait for the result (only Error) on the unique result channel
	result := <-resultChan
	// The writer closes the channel after sending the result

	return result.Error // Return the error from the operation
}

// Delete sends a Delete request to the writer and waits for the error result.
func Delete(bucket string, key []byte) error {
	resultChan := make(chan Result)
	req := RequestForWriter{
		Operations: []RequestItem{
			{Type: DeleteType, Bucket: bucket, Key: key},
		},
		ResultChan: resultChan,
	}

	writerInputChan <- req
	result := <-resultChan
	return result.Error
}

// Get sends a Get request to the writer and waits for the error result.
// NOTE: Due to the Result type constraint (Error only), this function
// cannot return the retrieved value. It only signals if the Get operation
// itself succeeded or failed (e.g., DB error, but not "key not found").
// A successful Get (even if key not found) will result in a nil error in the Result.
func Get(bucket string, key []byte) error {
	resultChan := make(chan Result)
	req := RequestForWriter{
		Operations: []RequestItem{
			{Type: GetType, Bucket: bucket, Key: key},
		},
		ResultChan: resultChan, // Include the channel for the error result
	}

	writerInputChan <- req
	result := <-resultChan
	// The writer closes the channel
	return result.Error
}

// DoTransaction sends a transaction request (multiple operations) to the writer
// and waits for the error result for the entire transaction.
// items must have len > 1.
func DoTransaction(items []RequestItem) error {
	if len(items) <= 1 {
		return fmt.Errorf("transaction must contain more than one operation item")
	}
	resultChan := make(chan Result)
	req := RequestForWriter{
		Operations: items, // The list itself is the transaction
		ResultChan: resultChan,
	}
	writerInputChan <- req
	result := <-resultChan
	// The writer closes the channel
	return result.Error
}

// --- Sequential Writer Goroutine ---

// writerGoroutine reads requests from the channel, collects batches, and executes them on BoltDB.
// Uses context for graceful shutdown.
func writerGoroutine(db *bbolt.DB, reqChan <-chan RequestForWriter, ctx context.Context) {
	fmt.Println("Writer goroutine started.")

	// Batching parameters
	const maxBatchSize = 100                   // Maximum number of ExecutionRequests to collect in a batch cycle
	const batchTimeout = 10 * time.Millisecond // Maximum time to wait for a batch

	// Batch collection slice for ExecutionRequests
	batch := make([]RequestForWriter, 0, maxBatchSize)

	// Timer for time-based batching
	batchTimer := time.NewTimer(batchTimeout)
	// Stop timer if we exit early
	defer batchTimer.Stop()

	// Infinite loop for reading requests and processing batches
	for {
		// Use a select to read from requests channel, context cancellation, or batch timeout
		select {
		case <-ctx.Done():
			// Received shutdown signal via context cancellation
			fmt.Println("Writer goroutine received shutdown signal via context.")
			// Process any remaining requests in the current 'batch' before exiting
			if len(batch) > 0 {
				fmt.Printf("Writer processing remaining batch of %d requests before shutdown.\n", len(batch))
				processBatch(db, batch)
			}
			// In a real app, you might also try to drain reqChan here.
			return // Exit the goroutine

		case req, ok := <-reqChan:
			if !ok {
				// Request channel was closed and drained
				fmt.Println("Writer goroutine: Request channel closed and drained.")
				// Process any requests collected in the current batch
				if len(batch) > 0 {
					fmt.Printf("Writer processing final batch of %d requests before channel close.\n", len(batch))
					processBatch(db, batch)
				}
				return // Exit the goroutine
			}

			// Add the received request to the batch
			batch = append(batch, req)

			// If max batch size is reached or this is a special request type (like Shutdown, if added here),
			// process the batch immediately.
			// For simplicity, we'll only trigger immediate processing on max size.
			// Shutdown is handled by context in this example.
			if len(batch) >= maxBatchSize {
				fmt.Printf("Writer processing full batch of %d requests.\n", len(batch))
				batchTimer.Stop()              // Stop the timer as we are processing now
				processBatch(db, batch)        // Process the collected batch
				batch = batch[:0]              // Clear the batch slice
				batchTimer.Reset(batchTimeout) // Reset the timer for the next batch cycle
			} else {
				// Reset timer if it has already fired, otherwise it's still running.
				// This logic ensures the timer is always set relative to the batch *start*.
				// A simpler timer logic is to reset it only when the batch transitions from empty to non-empty.
				// Let's simplify: timer is always active.
			}

		case <-batchTimer.C:
			// Batch timeout reached
			if len(batch) > 0 {
				fmt.Printf("Writer processing batch of %d requests after timeout.\n", len(batch))
				processBatch(db, batch) // Process the collected batch
				batch = batch[:0]       // Clear the batch slice
			} else {
				fmt.Println("Writer timeout reached, but batch was empty.")
			}
			batchTimer.Reset(batchTimeout) // Reset the timer for the next batch cycle
		}
	}
}

// processBatch executes a collected batch of ExecutionRequests.
// It separates single Set/Delete/Get ops from Transaction requests.
// Groups single Set/Delete ops into one db.Update.
// Executes Transaction requests individually in db.Update.
// Executes Get requests individually in db.View.
// Sends results back on the original request channels.
func processBatch(db *bbolt.DB, batch []RequestForWriter) {
	// Prepare lists for different types of requests within this batch
	writeBatchCandidates := make([]struct { // For the single db.Update batch (Set/Delete items from single ops)
		Item       RequestItem
		ResultChan chan Result
	}, 0, len(batch))
	transactionRequests := make([]RequestForWriter, 0, len(batch)) // Transaction requests (len > 1)
	readRequests := make([]RequestForWriter, 0, len(batch))        // Get requests (len == 1, GetType)

	// Separate requests by type
	for _, req := range batch {
		if len(req.Operations) == 1 { // Single operation request
			op := req.Operations[0]
			switch op.Type {
			case SetType, DeleteType:
				writeBatchCandidates = append(writeBatchCandidates, struct {
					Item       RequestItem
					ResultChan chan Result
				}{Item: op, ResultChan: req.ResultChan})
			case GetType:
				readRequests = append(readRequests, req)
			// Handle ShutdownRequest here if it were sent via this channel
			default:
				// Unknown single item type, send error back immediately
				req.ResultChan <- Result{Error: fmt.Errorf("writer received unknown single op type in batch: %v", op.Type)}
				close(req.ResultChan)
			}
		} else if len(req.Operations) > 1 { // Transaction request
			transactionRequests = append(transactionRequests, req)
		} else { // Empty operations list? Invalid request format.
			req.ResultChan <- Result{Error: fmt.Errorf("writer received request with empty operations list")}
			close(req.ResultChan)
		}
	}

	// --- Execute Read Requests (Get) ---
	// Get requests are typically independent and executed individually.
	if len(readRequests) > 0 {
		fmt.Printf("Processing %d Get requests individually.\n", len(readRequests))
	}
	for _, req := range readRequests {
		op := req.Operations[0] // GetRequest has exactly one item
		// Execute Get in a bbolt View (read-only) transaction
		// Note: Value cannot be returned via Result due to its structure constraint.
		// This View primarily checks if the Get operation itself encounters a DB error.
		err := db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(op.Bucket))
			if b == nil {
				// Bucket doesn't exist, key not found. No DB error.
				return nil
			}
			// Get the value (even though we can't return it) to simulate access.
			// b.Get returns nil if key not found.
			value := b.Get(op.Key)
			if value == nil {
				// Key not found. No DB error.
			} else {
				// Key found. No DB error. Value is available within this transaction.
			}
			return nil // Return nil error from View unless there's a DB issue
		})
		// Send result back (only error)
		req.ResultChan <- Result{Error: err} // Send error from the View transaction
		close(req.ResultChan)                // Close the channel
	}

	// --- Execute Single Write Batch (Set/Delete from collected single ops) ---
	if len(writeBatchCandidates) > 0 {
		fmt.Printf("Processing write batch with %d operations.\n", len(writeBatchCandidates))
		// Execute all collected Set/Delete operations in a single bbolt Update transaction
		batchErr := db.Update(func(tx *bbolt.Tx) error {
			for _, candidate := range writeBatchCandidates {
				op := candidate.Item // Get the inner operation item
				// Create bucket if needed for Set operations
				if op.Type == SetType {
					if _, err := tx.CreateBucketIfNotExists([]byte(op.Bucket)); err != nil {
						// This error causes rollback for the entire batch transaction
						return fmt.Errorf("batch CreateBucket for Set op '%s': %w", op.Bucket, err)
					}
				}

				b := tx.Bucket([]byte(op.Bucket))
				if b == nil && op.Type == DeleteType {
					// Bucket doesn't exist for a delete op, just skip it.
					continue
				} else if b == nil {
					// Bucket not found for a Set op, this is an error for the batch.
					return fmt.Errorf("batch Bucket '%s' not found for operation %v", op.Bucket, op.Type)
				}

				switch op.Type {
				case SetType:
					if err := b.Put(op.Key, op.Value); err != nil {
						// This error causes rollback for the entire batch transaction
						return fmt.Errorf("batch Set op failed for key %s: %w", op.Key, err)
					}
				case DeleteType:
					if err := b.Delete(op.Key); err != nil {
						// This error causes rollback for the entire batch transaction
						return fmt.Errorf("batch Delete op failed for key %s: %w", op.Key, err)
					}
				default:
					// Should not happen based on how writeBatchCandidates is populated
					return fmt.Errorf("unexpected item type in write batch: %v", op.Type)
				}
			}
			return nil // Commit the batch transaction if loop completes
		})

		// Send the batch result back to each original request that contributed to the batch
		// Use the stored result channels
		for _, candidate := range writeBatchCandidates {
			candidate.ResultChan <- Result{Error: batchErr} // Send the batch error
			close(candidate.ResultChan)                     // Close the channel
		}
	}

	// --- Execute Transaction Requests (len > 1) ---
	// Each TransactionRequest needs its own atomic Update transaction.
	if len(transactionRequests) > 0 {
		fmt.Printf("Processing %d transaction requests individually.\n", len(transactionRequests))
	}
	for _, req := range transactionRequests {
		// Execute the transaction (list of operations) in a single BoltDB Update transaction
		txErr := db.Update(func(tx *bbolt.Tx) error {
			// Iterate over each operation item within the transaction request
			for _, op := range req.Operations {
				// Create bucket if needed for Set operations within a transaction
				if op.Type == SetType {
					if _, err := tx.CreateBucketIfNotExists([]byte(op.Bucket)); err != nil {
						// This error causes rollback for the entire transaction
						return fmt.Errorf("tx CreateBucket for Set item '%s': %w", op.Bucket, err)
					}
				}

				b := tx.Bucket([]byte(op.Bucket))
				if b == nil && op.Type == DeleteType {
					// Bucket doesn't exist for a delete op, just skip it in the transaction.
					continue
				} else if b == nil {
					// Bucket not found for a non-delete op (like Set), this is an error in tx.
					return fmt.Errorf("tx Bucket '%s' not found for item type %v", op.Bucket, op.Type)
				}

				switch op.Type {
				case SetType:
					if err := b.Put(op.Key, op.Value); err != nil {
						// This error causes rollback for the entire transaction
						return fmt.Errorf("tx Set item failed for key %s: %w", op.Key, err)
					}
				case DeleteType:
					if err := b.Delete(op.Key); err != nil {
						// This error causes rollback for the entire transaction
						return fmt.Errorf("tx Delete item failed for key %s: %w", op.Key, err)
					}
				case GetType: // Get items are NOT expected within a TransactionRequest according to standard DB design
					// This error indicates an invalid transaction structure sent by the caller
					return fmt.Errorf("tx received unexpected GetType for key %s", op.Key)
				default:
					// Unknown item type within transaction -> rollback
					return fmt.Errorf("unknown item type in transaction: %v", op.Type)
				}
			}
			return nil // If the loop completes without errors, the transaction is committed
		})
		// Send the transaction result back
		req.ResultChan <- Result{Error: txErr}
		close(req.ResultChan) // Close the channel
	}

	// The outer for loop continues to the next batch collection cycle
	// Reset batch slice is handled at the end of the select case or timeout case
}

// --- Main Function and Setup ---

const dbFilePath = "my_concurrent_bbolt_batched_db.bbolt"

func main() {
	fmt.Println("Starting concurrent bbolt (batched writer) example...")

	// Remove previous DB file
	os.Remove(dbFilePath)

	// Open BoltDB database
	// 0666 is file permissions, nil for default options (good durability by default)
	db, err := bbolt.Open(dbFilePath, 0666, nil)
	if err != nil {
		log.Fatalf("Failed to open BoltDB: %v", err)
	}
	// Defer closing the database when main exits
	defer func() {
		fmt.Println("\nClosing BoltDB...")
		if err := db.Close(); err != nil {
			log.Printf("Error closing BoltDB: %v", err)
		} else {
			fmt.Println("BoltDB closed.")
		}
	}()

	// Create the request channel the writer reads from.
	// Buffered channel allows API calls to return faster by queuing requests.
	// Size should be large enough to absorb bursts of requests.
	writerInputChan = make(chan RequestForWriter, 1000) // Example buffer size

	// Create Context for graceful shutdown of the writer goroutine
	ctx, cancel := context.WithCancel(context.Background())
	// Defer calling cancel to send shutdown signal when main exits
	defer cancel()

	// Start the sequential writer goroutine
	go writerGoroutine(db, writerInputChan, ctx)

	// Give the writer a moment to start
	time.Sleep(100 * time.Millisecond)

	// --- Example Usage (as if called from different Goroutines) ---

	// Example 1: Concurrent Set Examples (launching calls in different Goroutines)
	fmt.Println("\n--- Concurrent Set Examples ---")
	go func() {
		err := Set("users", []byte("user1"), []byte("value for user1"))
		if err != nil {
			fmt.Printf("Set user1 failed: %v\n", err)
		} else {
			fmt.Println("Set user1 success.")
		}
	}()
	go func() {
		err := Set("users", []byte("user2"), []byte("value for user2"))
		if err != nil {
			fmt.Printf("Set user2 failed: %v\n", err)
		} else {
			fmt.Println("Set user2 success.")
		}
	}()
	go func() {
		err := Set("products", []byte("prodA"), []byte("details for prodA"))
		if err != nil {
			fmt.Printf("Set prodA failed: %v\n", err)
		} else {
			fmt.Println("Set prodA success.")
		}
	}()
	go func() {
		err := Set("users", []byte("user4"), []byte("value4"))
		if err != nil {
			fmt.Printf("Set user4 failed: %v\n", err)
		} else {
			fmt.Println("Set user4 success.")
		}
	}()

	// Give concurrent operations time to reach the writer, be processed, and return results
	// The batching timeout/size will also play a role here.
	time.Sleep(500 * time.Millisecond)

	// Example 2: Get Example
	fmt.Println("\n--- Get Examples ---")
	// NOTE: Get only returns error due to Result struct constraint.
	// It indicates if the DB access failed, not if the key was found.
	err = Get("users", []byte("user1")) // This key should exist
	if err != nil {
		fmt.Printf("Get user1 failed: %v\n", err) // Expect nil error if DB access OK
	} else {
		fmt.Println("Get user1 success (DB access ok).")
	}

	err = Get("products", []byte("nonexistent")) // This key should NOT exist
	if err != nil {
		fmt.Printf("Get nonexistent failed: %v\n", err) // Expect nil error if DB access OK
	} else {
		fmt.Println("Get nonexistent success (DB access ok, key not found is not an error here).")
	}

	err = Get("nonexistent_bucket", []byte("some_key")) // Non-existent bucket
	if err != nil {
		fmt.Printf("Get from nonexistent_bucket failed: %v\n", err) // Expect nil error if DB access OK
	} else {
		fmt.Println("Get from nonexistent_bucket success (DB access ok, bucket/key not found is not an error here).")
	}

	// Example 3: Delete Example
	fmt.Println("\n--- Delete Example ---")
	err = Delete("users", []byte("user1"))
	if err != nil {
		fmt.Printf("Delete user1 failed: %v\n", err)
	} else {
		fmt.Println("Delete user1 success.")
	}

	// Example 4: Transaction Example
	fmt.Println("\n--- Transaction Example ---")
	transactionItems := []RequestItem{
		{Type: SetType, Bucket: "users", Key: []byte("user5"), Value: []byte("value5_from_tx")},
		{Type: SetType, Bucket: "products", Key: []byte("prodD"), Value: []byte("detailsD_from_tx")},
		{Type: DeleteType, Bucket: "users", Key: []byte("user2")},                                          // Delete user2 within the same transaction
		{Type: SetType, Bucket: "products", Key: []byte("prodE"), Value: []byte("detailsE_from_tx")},       // Add another item
		{Type: SetType, Bucket: "new_bucket_tx", Key: []byte("key_in_new_bucket"), Value: []byte("value")}, // Create new bucket in TX
	}
	err = DoTransaction(transactionItems)
	if err != nil {
		fmt.Printf("Transaction failed: %v\n", err) // Expect error if any item op failed
	} else {
		fmt.Println("Transaction successful.") // Expect success if all item ops succeeded
	}

	// Example 5: Transaction Failure Simulation (Optional)
	// To simulate failure, you would create a TransactionRequest with an item operation
	// that will cause an error inside the db.Update function (e.g., trying to Put nil key).
	// For simplicity, skipping simulation here.

	fmt.Println("\nExamples finished.")

	// --- Signal Writer Shutdown ---
	// The defer cancel() call at the top of main will handle signaling the writer
	// via the context when main is about to exit.
	// If you needed to wait for confirmation of shutdown, you would use a separate
	// Shutdown request type sent to writerInputChan and wait on its ResultChan.

	// Wait briefly for the writer to finish processing any pending requests
	// and shutdown gracefully. In a real app, you might use a sync.WaitGroup
	// to wait for the writer goroutine to truly exit after ctx.Done() is sent.
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\nMain function finishing.")
	// BoltDB will be closed by defer
}
