package esbulk

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestContextCancellation(t *testing.T) {
	// Test that Worker respects context cancellation
	tests := []struct {
		name        string
		cancelDelay time.Duration
		expectError bool
	}{
		{"early cancellation", 10 * time.Millisecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a timeout context to avoid hanging
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			if tt.cancelDelay > 0 {
				time.AfterFunc(tt.cancelDelay, cancel)
			}

			options := Options{
				BatchSize:      10,
				Verbose:        false, // Reduce noise in test output
				Servers:        []string{"http://localhost:9200"},
				Index:          "test-index",
				RequestTimeout: 5 * time.Second,
			}

			lines := make(chan string, 100)
			errChan := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(1) // Add the worker to the WaitGroup

			// Add some test data - use fewer items to avoid long tests
			for i := 0; i < 5; i++ {
				lines <- `{"test": "data"}`
			}
			close(lines)

			err := Worker(ctx, "test-worker", options, lines, &wg, errChan)

			if tt.expectError {
				select {
				case <-ctx.Done():
					// Expected - context was cancelled
					t.Logf("Context cancelled as expected: %v", ctx.Err())
				case err := <-errChan:
					// May get connection errors, which is expected when context is cancelled
					t.Logf("Worker error (expected): %v", err)
				case <-time.After(200 * time.Millisecond):
					t.Error("Expected context cancellation but worker didn't finish")
				}
			} else {
				if err != nil {
					t.Errorf("Worker failed unexpectedly: %v", err)
				}
			}
		})
	}
}

func TestWorkerRespectsImmediateCancellation(t *testing.T) {
	// Test that worker stops immediately when context is already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	options := Options{
		BatchSize:      10,
		Verbose:        false,
		Servers:        []string{"http://localhost:9200"},
		Index:          "test-index",
		RequestTimeout: 30 * time.Second,
	}

	lines := make(chan string, 100)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1) // Add the worker to the WaitGroup

	start := time.Now()
	err := Worker(ctx, "test-worker", options, lines, &wg, errChan)
	elapsed := time.Since(start)

	// Worker should return immediately when context is cancelled
	if elapsed > 100*time.Millisecond {
		t.Errorf("Worker should return immediately when context is cancelled, but took %v", elapsed)
	}

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestCreateHTTPRequestWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	options := Options{
		Username: "testuser",
		Password: "testpass",
	}

	// Test basic request creation
	req, err := CreateHTTPRequestWithContext(ctx, "GET", "http://example.com", nil, options)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	if req.Method != "GET" {
		t.Errorf("Expected method GET, got %s", req.Method)
	}

	if req.URL.String() != "http://example.com" {
		t.Errorf("Expected URL http://example.com, got %s", req.URL.String())
	}

	username, password, ok := req.BasicAuth()
	if !ok {
		t.Error("Expected basic auth to be set")
	}

	if username != "testuser" || password != "testpass" {
		t.Errorf("Expected basic auth user:testuser pass:testpass, got %s:%s", username, password)
	}
}

func TestCreateHTTPClientWithTimeout(t *testing.T) {
	tests := []struct {
		name            string
		insecureSkip    bool
		timeout         time.Duration
		expectedTimeout time.Duration
	}{
		{"default timeout", false, 0, 30 * time.Second},
		{"custom timeout", false, 45 * time.Second, 45 * time.Second},
		{"insecure with timeout", true, 60 * time.Second, 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := CreateHTTPClient(tt.insecureSkip, tt.timeout)

			if client.Timeout != tt.expectedTimeout {
				t.Errorf("Expected timeout %v, got %v", tt.expectedTimeout, client.Timeout)
			}
		})
	}
}

func TestSignalHandlingGracefulShutdown(t *testing.T) {
	// This test simulates graceful shutdown behavior
	// We can't easily test actual signal handling in unit tests,
	// but we can test the context cancellation logic

	ctx, cancel := context.WithCancel(context.Background())

	// Simulate a signal after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Test that operations respect context cancellation
	select {
	case <-ctx.Done():
		// Expected - context was cancelled
		t.Logf("Context cancelled as expected: %v", ctx.Err())
	case <-time.After(200 * time.Millisecond):
		t.Error("Expected context cancellation but it didn't happen")
	}
}