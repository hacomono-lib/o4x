package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
)

type SystemInfo struct {
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	NumCPU      int    `json:"num_cpu"`
	GoVersion   string `json:"go_version"`
}

type BenchmarkConfig struct {
	APIEndpoint string
	NumRequests int
	Concurrency int
	RequestType string // "order", "user", "notification"
	Duration    time.Duration
}

type BenchmarkResult struct {
	Timestamp       time.Time       `json:"timestamp"`
	System          SystemInfo      `json:"system"`
	Config          BenchmarkConfig `json:"config"`
	TotalRequests   int64           `json:"total_requests"`
	SuccessRequests int64           `json:"success_requests"`
	FailedRequests  int64           `json:"failed_requests"`
	Duration        time.Duration   `json:"duration"`
	DurationMs      int64           `json:"duration_ms"`
	RequestsPerSec  float64         `json:"requests_per_sec"`
	AvgLatency      time.Duration   `json:"avg_latency"`
	AvgLatencyMs    float64         `json:"avg_latency_ms"`
	MinLatency      time.Duration   `json:"min_latency"`
	MinLatencyMs    float64         `json:"min_latency_ms"`
	MaxLatency      time.Duration   `json:"max_latency"`
	MaxLatencyMs    float64         `json:"max_latency_ms"`
	P50Latency      time.Duration   `json:"p50_latency"`
	P50LatencyMs    float64         `json:"p50_latency_ms"`
	P95Latency      time.Duration   `json:"p95_latency"`
	P95LatencyMs    float64         `json:"p95_latency_ms"`
	P99Latency      time.Duration   `json:"p99_latency"`
	P99LatencyMs    float64         `json:"p99_latency_ms"`
}

func main() {
	// Command-line flags
	apiEndpoint := flag.String("endpoint", "http://localhost:8000", "API endpoint")
	numRequests := flag.Int("requests", 1000, "Total number of requests")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	requestType := flag.String("type", "order", "Request type: order, user, notification")
	duration := flag.Duration("duration", 0, "Test duration (0 = use request count instead)")
	format := flag.String("format", "text", "Output format: text, json, csv")
	output := flag.String("output", "", "Output file path (empty = stdout)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	config := BenchmarkConfig{
		APIEndpoint: *apiEndpoint,
		NumRequests: *numRequests,
		Concurrency: *concurrency,
		RequestType: *requestType,
		Duration:    *duration,
	}

	logger.Info("starting benchmark",
		"endpoint", config.APIEndpoint,
		"requests", config.NumRequests,
		"concurrency", config.Concurrency,
		"type", config.RequestType,
		"duration", config.Duration,
	)

	result := runBenchmark(config, logger)

	if err := outputResults(result, *format, *output); err != nil {
		logger.Error("failed to output results", "error", err)
		os.Exit(1)
	}
}

func runBenchmark(config BenchmarkConfig, logger *slog.Logger) BenchmarkResult {
	startTime := time.Now()

	var totalRequests int64
	var successRequests int64
	var failedRequests int64
	var latencies []time.Duration
	var latenciesMu sync.Mutex

	// Worker function
	worker := func(requests chan struct{}, wg *sync.WaitGroup) {
		defer wg.Done()
		client := &http.Client{Timeout: 30 * time.Second}

		for range requests {
			reqStartTime := time.Now()

			var err error
			switch config.RequestType {
			case "order":
				err = sendOrderRequest(client, config.APIEndpoint)
			case "user":
				err = sendUserRequest(client, config.APIEndpoint)
			case "notification":
				err = sendNotificationRequest(client, config.APIEndpoint)
			default:
				logger.Error("unknown request type", "type", config.RequestType)
				return
			}

			latency := time.Since(reqStartTime)
			latenciesMu.Lock()
			latencies = append(latencies, latency)
			latenciesMu.Unlock()

			atomic.AddInt64(&totalRequests, 1)
			if err != nil {
				atomic.AddInt64(&failedRequests, 1)
				logger.Debug("request failed", "error", err, "latency", latency)
			} else {
				atomic.AddInt64(&successRequests, 1)
			}
		}
	}

	// Start workers
	var wg sync.WaitGroup
	requests := make(chan struct{}, config.NumRequests)

	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go worker(requests, &wg)
	}

	// Send requests
	if config.Duration > 0 {
		// Duration-based benchmark
		endTime := time.Now().Add(config.Duration)
		for time.Now().Before(endTime) {
			requests <- struct{}{}
		}
	} else {
		// Request count-based benchmark
		for i := 0; i < config.NumRequests; i++ {
			requests <- struct{}{}
		}
	}

	close(requests)
	wg.Wait()

	duration := time.Since(startTime)

	// Calculate statistics
	result := BenchmarkResult{
		Timestamp: startTime,
		System: SystemInfo{
			OS:        runtime.GOOS,
			Arch:      runtime.GOARCH,
			NumCPU:    runtime.NumCPU(),
			GoVersion: runtime.Version(),
		},
		Config:          config,
		TotalRequests:   totalRequests,
		SuccessRequests: successRequests,
		FailedRequests:  failedRequests,
		Duration:        duration,
		DurationMs:      duration.Milliseconds(),
		RequestsPerSec:  float64(totalRequests) / duration.Seconds(),
	}

	if len(latencies) > 0 {
		// Sort latencies for percentile calculation
		sortLatencies(latencies)

		result.MinLatency = latencies[0]
		result.MinLatencyMs = float64(latencies[0].Microseconds()) / 1000.0
		result.MaxLatency = latencies[len(latencies)-1]
		result.MaxLatencyMs = float64(latencies[len(latencies)-1].Microseconds()) / 1000.0
		result.AvgLatency = calculateAvgLatency(latencies)
		result.AvgLatencyMs = float64(result.AvgLatency.Microseconds()) / 1000.0
		result.P50Latency = calculatePercentile(latencies, 0.50)
		result.P50LatencyMs = float64(result.P50Latency.Microseconds()) / 1000.0
		result.P95Latency = calculatePercentile(latencies, 0.95)
		result.P95LatencyMs = float64(result.P95Latency.Microseconds()) / 1000.0
		result.P99Latency = calculatePercentile(latencies, 0.99)
		result.P99LatencyMs = float64(result.P99Latency.Microseconds()) / 1000.0
	}

	return result
}

func sendOrderRequest(client *http.Client, endpoint string) error {
	req := domain.CreateOrderRequest{
		UserID:     uuid.New(),
		ProductID:  fmt.Sprintf("product-%03d", 1+(int(time.Now().UnixNano())%5)),
		Quantity:   1 + (int(time.Now().UnixNano()) % 10),
		TotalPrice: 1000 + (int(time.Now().UnixNano()) % 9000),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := client.Post(endpoint+"/api/orders", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func sendUserRequest(client *http.Client, endpoint string) error {
	req := domain.CreateUserRequest{
		Email: fmt.Sprintf("user-%s@example.com", uuid.New().String()[:8]),
		Name:  fmt.Sprintf("User %d", time.Now().UnixNano()%10000),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := client.Post(endpoint+"/api/users", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func sendNotificationRequest(client *http.Client, endpoint string) error {
	notifTypes := []domain.NotificationType{
		domain.NotificationTypeEmail,
		domain.NotificationTypeSMS,
		domain.NotificationTypePush,
	}

	req := domain.SendNotificationRequest{
		Type:      notifTypes[time.Now().UnixNano()%int64(len(notifTypes))],
		Recipient: fmt.Sprintf("user-%s@example.com", uuid.New().String()[:8]),
		Subject:   "Benchmark Test Notification",
		Body:      "This is a benchmark test notification.",
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := client.Post(endpoint+"/api/notifications", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func sortLatencies(latencies []time.Duration) {
	// Simple bubble sort for small datasets
	n := len(latencies)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if latencies[j] > latencies[j+1] {
				latencies[j], latencies[j+1] = latencies[j+1], latencies[j]
			}
		}
	}
}

func calculateAvgLatency(latencies []time.Duration) time.Duration {
	var total time.Duration
	for _, latency := range latencies {
		total += latency
	}
	return total / time.Duration(len(latencies))
}

func calculatePercentile(sortedLatencies []time.Duration, percentile float64) time.Duration {
	index := int(float64(len(sortedLatencies)) * percentile)
	if index >= len(sortedLatencies) {
		index = len(sortedLatencies) - 1
	}
	return sortedLatencies[index]
}

func outputResults(result BenchmarkResult, format, outputPath string) error {
	var output []byte
	var err error

	switch format {
	case "json":
		output, err = json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
	case "csv":
		output, err = generateCSV(result)
		if err != nil {
			return fmt.Errorf("failed to generate CSV: %w", err)
		}
	case "text":
		output = []byte(formatTextOutput(result))
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}

	if outputPath != "" {
		if err := os.WriteFile(outputPath, output, 0644); err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
		fmt.Printf("Results written to: %s\n", outputPath)
	} else {
		fmt.Print(string(output))
	}

	return nil
}

func formatTextOutput(result BenchmarkResult) string {
	return fmt.Sprintf(`
=== System Info ===
OS:                 %s
Arch:               %s
CPU:                %d cores
Go:                 %s

=== Benchmark Config ===
Timestamp:          %s
Request Type:       %s
Endpoint:           %s
Concurrency:        %d

=== Results ===
Total Requests:     %d
Success Requests:   %d
Failed Requests:    %d
Duration:           %v
Requests/sec:       %.2f

=== Latency ===
Min:                %v (%.2fms)
Avg:                %v (%.2fms)
Max:                %v (%.2fms)
P50:                %v (%.2fms)
P95:                %v (%.2fms)
P99:                %v (%.2fms)
`,
		result.System.OS,
		result.System.Arch,
		result.System.NumCPU,
		result.System.GoVersion,
		result.Timestamp.Format(time.RFC3339),
		result.Config.RequestType,
		result.Config.APIEndpoint,
		result.Config.Concurrency,
		result.TotalRequests,
		result.SuccessRequests,
		result.FailedRequests,
		result.Duration,
		result.RequestsPerSec,
		result.MinLatency, result.MinLatencyMs,
		result.AvgLatency, result.AvgLatencyMs,
		result.MaxLatency, result.MaxLatencyMs,
		result.P50Latency, result.P50LatencyMs,
		result.P95Latency, result.P95LatencyMs,
		result.P99Latency, result.P99LatencyMs,
	)
}

func generateCSV(result BenchmarkResult) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := csv.NewWriter(buf)

	// Header
	header := []string{
		"timestamp", "os", "arch", "num_cpu", "go_version",
		"request_type", "endpoint", "concurrency",
		"total_requests", "success_requests", "failed_requests",
		"duration_ms", "requests_per_sec",
		"min_latency_ms", "avg_latency_ms", "max_latency_ms",
		"p50_latency_ms", "p95_latency_ms", "p99_latency_ms",
	}
	if err := writer.Write(header); err != nil {
		return nil, err
	}

	// Data
	record := []string{
		result.Timestamp.Format(time.RFC3339),
		result.System.OS,
		result.System.Arch,
		fmt.Sprintf("%d", result.System.NumCPU),
		result.System.GoVersion,
		result.Config.RequestType,
		result.Config.APIEndpoint,
		fmt.Sprintf("%d", result.Config.Concurrency),
		fmt.Sprintf("%d", result.TotalRequests),
		fmt.Sprintf("%d", result.SuccessRequests),
		fmt.Sprintf("%d", result.FailedRequests),
		fmt.Sprintf("%d", result.DurationMs),
		fmt.Sprintf("%.2f", result.RequestsPerSec),
		fmt.Sprintf("%.2f", result.MinLatencyMs),
		fmt.Sprintf("%.2f", result.AvgLatencyMs),
		fmt.Sprintf("%.2f", result.MaxLatencyMs),
		fmt.Sprintf("%.2f", result.P50LatencyMs),
		fmt.Sprintf("%.2f", result.P95LatencyMs),
		fmt.Sprintf("%.2f", result.P99LatencyMs),
	}
	if err := writer.Write(record); err != nil {
		return nil, err
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
