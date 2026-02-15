package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// Extreme tuning for maximum speed
	NumWorkers  = 24                // More workers
	ChunkSize   = 32 * 1024 * 1024  // 32MB chunks
	BufferSize  = 4 * 1024 * 1024   // 4MB I/O buffers
	MaxLineSize = 512 * 1024 * 1024 // 512MB max line
)

type Chunk struct {
	data   []byte
	offset int64
}

type Stats struct {
	bytesRead       atomic.Int64
	bytesWritten    atomic.Int64
	chunksProcessed atomic.Int64
	startTime       time.Time
	fileSize        int64
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <dump.sql.gz> <table_name[,table_name2,...]>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nUltra-fast parallel table extraction using pigz.\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s dump.sql.gz users\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s dump.sql.gz users,orders,products\n", os.Args[0])
		os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	inputFile := os.Args[1]
	tablesArg := os.Args[2]

	// Parse comma-separated table names
	tableNames := parseTableNames(tablesArg)
	if len(tableNames) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no valid table names provided\n")
		os.Exit(1)
	}

	// Check if pigz is available
	if _, err := exec.LookPath("pigz"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: pigz not found. Install with: brew install pigz\n")
		os.Exit(1)
	}

	if err := validateInputFile(inputFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Validate all table names
	for _, tableName := range tableNames {
		if err := validateTableName(tableName); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Fprintf(os.Stderr, "\n\nInterrupted, cleaning up...\n")
		cancel()
	}()

	if len(tableNames) == 1 {
		fmt.Printf("Extracting table '%s' from %s (using pigz)...\n", tableNames[0], inputFile)
	} else {
		fmt.Printf("Extracting %d tables from %s (using pigz)...\n", len(tableNames), inputFile)
		fmt.Printf("Tables: %s\n", strings.Join(tableNames, ", "))
	}

	if err := extractTables(ctx, inputFile, tableNames); err != nil {
		// Clean up any partial outputs
		for _, tableName := range tableNames {
			os.Remove(tableName + ".sql")
		}
		if errors.Is(err, context.Canceled) {
			os.Exit(130)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n✓ Successfully extracted %d table(s)\n", len(tableNames))
	for _, tableName := range tableNames {
		if info, err := os.Stat(tableName + ".sql"); err == nil {
			fmt.Printf("  - %s.sql (%s)\n", tableName, formatBytes(info.Size()))
		}
	}
}

func parseTableNames(input string) []string {
	parts := strings.Split(input, ",")
	var tables []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			tables = append(tables, trimmed)
		}
	}
	return tables
}

func validateInputFile(inputFile string) error {
	info, err := os.Stat(inputFile)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("input file does not exist: %s", inputFile)
		}
		return fmt.Errorf("cannot access input file: %w", err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("input is not a regular file: %s", inputFile)
	}

	if info.Size() == 0 {
		return fmt.Errorf("input file is empty")
	}

	return nil
}

func validateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	for _, c := range tableName {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
			c == ';' || c == '\'' || c == '"' || c == '\\' {
			return fmt.Errorf("invalid table name '%s' (contains illegal characters)", tableName)
		}
	}

	return nil
}

func extractTables(ctx context.Context, gzipPath string, tableNames []string) error {
	fileInfo, err := os.Stat(gzipPath)
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}

	stats := &Stats{
		startTime: time.Now(),
		fileSize:  fileInfo.Size(),
	}

	// Use pigz for parallel decompression
	cmd := exec.CommandContext(ctx, "pigz", "-dc", "-p", fmt.Sprintf("%d", runtime.NumCPU()), gzipPath)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting pigz: %w", err)
	}

	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	// Create output files for each table
	outputFiles := make(map[string]*os.File)
	outputWriters := make(map[string]*bufio.Writer)
	tempFiles := make([]string, 0, len(tableNames))

	for _, tableName := range tableNames {
		tempPath := tableName + ".sql.tmp"
		tempFiles = append(tempFiles, tempPath)

		out, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			// Clean up already opened files
			for _, f := range outputFiles {
				f.Close()
			}
			return fmt.Errorf("creating output file for %s: %w", tableName, err)
		}
		outputFiles[tableName] = out
		outputWriters[tableName] = bufio.NewWriterSize(out, BufferSize)
	}

	defer func() {
		// Close all files
		for _, w := range outputWriters {
			w.Flush()
		}
		for _, f := range outputFiles {
			f.Close()
		}
		// Clean up temp files
		for _, tempFile := range tempFiles {
			os.Remove(tempFile)
		}
	}()

	progressDone := make(chan struct{})
	go reportProgress(ctx, stats, progressDone)

	tablesFound, err := processMultipleTablesFast(ctx, stdout, outputWriters, tableNames, stats)
	close(progressDone)

	if waitErr := cmd.Wait(); waitErr != nil && err == nil {
		err = fmt.Errorf("pigz error: %w", waitErr)
	}

	if err != nil {
		return err
	}

	// Check which tables were found
	var notFound []string
	for _, tableName := range tableNames {
		if !tablesFound[tableName] {
			notFound = append(notFound, tableName)
		}
	}

	if len(notFound) > 0 {
		return fmt.Errorf("tables not found in dump: %s", strings.Join(notFound, ", "))
	}

	// Flush and sync all files
	for tableName, writer := range outputWriters {
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("flushing output for %s: %w", tableName, err)
		}
		if err := outputFiles[tableName].Sync(); err != nil {
			return fmt.Errorf("syncing output for %s: %w", tableName, err)
		}
		if err := outputFiles[tableName].Close(); err != nil {
			return fmt.Errorf("closing output for %s: %w", tableName, err)
		}
	}

	// Rename temp files to final names
	for _, tableName := range tableNames {
		tempPath := tableName + ".sql.tmp"
		finalPath := tableName + ".sql"
		if err := os.Rename(tempPath, finalPath); err != nil {
			return fmt.Errorf("finalizing output file for %s: %w", tableName, err)
		}
	}

	duration := time.Since(stats.startTime)
	throughput := float64(stats.bytesRead.Load()) / duration.Seconds() / (1024 * 1024)
	fmt.Printf("\nProcessed %s decompressed in %s (%.2f MB/s decompressed)\n",
		formatBytes(stats.bytesRead.Load()),
		duration.Round(time.Millisecond),
		throughput)

	return nil
}

func processMultipleTablesFast(ctx context.Context, reader io.Reader, writers map[string]*bufio.Writer, tableNames []string, stats *Stats) (map[string]bool, error) {
	chunks := make(chan Chunk, NumWorkers*4)
	results := make(chan map[string][]byte, NumWorkers*4)

	var wg sync.WaitGroup
	var processingErr error
	var processingErrOnce sync.Once
	tablesFound := make(map[string]*atomic.Bool)

	for _, tableName := range tableNames {
		tablesFound[tableName] = &atomic.Bool{}
	}

	// Start workers
	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := workerMultiTable(ctx, chunks, results, tableNames, stats, tablesFound); err != nil {
				processingErrOnce.Do(func() {
					processingErr = err
				})
			}
		}()
	}

	// Writer goroutine
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- multiTableWriter(ctx, results, writers, stats)
	}()

	// Feed chunks
	readerErr := feedChunksFast(ctx, reader, chunks, stats)
	close(chunks)

	wg.Wait()
	close(results)
	writerErr := <-writerDone

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if readerErr != nil {
		return nil, fmt.Errorf("reading input: %w", readerErr)
	}
	if processingErr != nil {
		return nil, fmt.Errorf("processing chunks: %w", processingErr)
	}
	if writerErr != nil {
		return nil, fmt.Errorf("writing output: %w", writerErr)
	}

	// Convert to regular map
	foundMap := make(map[string]bool)
	for tableName, found := range tablesFound {
		foundMap[tableName] = found.Load()
	}

	return foundMap, nil
}

func feedChunksFast(ctx context.Context, reader io.Reader, chunks chan<- Chunk, stats *Stats) error {
	buf := make([]byte, ChunkSize)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(buf, MaxLineSize)

	var buffer bytes.Buffer
	buffer.Grow(ChunkSize)
	offset := int64(0)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Bytes()
		stats.bytesRead.Add(int64(len(line) + 1))
		buffer.Write(line)
		buffer.WriteByte('\n')

		if buffer.Len() >= ChunkSize {
			data := make([]byte, buffer.Len())
			copy(data, buffer.Bytes())

			select {
			case chunks <- Chunk{data: data, offset: offset}:
				offset++
				buffer.Reset()
				buffer.Grow(ChunkSize)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if buffer.Len() > 0 {
		data := make([]byte, buffer.Len())
		copy(data, buffer.Bytes())
		select {
		case chunks <- Chunk{data: data, offset: offset}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return scanner.Err()
}

func workerMultiTable(ctx context.Context, chunks <-chan Chunk, results chan<- map[string][]byte, tableNames []string, stats *Stats, tablesFound map[string]*atomic.Bool) error {
	// Pre-compute search patterns for each table
	type tablePattern struct {
		nameBytes    []byte
		nameBacktick []byte
	}

	patterns := make(map[string]tablePattern)
	for _, tableName := range tableNames {
		patterns[tableName] = tablePattern{
			nameBytes:    []byte(tableName),
			nameBacktick: []byte("`" + tableName + "`"),
		}
	}

	dropPrefix := []byte("DROP TABLE")
	createPrefix := []byte("CREATE TABLE")
	insertPrefix := []byte("INSERT INTO")
	lockBytes := []byte("LOCK TABLES")
	unlockBytes := []byte("UNLOCK TABLES")

	outputs := make(map[string]*bytes.Buffer)
	for _, tableName := range tableNames {
		buf := &bytes.Buffer{}
		buf.Grow(ChunkSize / len(tableNames))
		outputs[tableName] = buf
	}

	for chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Reset all output buffers
		for _, buf := range outputs {
			buf.Reset()
		}

		data := chunk.data
		start := 0

		for i := 0; i < len(data); i++ {
			if data[i] == '\n' {
				line := data[start:i]
				start = i + 1

				if len(line) == 0 {
					continue
				}

				// Check each table
				for tableName, pattern := range patterns {
					if !bytes.Contains(line, pattern.nameBytes) {
						continue
					}

					if !bytes.Contains(line, pattern.nameBacktick) {
						continue
					}

					if bytes.HasPrefix(line, dropPrefix) ||
						bytes.HasPrefix(line, createPrefix) ||
						bytes.HasPrefix(line, insertPrefix) ||
						bytes.Contains(line, lockBytes) ||
						bytes.Contains(line, unlockBytes) {

						outputs[tableName].Write(line)
						outputs[tableName].WriteByte('\n')
						tablesFound[tableName].Store(true)
					}
				}
			}
		}

		stats.chunksProcessed.Add(1)

		// Prepare result
		result := make(map[string][]byte)
		for tableName, buf := range outputs {
			if buf.Len() > 0 {
				data := make([]byte, buf.Len())
				copy(data, buf.Bytes())
				result[tableName] = data
			}
		}

		if len(result) > 0 {
			select {
			case results <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

func multiTableWriter(ctx context.Context, results <-chan map[string][]byte, writers map[string]*bufio.Writer, stats *Stats) error {
	for result := range results {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for tableName, data := range result {
			if len(data) > 0 {
				n, err := writers[tableName].Write(data)
				if err != nil {
					return fmt.Errorf("writing result for %s: %w", tableName, err)
				}
				stats.bytesWritten.Add(int64(n))
			}
		}
	}

	return nil
}

func reportProgress(ctx context.Context, stats *Stats, done <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(stats.startTime)
			bytesRead := stats.bytesRead.Load()
			throughput := float64(bytesRead) / elapsed.Seconds() / (1024 * 1024)

			fmt.Printf("\r[%s] Processed: %s | %.1f MB/s | %d chunks    ",
				elapsed.Round(time.Second),
				formatBytes(bytesRead),
				throughput,
				stats.chunksProcessed.Load(),
			)

		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
