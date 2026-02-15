# Makefile for macOS
.PHONY: build install clean test help

BINARY_NAME=pluck-table
GO=go

help:
	@echo 'Ultra-Fast SQL Table Extractor - macOS'
	@echo ''
	@echo 'Targets:'
	@echo '  build      Build ultra-fast version'
	@echo '  install    Install to /usr/local/bin'
	@echo '  clean      Clean build artifacts'
	@echo '  test       Quick performance test'

build:
	@echo "Building ultra-fast extractor for macOS..."
	@$(GO) mod download
	@$(GO) build -o $(BINARY_NAME) -ldflags="-s -w" main.go
	@echo "✓ Build complete: ./$(BINARY_NAME)"
	@ls -lh $(BINARY_NAME)

build-universal: ## Build universal binary (Intel + Apple Silicon)
	@echo "Building universal binary..."
	@$(GO) mod download
	@GOOS=darwin GOARCH=arm64 $(GO) build -o $(BINARY_NAME)-arm64 -ldflags="-s -w" main.go
	@GOOS=darwin GOARCH=amd64 $(GO) build -o $(BINARY_NAME)-amd64 -ldflags="-s -w" main.go
	@lipo -create -output $(BINARY_NAME) $(BINARY_NAME)-arm64 $(BINARY_NAME)-amd64
	@rm -f $(BINARY_NAME)-arm64 $(BINARY_NAME)-amd64
	@echo "✓ Universal binary created: ./$(BINARY_NAME)"
	@file $(BINARY_NAME)
	@ls -lh $(BINARY_NAME)

install: build
	@echo "Installing to /usr/local/bin..."
	@sudo cp $(BINARY_NAME) /usr/local/bin/
	@echo "✓ Installed: /usr/local/bin/$(BINARY_NAME)"

test: build
	@if [ ! -f ../generate_test_dump.sh ]; then \
		echo "Error: generate_test_dump.sh not found"; \
		echo "Please copy it to parent directory or current directory"; \
		exit 1; \
	fi
	@echo "Creating test dump (100MB)..."
	@bash ../generate_test_dump.sh test.sql.gz 3 10000 || bash ./generate_test_dump.sh test.sql.gz 3 10000
	@echo ""
	@echo "Running extraction test..."
	@time ./$(BINARY_NAME) test.sql.gz users
	@echo ""
	@echo "✓ Test complete! Check users.sql"
	@ls -lh users.sql
	@rm -f test.sql.gz users.sql

benchmark: build
	@echo "Creating benchmark dump (500MB)..."
	@bash ../generate_test_dump.sh bench.sql.gz 5 50000 || bash ./generate_test_dump.sh bench.sql.gz 5 50000
	@echo ""
	@echo "Benchmarking..."
	@/usr/bin/time ./$(BINARY_NAME) bench.sql.gz users
	@rm -f bench.sql.gz users.sql
	@echo "✓ Benchmark complete!"

clean:
	@echo "Cleaning..."
	@rm -f $(BINARY_NAME) *.sql *.sql.gz
	@echo "✓ Clean complete"

# Quick start guide
quickstart:
	@echo "=== Quick Start ==="
	@echo ""
	@echo "1. Build:"
	@echo "   make build"
	@echo ""
	@echo "2. Test:"
	@echo "   make test"
	@echo ""
	@echo "3. Use with real dump:"
	@echo "   ./pluck-table your_dump.sql.gz table_name"
	@echo ""
	@echo "4. Install system-wide:"
	@echo "   make install"
	@echo ""

# macOS-specific info
sysinfo:
	@echo "=== System Information ==="
	@echo "CPU: $$(sysctl -n machdep.cpu.brand_string)"
	@echo "Cores: $$(sysctl -n hw.ncpu)"
	@echo "RAM: $$(echo $$(sysctl -n hw.memsize) / 1024 / 1024 / 1024 | bc)GB"
	@echo "OS: $$(sw_vers -productName) $$(sw_vers -productVersion)"
	@echo ""
	@echo "Recommended settings for your system:"
	@echo "  NumWorkers: $$(sysctl -n hw.ncpu)"
	@echo "  Expected throughput: 200-300 MB/s"