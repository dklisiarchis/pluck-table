# pluck-table

Fast CLI tool to extract specific tables from large gzipped MySQL dump files. Uses [pigz](https://zlib.net/pigz/) for parallel decompression and concurrent chunk processing for high throughput.

## Features

- Extract one or multiple tables from `.sql.gz` dumps
- Parallel decompression via `pigz`
- Multi-worker chunk processing
- Real-time progress reporting

## Requirements

- Go 1.25+
- [pigz](https://zlib.net/pigz/) (`brew install pigz` on macOS)

## Build

```sh
make build
```

Or build a universal macOS binary (Intel + Apple Silicon):

```sh
make build-universal
```

## Usage

```sh
# Extract a single table
pluck-table dump.sql.gz users

# Extract multiple tables
pluck-table dump.sql.gz users,orders,products
```

Each extracted table is written to `<table_name>.sql` in the current directory.

## Install

```sh
go install github.com/dklisiarchis/pluck-table@latest
```

## License

MIT
