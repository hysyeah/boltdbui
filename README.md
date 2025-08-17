# BoltDB UI 

A web-based user interface for viewing and exploring BoltDB databases（eg. containerd meta.db), specifically designed for containerd metadata databases.

## Screenshot

![BoltDB UI Interface](https://github.com/hysyeah/boltdbui/screenshot.png)

*The web interface showing the containerd metadata viewer with bucket hierarchy, statistics, and key-value exploration capabilities.*

## Features

- **Web Interface**: Clean and intuitive web UI for browsing BoltDB databases
- **Hierarchical Navigation**: Browse buckets and sub-buckets in a tree structure
- **Key-Value Exploration**: View and search key-value pairs within buckets
- **Data Type Support**: 
  - JSON data with syntax highlighting and formatting
  - Binary data with hexadecimal preview
  - UTF-8 text data
- **Advanced Features**:
  - Timestamp decoding for time-based values
  - Protobuf decoding support
  - Full-text search across keys
  - Database statistics
  - Real-time updates via WebSocket
- **Containerd Integration**: Optimized for containerd metadata database structure

## Installation

### Prerequisites

- Go 1.23 or later
- Access to a BoltDB database file

### Build from Source

```bash
# Clone the repository
git clone https://github.com/hysyeah/boltdbui.git
cd boltdbui

# Install dependencies
go mod download

# Build the application
go build -o boltdbui main.go
```

## Usage

### Basic Usage

```bash
# Use default containerd metadata database path
./boltdbui

# Specify custom database path
./boltdbui /path/to/your/database.db

# Set custom port via environment variable
PORT=8080 ./boltdbui
```

### Default Configuration

- **Default Database Path**: `/var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db`
- **Default Port**: `8081`
- **Web Interface**: `http://localhost:8081`


### Environment Variables

- `PORT`: Set the web server port (default: 8081)

## API Endpoints

The application provides a RESTful API for programmatic access:

- `GET /api/buckets` - List all buckets
- `GET /api/bucket/{path}` - Get bucket details and contents
- `GET /api/key/{bucketPath}/{key}` - Get specific key details
- `GET /api/key/{bucketPath}/{key}?full=1` - Get full key data (no truncation)
- `GET /api/search?q={query}` - Search keys by name
- `GET /api/decode/time/{bucketPath}/{key}` - Decode timestamp values
- `GET /api/decode/protobuf/{bucketPath}/{key}` - Decode protobuf values
- `GET /api/stats` - Get database statistics
- `GET /api/ws` - WebSocket endpoint for real-time updates

## Web Interface Features

### Navigation
- **Sidebar**: Hierarchical bucket tree with expand/collapse functionality
- **Main Panel**: Key-value pairs display with pagination
- **Search**: Global search across all keys

### Data Visualization
- **JSON Data**: Formatted with syntax highlighting
- **Binary Data**: Hexadecimal dump with ASCII representation
- **Large Data**: Automatic truncation with "View Full" option
- **Statistics**: Bucket-level statistics (key count, page info, depth)

### Special Features
- **Timestamp Decoding**: Convert binary timestamps to human-readable format
- **Protobuf Decoding**: Decode protobuf messages with type information
- **Real-time Updates**: Live connection status and heartbeat

## Use Cases

### Containerd Debugging
- Inspect containerd metadata database
- Debug container and image metadata
- Analyze storage and runtime information

### General BoltDB Exploration
- Browse any BoltDB database structure
- Analyze data organization and content
- Debug application data storage


