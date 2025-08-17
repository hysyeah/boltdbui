// main.go - containerd metadata viewer backend service
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"k8s.io/klog/v2"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ContainerdMetadataViewer containerd metadata viewer
type ContainerdMetadataViewer struct {
	dbPath   string
	upgrader websocket.Upgrader
}

// BucketInfo bucket information
type BucketInfo struct {
	Name       string         `json:"name"`
	Path       string         `json:"path"`
	Level      int            `json:"level"`
	KeyCount   int            `json:"keyCount"`
	SubBuckets []BucketInfo   `json:"subBuckets,omitempty"`
	Keys       []KeyValuePair `json:"keys,omitempty"`
	Stats      BucketStats    `json:"stats"`
	IsExpanded bool           `json:"isExpanded"`
}

// KeyValuePair key-value pair
type KeyValuePair struct {
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	ValueType string      `json:"valueType"`
	ValueSize int         `json:"valueSize"`
	IsJSON    bool        `json:"isJson"`
	IsBinary  bool        `json:"isBinary"`
	Preview   string      `json:"preview"`
}

// BucketStats bucket statistics
type BucketStats struct {
	BranchPageN     int `json:"branchPageN"`
	BranchOverflowN int `json:"branchOverflowN"`
	LeafPageN       int `json:"leafPageN"`
	LeafOverflowN   int `json:"leafOverflowN"`
	KeyN            int `json:"keyN"`
	Depth           int `json:"depth"`
	BranchInuse     int `json:"branchInuse"`
	LeafInuse       int `json:"leafInuse"`
}

// APIResponse API response
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Buckets interface{} `json:"buckets,omitempty"` // for frontend compatibility
	Bucket  interface{} `json:"bucket,omitempty"`  // for frontend compatibility
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

// NewContainerdMetadataViewer creates metadata viewer
func NewContainerdMetadataViewer(dbPath string) *ContainerdMetadataViewer {
	return &ContainerdMetadataViewer{
		dbPath: dbPath,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // allow cross-origin
			},
		},
	}
}

// StartServer starts web server
func (c *ContainerdMetadataViewer) StartServer(port int) error {
	r := mux.NewRouter()
	// ensure routes preserve encoded paths for server-side decoding
	r.UseEncodedPath()

	// static file service
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/",
		http.FileServer(http.Dir("./static/"))))

	// API routes
	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/buckets", c.handleGetBuckets).Methods("GET")
	api.HandleFunc("/bucket/{path:.*}", c.handleGetBucket).Methods("GET")
	api.HandleFunc("/key/{bucketPath:.*}/{key}", c.handleGetKey).Methods("GET")
	api.HandleFunc("/decode/time/{bucketPath:.*}/{key}", c.handleDecodeTime).Methods("GET")
	api.HandleFunc("/decode/protobuf/{bucketPath:.*}/{key}", c.handleDecodeProtobuf).Methods("GET")
	api.HandleFunc("/search", c.handleSearch).Methods("GET")
	api.HandleFunc("/stats", c.handleGetStats).Methods("GET")

	// WebSocket routes
	api.HandleFunc("/ws", c.handleWebSocket)

	// Home page
	r.HandleFunc("/", c.handleIndex).Methods("GET")

	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("containerd metadata viewer started at: http://localhost%s\n", addr)
	fmt.Printf("Database path: %s\n", c.dbPath)

	return http.ListenAndServe(addr, r)
}

// handleIndex handles home page requests
func (c *ContainerdMetadataViewer) handleIndex(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>containerd metadata viewer</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            height: 100vh;
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem 2rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            z-index: 1000;
            position: relative;
        }

        .header h1 {
            font-size: 1.5rem;
            font-weight: 600;
        }

        .container {
            display: flex;
            height: calc(100vh - 80px);
            position: relative;
        }

        .sidebar {
            background: white;
            border-right: 1px solid #e1e5e9;
            overflow-y: auto;
            overflow-x: hidden;
            min-width: 200px;
            max-width: 60%;
            width: 350px;
            position: relative;
            transition: width 0.2s ease;
        }

        .resizer {
            width: 6px;
            background: #e1e5e9;
            cursor: col-resize;
            position: relative;
            transition: background-color 0.2s ease;
            flex-shrink: 0;
        }

        .resizer:hover {
            background: #667eea;
        }

        .resizer::before {
            content: '';
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 3px;
            height: 30px;
            background: #cbd5e0;
            border-radius: 2px;
            opacity: 0;
            transition: opacity 0.2s ease;
        }

        .resizer:hover::before {
            opacity: 1;
        }

        .main-content {
            flex: 1;
            background: white;
            overflow-y: auto;
            position: relative;
        }

        .sidebar-header {
            padding: 1rem;
            border-bottom: 1px solid #e1e5e9;
            background: #f8f9fa;
            position: sticky;
            top: 0;
            z-index: 100;
        }

        .sidebar-title {
            display: flex;
            align-items: center;
            font-weight: 600;
            color: #2d3748;
            font-size: 0.95rem;
        }

        .sidebar-title::before {
            content: "📁";
            margin-right: 0.5rem;
            font-size: 1.1rem;
        }

        .search-container {
            margin-top: 1rem;
            position: relative;
        }

        .search-input {
            width: 100%;
            padding: 0.5rem 0.75rem 0.5rem 2.5rem;
            border: 1px solid #e1e5e9;
            border-radius: 6px;
            font-size: 0.875rem;
            transition: all 0.2s ease;
        }

        .search-input:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        .search-icon {
            position: absolute;
            left: 0.75rem;
            top: 50%;
            transform: translateY(-50%);
            color: #a0aec0;
            font-size: 0.875rem;
        }

        .tree-container {
            padding: 0.5rem 0;
        }

        .tree-item {
            display: block;
            padding: 0.4rem 0.5rem 0.4rem 1rem;
            color: #4a5568;
            text-decoration: none;
            border-radius: 4px;
            margin: 1px 0.5rem;
            font-size: 0.875rem;
            line-height: 1.4;
            position: relative;
            transition: all 0.15s ease;
            cursor: pointer;
            user-select: none;
        }

        .tree-item:hover {
            background: #f7fafc;
            color: #2d3748;
        }

        .tree-item.active {
            background: #667eea;
            color: white;
        }

        .tree-item.active .item-count {
            background: rgba(255, 255, 255, 0.2);
            color: white;
        }

        .tree-toggle {
            position: absolute;
            left: 0.25rem;
            top: 50%;
            transform: translateY(-50%);
            width: 16px;
            height: 16px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.75rem;
            color: #a0aec0;
        }

        .tree-toggle::before {
            content: "📄";
        }

        .sub-buckets-container {
            display: none;
        }

        .tree-item.has-children .tree-toggle::before {
            content: "▶";
            font-size: 0.65rem;
            transition: transform 0.15s ease;
        }

        .tree-item.has-children.expanded .tree-toggle::before {
            transform: rotate(90deg);
        }

        .item-count {
            background: #e2e8f0;
            color: #4a5568;
            padding: 0.125rem 0.375rem;
            border-radius: 10px;
            font-size: 0.75rem;
            font-weight: 500;
            margin-left: auto;
            min-width: 20px;
            text-align: center;
        }

        .tree-item-content {
            display: flex;
            align-items: center;
            justify-content: space-between;
            width: 100%;
        }

        .tree-item-name {
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            margin-right: 0.5rem;
        }

        .content-header {
            padding: 1.5rem 2rem 1rem;
            border-bottom: 1px solid #e1e5e9;
            background: white;
            position: sticky;
            top: 0;
            z-index: 50;
        }

        .content-title {
            font-size: 1.25rem;
            font-weight: 600;
            color: #2d3748;
            margin-bottom: 0.5rem;
        }

        .content-subtitle {
            color: #718096;
            font-size: 0.875rem;
        }

        .content-body {
            padding: 1.5rem 2rem;
        }

        .key-item {
            background: #f8f9fa;
            border: 1px solid #e1e5e9;
            border-radius: 6px;
            margin-bottom: 0.5rem;
            overflow: hidden;
        }

        .key-header {
            padding: 0.75rem 1rem;
            background: white;
            border-bottom: 1px solid #e1e5e9;
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .key-name {
            font-weight: 600;
            color: #2d3748;
            flex: 1;
        }

        .key-type {
            background: #667eea;
            color: white;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
        }

        .key-size {
            color: #718096;
            font-size: 0.875rem;
        }

        .key-preview {
            padding: 0.75rem 1rem;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 0.875rem;
            color: #4a5568;
            background: #f8f9fa;
            white-space: pre-wrap;
            word-break: break-all;
            max-height: 200px;
            overflow-y: auto;
        }

        .view-full-btn {
            background: #48bb78;
            color: white;
            border: none;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            cursor: pointer;
            margin-left: 0.5rem;
            transition: background-color 0.2s;
        }

        .view-full-btn:hover {
            background: #38a169;
        }

        .decode-btn {
            background: #3182ce;
            color: white;
            border: none;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            cursor: pointer;
            margin-left: 0.5rem;
            transition: background-color 0.2s;
        }

        .decode-btn:hover {
            background: #2c5282;
        }

        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.5);
        }

        .modal-content {
            background-color: white;
            margin: 5% auto;
            padding: 0;
            border-radius: 8px;
            width: 90%;
            max-width: 1000px;
            max-height: 80%;
            overflow: hidden;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        }

        .modal-header {
            background: #667eea;
            color: white;
            padding: 1rem 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .modal-title {
            font-size: 1.125rem;
            font-weight: 600;
        }

        .close {
            color: white;
            font-size: 1.5rem;
            font-weight: bold;
            cursor: pointer;
            border: none;
            background: none;
        }

        .close:hover {
            opacity: 0.8;
        }

        .modal-body {
            padding: 1.5rem;
            max-height: 60vh;
            overflow-y: auto;
        }

        .full-data-content {
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 0.875rem;
            color: #4a5568;
            background: #f8f9fa;
            border: 1px solid #e1e5e9;
            border-radius: 4px;
            padding: 1rem;
            white-space: pre-wrap;
            word-break: break-all;
        }

        .stats-section {
            margin-bottom: 2rem;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }

        .stat-card {
            background: white;
            border: 1px solid #e1e5e9;
            border-radius: 8px;
            padding: 1rem;
            text-align: center;
        }

        .stat-value {
            font-size: 1.5rem;
            font-weight: 700;
            color: #667eea;
            margin-bottom: 0.25rem;
        }

        .stat-label {
            color: #718096;
            font-size: 0.875rem;
        }

        .loading {
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
            color: #718096;
        }

        .loading::before {
            content: "";
            width: 20px;
            height: 20px;
            border: 2px solid #e1e5e9;
            border-top: 2px solid #667eea;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-right: 0.5rem;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .empty-state {
            text-align: center;
            padding: 3rem 2rem;
            color: #718096;
        }

        .empty-state::before {
            content: "📭";
            font-size: 3rem;
            display: block;
            margin-bottom: 1rem;
        }

        .error-message {
            color: #e53e3e;
            padding: 1rem;
            background: #fed7d7;
            border-radius: 6px;
            border: 1px solid #feb2b2;
        }

        @media (max-width: 768px) {
            .container {
                flex-direction: column;
            }
            
            .sidebar {
                width: 100% !important;
                max-width: none;
                height: 40vh;
                min-width: auto;
            }
            
            .resizer {
                width: 100%;
                height: 6px;
                cursor: row-resize;
            }
            
            .main-content {
                height: 60vh;
            }

            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }

            .header {
                padding: 0.75rem 1rem;
            }

            .header h1 {
                font-size: 1.25rem;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>containerd metadata viewer</h1>
    </div>

    <div class="container">
        <div class="sidebar" id="sidebar">
            <div class="sidebar-header">
                <div class="sidebar-title">Bucket Hierarchy</div>
                <div class="search-container">
                    <input type="text" class="search-input" id="searchInput" placeholder="Search Bucket...">
                    <span class="search-icon">🔍</span>
                </div>
            </div>
            <div class="tree-container" id="treeContainer">
                <div class="loading">Loading...</div>
            </div>
        </div>

        <div class="resizer" id="resizer"></div>

        <div class="main-content" id="mainContent">
            <div class="content-header">
                <div class="content-title">Select a Bucket</div>
                <div class="content-subtitle">Choose a bucket from the left sidebar to view</div>
            </div>
            <div class="content-body">
                <div class="empty-state">
                    Please select a bucket from the left sidebar to view details
                </div>
            </div>
        </div>
    </div>

    <script>
        // Global variables
        var expandedBuckets = new Set();
        var allBuckets = [];
        var currentBucketPath = '';

        // Initialize draggable splitter
        function initializeResizer() {
            var resizer = document.getElementById('resizer');
            var sidebar = document.getElementById('sidebar');
            var container = document.querySelector('.container');
            
            var isResizing = false;
            var startX = 0;
            var startWidth = 0;

            resizer.addEventListener('mousedown', function(e) {
                isResizing = true;
                startX = e.clientX;
                startWidth = sidebar.offsetWidth;
                
                document.body.style.cursor = 'col-resize';
                document.body.style.userSelect = 'none';
                
                e.preventDefault();
            });

            document.addEventListener('mousemove', function(e) {
                if (!isResizing) return;
                
                var deltaX = e.clientX - startX;
                var newWidth = startWidth + deltaX;
                var containerWidth = container.offsetWidth;
                
                var minWidth = 200;
                var maxWidth = containerWidth * 0.6;
                
                if (newWidth >= minWidth && newWidth <= maxWidth) {
                    sidebar.style.width = newWidth + 'px';
                }
            });

            document.addEventListener('mouseup', function() {
                if (isResizing) {
                    isResizing = false;
                    document.body.style.cursor = '';
                    document.body.style.userSelect = '';
                }
            });

            resizer.addEventListener('dblclick', function() {
                sidebar.style.width = '350px';
            });
        }

        // Load buckets
        function loadBuckets() {
            fetch('/api/buckets')
                .then(function(response) {
                    if (!response.ok) {
                        throw new Error('HTTP ' + response.status + ': ' + response.statusText);
                    }
                    return response.json();
                })
                .then(function(data) {
                    console.log('API Response:', data);
                    if (data.success) {
                        allBuckets = data.buckets || data.data || [];
                        renderBuckets(allBuckets);
                    } else {
                        showError('Load failed: ' + (data.error || 'Unknown error'));
                    }
                })
                .catch(function(error) {
                    console.error('Fetch error:', error);
                    showError('Network error: ' + error.message);
                });
        }

        function renderBuckets(buckets, filter) {
            var container = document.getElementById('treeContainer');
            
            if (!buckets || buckets.length === 0) {
                container.innerHTML = '<div class="empty-state">No buckets found</div>';
                return;
            }

            var totalHtml = '';
            buckets.forEach(function(bucket) {
                totalHtml += renderBucketItem(bucket, filter, 0);
            });

            if (totalHtml) {
                container.innerHTML = totalHtml;
            } else {
                container.innerHTML = '<div class="empty-state">' + (filter ? 'No matching buckets found' : 'No buckets found') + '</div>';
            }
        }

        // Render single bucket item (returns DOM element or null)
        function renderBucketItem(bucket, filter, level) {
            var hasSubBuckets = bucket.subBuckets && bucket.subBuckets.length > 0;

            var subBucketsHtml = '';
            var hasVisibleChildren = false;
            if (hasSubBuckets) {
                bucket.subBuckets.forEach(function(subBucket) {
                    var childHtml = renderBucketItem(subBucket, filter, level + 1);
                    if (childHtml) {
                        subBucketsHtml += childHtml;
                        hasVisibleChildren = true;
                    }
                });
            }

            var isMatch = !filter || bucket.name.toLowerCase().indexOf(filter.toLowerCase()) > -1;
            if (!isMatch && !hasVisibleChildren) {
                return '';
            }

            var isExpanded = bucket.isExpanded || (filter && filter.length > 0);
            var expandedClass = isExpanded ? 'expanded' : '';
            var childrenDisplay = isExpanded ? 'block' : 'none';

            var itemHtml = 
                '<div class="tree-item ' + (hasSubBuckets ? 'has-children ' : '') + expandedClass + '" data-path="' + bucket.path + '" style="padding-left: ' + (level * 1.1 + 1.0) + 'rem;">' +
                    '<div class="tree-toggle"></div>' +
                    '<div class="tree-item-content">' +
                        '<div class="tree-item-name" title="' + bucket.path + '">' + bucket.name + '</div>' +
                        '<div class="item-count">' + (bucket.keyCount || 0) + '</div>' +
                    '</div>' +
                '</div>' +
                (hasVisibleChildren ? '<div class="sub-buckets-container" style="display: ' + childrenDisplay + ';">' + subBucketsHtml + '</div>' : '');

            return itemHtml;
        }

        function findBucketByPath(buckets, path) {
            for (var i = 0; i < buckets.length; i++) {
                if (buckets[i].path === path) {
                    return buckets[i];
                }
                if (buckets[i].subBuckets) {
                    var found = findBucketByPath(buckets[i].subBuckets, path);
                    if (found) {
                        return found;
                    }
                }
            }
            return null;
        }

        // Select bucket
        function selectBucket(bucket, item) {
            console.log('Selecting bucket:', bucket.path);
            currentBucketPath = bucket.path;
            var activeItems = document.querySelectorAll('.tree-item.active');
            activeItems.forEach(function(i) {
                i.classList.remove('active');
            });

            if (item) {
                item.classList.add('active');
            } else {
                var selector = '.tree-item[data-path="' + bucket.path.replace(/"/g, '\"') + '"]';
                var currentItem = document.querySelector(selector);
                if (currentItem) {
                    currentItem.classList.add('active');
                } else {
                    console.error('Could not find item to activate for path:', bucket.path);
                }
            }

            loadBucketDetails(bucket.path);
        }

        // Load bucket details
        function loadBucketDetails(bucketPath) {
            var mainContent = document.getElementById('mainContent');
            mainContent.innerHTML = 
                '<div class="content-header">' +
                    '<div class="content-title">' + bucketPath.split('/').pop() + '</div>' +
                    '<div class="content-subtitle">Loading details...</div>' +
                '</div>' +
                '<div class="content-body">' +
                    '<div class="loading">Loading...</div>' +
                '</div>';

            fetch('/api/bucket/' + encodeURIComponent(bucketPath))
                .then(function(response) {
                    if (!response.ok) {
                        throw new Error('HTTP ' + response.status + ': ' + response.statusText);
                    }
                    return response.json();
                })
                .then(function(data) {
                    console.log('Bucket details:', data);
                    if (data.success) {
                        renderBucketDetails(data.bucket || data.data);
                    } else {
                        showError('Failed to load details: ' + (data.error || 'Unknown error'));
                    }
                })
                .catch(function(error) {
                    console.error('Fetch error:', error);
                    showError('Network error: ' + error.message);
                });
        }

        // Render bucket details
        function renderBucketDetails(bucket) {
            var mainContent = document.getElementById('mainContent');
            
            var statsHtml = '';
            if (bucket.stats) {
                statsHtml = 
                    '<div class="stats-section">' +
                        '<h3>Statistics</h3>' +
                        '<div class="stats-grid">' +
                            '<div class="stat-card">' +
                                '<div class="stat-value">' + (bucket.stats.keyN || bucket.stats.KeyN || 0) + '</div>' +
                                '<div class="stat-label">Key Count</div>' +
                            '</div>' +
                            '<div class="stat-card">' +
                                '<div class="stat-value">' + (bucket.stats.leafPageN || bucket.stats.LeafPageN || 0) + '</div>' +
                                '<div class="stat-label">Leaf Pages</div>' +
                            '</div>' +
                            '<div class="stat-card">' +
                                '<div class="stat-value">' + (bucket.stats.branchPageN || bucket.stats.BranchPageN || 0) + '</div>' +
                                '<div class="stat-label">Branch Pages</div>' +
                            '</div>' +
                            '<div class="stat-card">' +
                                '<div class="stat-value">' + (bucket.stats.depth || bucket.stats.Depth || 0) + '</div>' +
                                '<div class="stat-label">Depth</div>' +
                            '</div>' +
                        '</div>' +
                    '</div>';
            }

            var keysHtml = '';
            if (bucket.keys && bucket.keys.length > 0) {
                var keyItems = '';
                for (var i = 0; i < bucket.keys.length; i++) {
                    var key = bucket.keys[i];
                    var keyName = (key.key || key.Key);
                    var bucketPathForBtn = (bucket.path || bucket.Path || '');
                    var valueSize = key.valueSize || key.ValueSize || 0;
                    var btnHtml = valueSize > 256 ? '<button class="view-full-btn" data-key-name="' + keyName + '">View Full</button>' : '';
                    var decodeBtnHtml = '';
                    // Timestamp decode button
                    if (keyName.indexOf('createdat') !== -1 || keyName.indexOf('updatedat') !== -1) {
                        decodeBtnHtml += '<button class="decode-btn" data-key-name="' + keyName + '" data-decode-type="time">Decode Time</button>';
                    }
                    // Protobuf decode button (for io.cri-containerd.container.metadata path or spec key)
                    if (keyName == 'io.cri-containerd.container.metadata' || keyName === 'spec' || keyName === 'metadata') {
                        decodeBtnHtml += '<button class="decode-btn" data-key-name="' + keyName + '" data-decode-type="protobuf">Decode Protobuf</button>';
                    }
                    keyItems += 
                        '<div class="key-item">' +
                            '<div class="key-header">' +
                                '<span class="key-name">' + keyName + '</span>' +
                                '<span class="key-type">' + (key.valueType || key.ValueType) + '</span>' +
                                '<span class="key-size">' + (key.valueSize || key.ValueSize) + ' bytes</span>' +
                                btnHtml +
                                decodeBtnHtml +
                            '</div>' +
                            '<div class="key-preview">' + (key.preview || key.Preview) + '</div>' +
                        '</div>';
                }
                keysHtml = 
                    '<div class="keys-section">' +
                        '<h3>Key-Value Pairs (' + bucket.keys.length + ')</h3>' +
                        keyItems +
                    '</div>';
            } else {
                keysHtml = '<div class="empty-state">No key-value pairs in this bucket</div>';
            }

            mainContent.innerHTML = 
                '<div class="content-header">' +
                    '<div class="content-title">' + bucket.name + '</div>' +
                    '<div class="content-subtitle">Bucket Details</div>' +
                '</div>' +
                '<div class="content-body">' +
                    statsHtml +
                    keysHtml +
                '</div>' +
                '<div id="fullDataModal" class="modal">' +
                    '<div class="modal-content">' +
                        '<div class="modal-header">' +
                            '<div class="modal-title">Full Data</div>' +
                            '<button class="close" id="closeFullDataModal">×</button>' +
                        '</div>' +
                        '<div class="modal-body">' +
                            '<pre class="full-data-content" id="fullDataContent"></pre>' +
                        '</div>' +
                    '</div>' +
                '</div>';
        }

        // Utility: escape HTML
        function escapeHTML(str) {
            if (str == null) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        // Show/hide modal
        function openFullDataModal(content, title) {
            var modal = document.getElementById('fullDataModal');
            var pre = document.getElementById('fullDataContent');
            document.querySelector('#fullDataModal .modal-title').textContent = title || 'Full Data';
            pre.textContent = content;
            modal.style.display = 'block';
        }
        function closeFullDataModal() {
            var modal = document.getElementById('fullDataModal');
            modal.style.display = 'none';
        }

        // Decode timestamp
        function fetchAndDecodeTime(bucketPath, keyName) {
            if (!bucketPath || !keyName) return;
            var url = '/api/decode/time/' + encodeURIComponent(bucketPath) + '/' + encodeURIComponent(keyName);
            fetch(url)
                .then(function(res){ if(!res.ok) throw new Error('HTTP '+res.status); return res.json(); })
                .then(function(json){
                    var data = json.data || json;
                    var decodedTime = data.decodedTime || '';
                    var timestamp = data.timestamp || '';
                    var iso = data.iso || '';
                    var title = 'Decoded Time: ' + keyName;
                    var content = 'Formatted Time: ' + decodedTime + '\n' +
                                  'Unix Timestamp: ' + timestamp + '\n' +
                                  'ISO Format: ' + iso;
                    openFullDataModal(content, title);
                })
                .catch(function(err){
                    openFullDataModal('Decode failed: ' + err.message, 'Error');
                });
        }

        function fetchAndDecodeProtobuf(bucketPath, keyName) {
            if (!bucketPath || !keyName) return;
            var url = '/api/decode/protobuf/' + encodeURIComponent(bucketPath) + '/' + encodeURIComponent(keyName);
            fetch(url)
                .then(function(res){ if(!res.ok) throw new Error('HTTP '+res.status); return res.json(); })
                .then(function(json){
                    var data = json.data || json;
                    var typeUrl = data.typeUrl || '';
                    var value = data.value || '';
                    var size = data.size || 0;
                    var title = 'Protobuf Decoded: ' + keyName;
                    var content = 'Type URL: ' + typeUrl + '\n' +
                                 'Value: ' + value + '\n' +
                                 'Size: ' + size + ' bytes';
                    openFullDataModal(content, title);
                })
                .catch(function(err){
                    openFullDataModal('Protobuf decode failed: ' + err.message, 'Error');
                });
        }

        // Request full data based on current selected bucketPath and keyName
        function fetchAndShowFullKey(bucketPath, keyName) {
            if (!bucketPath || !keyName) return;
            var url = '/api/key/' + encodeURIComponent(bucketPath) + '/' + encodeURIComponent(keyName) + '?full=1';
            fetch(url)
                .then(function(res){ if(!res.ok) throw new Error('HTTP '+res.status); return res.json(); })
                .then(function(json){
                    var data = json.data || json;
                    var value = data.value || data.Value;
                    var valueType = data.valueType || data.ValueType;
                    var isBinary = (data.isBinary || data.IsBinary) ? true : false;
                    var preview = data.preview || data.Preview;
                    var title = 'Key: ' + keyName + ' (' + (valueType || '') + ')';
                    var content;
                    if (isBinary || (valueType && String(valueType).toLowerCase() === 'binary')) {
                        content = typeof preview === 'string' ? preview : String(value || '');
                    } else if (typeof value === 'object' && value !== null) {
                        try { content = JSON.stringify(value, null, 2); }
                        catch (e) { content = String(value); }
                    } else {
                        content = String(value);
                    }
                    openFullDataModal(content, title);
                })
                .catch(function(err){
                    openFullDataModal('Load failed: ' + err.message, 'Error');
                });
        }

        // Filter buckets
        function filterBuckets(query) {
            var filteredBuckets = allBuckets.filter(function(bucket) {
                return bucket.name.toLowerCase().indexOf(query.toLowerCase()) !== -1;
            });
            renderBuckets(filteredBuckets, query);
        }

        // Show error
        function showError(message) {
            var mainContent = document.getElementById('mainContent');
            mainContent.innerHTML = 
                '<div class="content-header">' +
                    '<div class="content-title">Error</div>' +
                    '<div class="content-subtitle">An error occurred while loading</div>' +
                '</div>' +
                '<div class="content-body">' +
                    '<div class="error-message">' + message + '</div>' +
                '</div>';
        }

        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            initializeResizer();
            loadBuckets();

            var searchInput = document.getElementById('searchInput');
            searchInput.addEventListener('input', function(e) {
                renderBuckets(allBuckets, e.target.value);
            });

            document.getElementById('treeContainer').addEventListener('click', function(e) {
                console.log('Clicked on:', e.target);
                var item = e.target.closest('.tree-item');
                console.log('Found item:', item);
                if (!item) {
                    return;
                }

                var path = item.getAttribute('data-path');
                console.log('Item path:', path);
                var bucket = findBucketByPath(allBuckets, path);
                console.log('Found bucket:', bucket);

                if (!bucket) {
                    return;
                }

                if (e.target.classList.contains('tree-toggle')) {
                    console.log('Toggle clicked');
                    var subBucketsContainer = item.nextElementSibling;
                    if (subBucketsContainer && subBucketsContainer.classList.contains('sub-buckets-container')) {
                        var isExpanded = item.classList.toggle('expanded');
                        subBucketsContainer.style.display = isExpanded ? 'block' : 'none';
                        bucket.isExpanded = isExpanded;
                    }
                } else {
                    console.log('Content clicked');
                    selectBucket(bucket, item);
                }
            });

            // Global delegation: handle "View Full" buttons and modal close
            document.addEventListener('click', function(e) {
                // Close button
                if (e.target.id === 'closeFullDataModal' || e.target.closest('#closeFullDataModal')) {
                    closeFullDataModal();
                    return;
                }
                // Click overlay to close
                if (e.target.id === 'fullDataModal') {
                    closeFullDataModal();
                    return;
                }
                // View full button
                var btn = e.target.closest('.view-full-btn');
                if (btn) {
                    var keyName = btn.getAttribute('data-key-name');
                    fetchAndShowFullKey(currentBucketPath, keyName);
                }
                // Decode button
                var decodeBtn = e.target.closest('.decode-btn');
                if (decodeBtn) {
                    var keyName = decodeBtn.getAttribute('data-key-name');
                    var decodeType = decodeBtn.getAttribute('data-decode-type');
                    if (decodeType === 'time') {
                        fetchAndDecodeTime(currentBucketPath, keyName);
                    } else if (decodeType === 'protobuf') {
                        fetchAndDecodeProtobuf(currentBucketPath, keyName);
                    }
                }
            });

            document.addEventListener('keydown', function(e) {
                if (e.ctrlKey || e.metaKey) {
                    switch(e.key) {
                        case 'f':
                            e.preventDefault();
                            searchInput.focus();
                            break;
                        case 'r':
                            e.preventDefault();
                            loadBuckets();
                            break;
                    }
                }
            });
        });
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
}

// handleGetBuckets gets all buckets
func (c *ContainerdMetadataViewer) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	klog.Info("Received get buckets request")

	buckets, err := c.getAllBuckets()
	if err != nil {
		klog.Errorf("Failed to get buckets: %v", err)
		c.sendError(w, "Failed to get bucket list", err)
		return
	}

	klog.Infof("Successfully retrieved %d buckets", len(buckets))

	// Set correct response headers
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	response := APIResponse{
		Success: true,
		Buckets: buckets,
		Data:    buckets, // Also set data field for compatibility
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		klog.Errorf("Failed to encode JSON response: %v", err)
	}
}

// handleGetBucket gets detailed information for specified bucket
func (c *ContainerdMetadataViewer) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rawPath := vars["path"]

	// Decode path from frontend, handle encoded characters like %2F, %3A
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		klog.Warningf("PathUnescape failed, using original path: raw=%s, err=%v", rawPath, err)
		decodedPath = rawPath
	}
	decodedPath = strings.Trim(decodedPath, "/")

	klog.Infof("Received get bucket details request: raw=%s decoded=%s", rawPath, decodedPath)

	bucket, err := c.getBucketDetails(decodedPath)
	if err != nil {
		klog.Errorf("Failed to get bucket details: %v", err)
		c.sendError(w, "Failed to get bucket details", err)
		return
	}

	klog.Infof("Successfully retrieved bucket details: %s", decodedPath)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	response := APIResponse{
		Success: true,
		Bucket:  bucket,
		Data:    bucket, // Also set data field for compatibility
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		klog.Errorf("Failed to encode JSON response: %v", err)
	}
}

// handleGetKey gets detailed information for specified key
func (c *ContainerdMetadataViewer) handleGetKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rawBucketPath := vars["bucketPath"]
	rawKey := vars["key"]

	// Decode path and key, handle %2F and other encodings
	decodedPath, err := url.PathUnescape(rawBucketPath)
	if err != nil {
		klog.Warningf("PathUnescape failed, using original bucketPath: raw=%s, err=%v", rawBucketPath, err)
		decodedPath = rawBucketPath
	}
	decodedPath = strings.Trim(decodedPath, "/")

	decodedKey, err := url.PathUnescape(rawKey)
	if err != nil {
		klog.Warningf("PathUnescape key failed, using original key: raw=%s, err=%v", rawKey, err)
		decodedKey = rawKey
	}

	// Check if requesting full data
	fullParam := r.URL.Query().Get("full")
	if fullParam == "1" {
		keyValue, err := c.getFullKeyData(decodedPath, decodedKey)
		if err != nil {
			c.sendError(w, "Failed to get full key data", err)
			return
		}
		c.sendSuccess(w, keyValue)
		return
	}

	keyValue, err := c.getKeyDetails(decodedPath, decodedKey)
	if err != nil {
		c.sendError(w, "Failed to get key details", err)
		return
	}

	c.sendSuccess(w, keyValue)
}

// handleSearch search keys
func (c *ContainerdMetadataViewer) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		c.sendError(w, "Search query cannot be empty", nil)
		return
	}

	results, err := c.searchKeys(query)
	if err != nil {
		c.sendError(w, "Search failed", err)
		return
	}

	c.sendSuccess(w, results)
}

// handleDecodeTime decode timestamp
func (c *ContainerdMetadataViewer) handleDecodeTime(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketPath := vars["bucketPath"]
	key := vars["key"]

	// URL decode
	decodedPath, err := url.QueryUnescape(bucketPath)
	if err != nil {
		c.sendError(w, "Invalid bucket path", err)
		return
	}

	decodedKey, err := url.QueryUnescape(key)
	if err != nil {
		c.sendError(w, "Invalid key", err)
		return
	}

	// Open database
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		c.sendError(w, "Failed to open database", err)
		return
	}
	defer db.Close()

	// Get key value
	var value []byte
	err = db.View(func(tx *bolt.Tx) error {
		b := c.findBucket(tx, decodedPath)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", decodedPath)
		}
		value = b.Get([]byte(decodedKey))
		if value == nil {
			return fmt.Errorf("key not found: %s", decodedKey)
		}
		return nil
	})

	if err != nil {
		c.sendError(w, "Failed to get key", err)
		return
	}

	// Decode timestamp
	var t time.Time
	err = t.UnmarshalBinary(value)
	if err != nil {
		c.sendError(w, "Failed to decode timestamp", err)
		return
	}

	// Return formatted time
	result := map[string]interface{}{
		"decodedTime": t.Format("2006-01-02 15:04:05 MST"),
		"timestamp":   t.Unix(),
		"iso":         t.Format(time.RFC3339),
	}

	c.sendSuccess(w, result)
}

// handleDecodeProtobuf handles protobuf decode requests
func (c *ContainerdMetadataViewer) handleDecodeProtobuf(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketPath, err := url.QueryUnescape(vars["bucketPath"])
	if err != nil {
		c.sendError(w, "Invalid bucket path", err)
		return
	}

	keyName, err := url.QueryUnescape(vars["key"])
	if err != nil {
		c.sendError(w, "Invalid key name", err)
		return
	}

	// Open database
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		c.sendError(w, "Cannot open database", err)
		return
	}
	defer db.Close()

	var value []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := c.findBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist: %s", bucketPath)
		}

		value = bucket.Get([]byte(keyName))
		if value == nil {
			return fmt.Errorf("key does not exist: %s", keyName)
		}

		// Copy data as it cannot be accessed outside transaction
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		value = valueCopy

		return nil
	})

	if err != nil {
		c.sendError(w, "Failed to get key", err)
		return
	}

	// Use protobuf decoding
	var any anypb.Any
	if err := proto.Unmarshal(value, &any); err != nil {
		c.sendError(w, "Protobuf decoding failed", err)
		return
	}

	// Return decoding result
	result := map[string]interface{}{
		"typeUrl": any.GetTypeUrl(),
		"value":   string(any.GetValue()),
		"size":    len(any.GetValue()),
	}

	c.sendSuccess(w, result)
}

// handleGetStats gets database statistics
func (c *ContainerdMetadataViewer) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := c.getDatabaseStats()
	if err != nil {
		c.sendError(w, "Failed to get statistics", err)
		return
	}

	c.sendSuccess(w, stats)
}

// handleWebSocket handles WebSocket connections
func (c *ContainerdMetadataViewer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		klog.Errorf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Keep connection and send real-time updates
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Send heartbeat
			if err := conn.WriteJSON(map[string]interface{}{
				"type":      "heartbeat",
				"timestamp": time.Now().Unix(),
			}); err != nil {
				return
			}
		}
	}
}

// getAllBuckets gets hierarchical structure of all buckets
func (c *ContainerdMetadataViewer) getAllBuckets() ([]BucketInfo, error) {
	if _, err := os.Stat(c.dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", c.dbPath)
	}

	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	var buckets []BucketInfo

	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucket := c.buildBucketInfo(b, string(name), string(name), 0)
			buckets = append(buckets, bucket)
			return nil
		})
	})

	return buckets, err
}

// buildBucketInfo builds bucket information (recursive)
func (c *ContainerdMetadataViewer) buildBucketInfo(b *bolt.Bucket, name, path string, level int) BucketInfo {
	stats := b.Stats()

	bucket := BucketInfo{
		Name:     name,
		Path:     path,
		Level:    level,
		KeyCount: stats.KeyN,
		Stats: BucketStats{
			BranchPageN:     stats.BranchPageN,
			BranchOverflowN: stats.BranchOverflowN,
			LeafPageN:       stats.LeafPageN,
			LeafOverflowN:   stats.LeafOverflowN,
			KeyN:            stats.KeyN,
			Depth:           stats.Depth,
			BranchInuse:     stats.BranchInuse,
			LeafInuse:       stats.LeafInuse,
		},
		IsExpanded: level < 2, // Default expand first two levels
	}

	// Recursively get sub-buckets
	b.ForEach(func(k, v []byte) error {
		if v == nil { // This is a sub-bucket
			subBucket := b.Bucket(k)
			if subBucket != nil {
				subPath := path + "/" + string(k)
				subBucketInfo := c.buildBucketInfo(subBucket, string(k), subPath, level+1)
				bucket.SubBuckets = append(bucket.SubBuckets, subBucketInfo)
			}
		}
		return nil
	})

	return bucket
}

// getBucketDetails gets bucket detailed information including all key-value pairs
func (c *ContainerdMetadataViewer) getBucketDetails(bucketPath string) (*BucketInfo, error) {
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	var bucket *BucketInfo

	err = db.View(func(tx *bolt.Tx) error {
		b := c.findBucket(tx, bucketPath)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", bucketPath)
		}

		bucketInfo := c.buildBucketInfo(b, filepath.Base(bucketPath), bucketPath, 0)

		// Get all key-value pairs
		b.ForEach(func(k, v []byte) error {
			if v != nil { // This is a key-value pair, not a sub-bucket
				kv := c.parseKeyValue(k, v)
				bucketInfo.Keys = append(bucketInfo.Keys, kv)
			}
			return nil
		})

		bucket = &bucketInfo
		return nil
	})

	return bucket, err
}

// findBucket finds bucket by path
func (c *ContainerdMetadataViewer) findBucket(tx *bolt.Tx, path string) *bolt.Bucket {
	// Normalize path, remove extra slashes
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}

	partsRaw := strings.Split(path, "/")
	// Filter out empty segments to avoid empty names from consecutive slashes
	parts := make([]string, 0, len(partsRaw))
	for _, p := range partsRaw {
		if p != "" {
			parts = append(parts, p)
		}
	}

	klog.Infof("findBucket: path=%q parts=%v", path, parts)
	if len(parts) == 0 {
		return nil
	}

	bucket := tx.Bucket([]byte(parts[0]))
	if bucket == nil {
		klog.Infof("findBucket: top-level bucket not found=%q", parts[0])
		return nil
	}
	klog.Infof("findBucket: found top-level bucket=%q", parts[0])

	for i := 1; i < len(parts); i++ {
		name := parts[i]
		next := bucket.Bucket([]byte(name))
		if next == nil {
			// Try to match remaining path as single sub-bucket name (handle names containing '/')
			remainder := strings.Join(parts[i:], "/")
			if try := bucket.Bucket([]byte(remainder)); try != nil {
				klog.Infof("findBucket: matching remaining path as single name: %q", remainder)
				bucket = try
				return bucket
			}

			// Further try longest match, merge segments from right to left
			matched := false
			for j := len(parts); j > i+1; j-- {
				candidate := strings.Join(parts[i:j], "/")
				if cand := bucket.Bucket([]byte(candidate)); cand != nil {
					klog.Infof("findBucket: matched sub-bucket by merging segments=%q (i=%d,j=%d)", candidate, i, j)
					bucket = cand
					i = j - 1 // Next loop starts from j
					matched = true
					break
				}
			}
			if matched {
				continue
			}

			// List sub-buckets at current level to help locate actual names
			kids := make([]string, 0, 20)
			_ = bucket.ForEach(func(k, v []byte) error {
				if v == nil {
					kids = append(kids, string(k))
				}
				return nil
			})
			if len(kids) > 20 {
				kids = kids[:20]
			}
			klog.Infof("findBucket: sub-bucket not found at level %d=%q. Available sub-buckets=%v", i, name, kids)
			return nil
		}
		bucket = next
		klog.Infof("findBucket: entering level %d sub-bucket=%q", i, name)
	}

	return bucket
}

// parseKeyValue parses key-value pairs
func (c *ContainerdMetadataViewer) parseKeyValue(key, value []byte) KeyValuePair {
	kv := KeyValuePair{
		Key:       string(key),
		ValueSize: len(value),
		IsBinary:  !c.isUTF8(value),
	}

	// Try to parse as JSON
	var jsonValue interface{}
	if json.Unmarshal(value, &jsonValue) == nil {
		kv.IsJSON = true
		kv.ValueType = "JSON"
		kv.Value = jsonValue

		// Format JSON preview
		if formatted, err := json.MarshalIndent(jsonValue, "", "  "); err == nil {
			kv.Preview = string(formatted)
			if len(kv.Preview) > 1000 {
				kv.Preview = kv.Preview[:1000] + "\n... (truncated)"
			}
		} else {
			kv.Preview = string(value)
		}
	} else if kv.IsBinary {
		kv.ValueType = "Binary"
		kv.Value = fmt.Sprintf("<%d bytes binary data>", len(value))
		kv.Preview = c.formatBinaryPreview(value)
	} else {
		kv.ValueType = "String"
		kv.Value = string(value)
		kv.Preview = string(value)
		if len(kv.Preview) > 1000 {
			kv.Preview = kv.Preview[:1000] + "\n... (truncated)"
		}
	}

	return kv
}

// isUTF8 checks if data is valid UTF-8
func (c *ContainerdMetadataViewer) isUTF8(data []byte) bool {
	if len(data) == 0 || len(data) > 1024*1024 { // No more than 1MB
		return false
	}

	// Check if contains null characters
	for _, b := range data {
		if b == 0 {
			return false
		}
	}

	// Check if valid UTF-8
	return utf8.ValidString(string(data))
}

// formatBinaryPreview formats binary data preview
func (c *ContainerdMetadataViewer) formatBinaryPreview(data []byte) string {
	if len(data) == 0 {
		return "(empty data)"
	}

	preview := "Hexadecimal preview:\n"
	maxBytes := 256
	if len(data) < maxBytes {
		maxBytes = len(data)
	}

	for i := 0; i < maxBytes; i += 16 {
		end := i + 16
		if end > maxBytes {
			end = maxBytes
		}

		// Hexadecimal
		hex := ""
		ascii := ""
		for j := i; j < end; j++ {
			hex += fmt.Sprintf("%02x ", data[j])
			if data[j] >= 32 && data[j] <= 126 {
				ascii += string(data[j])
			} else {
				ascii += "."
			}
		}

		// Pad with spaces
		for len(hex) < 48 {
			hex += " "
		}

		preview += fmt.Sprintf("%04x: %s |%s|\n", i, hex, ascii)
	}

	if len(data) > maxBytes {
		preview += fmt.Sprintf("... %d more bytes", len(data)-maxBytes)
	}

	return preview
}

// getKeyDetails gets detailed information for key
func (c *ContainerdMetadataViewer) getKeyDetails(bucketPath, keyName string) (*KeyValuePair, error) {
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	var keyValue *KeyValuePair

	err = db.View(func(tx *bolt.Tx) error {
		bucket := c.findBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket not found: %s", bucketPath)
		}

		value := bucket.Get([]byte(keyName))
		if value == nil {
			return fmt.Errorf("key not found: %s", keyName)
		}

		kv := KeyValuePair{
			Key:       keyName,
			ValueSize: len(value),
			IsBinary:  !c.isUTF8(value),
		}

		var jsonVal interface{}
		if json.Unmarshal(value, &jsonVal) == nil {
			kv.IsJSON = true
			kv.ValueType = "JSON"
			kv.Value = jsonVal
			// Preview shows complete JSON text (no truncation)
			if formatted, err := json.MarshalIndent(jsonVal, "", "  "); err == nil {
				kv.Preview = string(formatted)
			} else {
				kv.Preview = string(value)
			}
		} else if kv.IsBinary {
			kv.ValueType = "Binary"
			kv.Value = fmt.Sprintf("<%d bytes binary data>", len(value))
			kv.Preview = c.formatBinaryPreview(value)
		} else {
			kv.ValueType = "String"
			kv.Value = string(value)
			kv.Preview = string(value)
		}

		keyValue = &kv
		return nil
	})

	return keyValue, err
}

// getFullKeyData gets complete raw data for key (no truncation)
func (c *ContainerdMetadataViewer) getFullKeyData(bucketPath, keyName string) (*KeyValuePair, error) {
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	var keyValue *KeyValuePair

	err = db.View(func(tx *bolt.Tx) error {
		bucket := c.findBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket not found: %s", bucketPath)
		}

		value := bucket.Get([]byte(keyName))
		if value == nil {
			return fmt.Errorf("key not found: %s", keyName)
		}

		kv := KeyValuePair{
			Key:       keyName,
			ValueSize: len(value),
			IsBinary:  !c.isUTF8(value),
		}

		var jsonVal interface{}
		if json.Unmarshal(value, &jsonVal) == nil {
			kv.IsJSON = true
			kv.ValueType = "JSON"
			kv.Value = jsonVal
			if formatted, err := json.MarshalIndent(jsonVal, "", "  "); err == nil {
				kv.Preview = string(formatted)
			} else {
				kv.Preview = string(value)
			}
		} else if kv.IsBinary {
			kv.ValueType = "Binary"
			kv.Value = fmt.Sprintf("<%d bytes binary data>", len(value))
			// Generate complete hexadecimal preview (no length limit)
			preview := "Hexadecimal preview:\n"
			for i := 0; i < len(value); i += 16 {
				end := i + 16
				if end > len(value) {
					end = len(value)
				}
				hex := ""
				ascii := ""
				for j := i; j < end; j++ {
					hex += fmt.Sprintf("%02x ", value[j])
					if value[j] >= 32 && value[j] <= 126 {
						ascii += string(value[j])
					} else {
						ascii += "."
					}
				}
				for len(hex) < 48 {
					hex += " "
				}
				preview += fmt.Sprintf("%04x: %s |%s|\n", i, hex, ascii)
			}
			kv.Preview = preview
		} else {
			kv.ValueType = "String"
			kv.Value = string(value)
			kv.Preview = string(value)
		}

		keyValue = &kv
		return nil
	})

	return keyValue, err
}

// searchKeys search keys
func (c *ContainerdMetadataViewer) searchKeys(query string) ([]map[string]interface{}, error) {
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	var results []map[string]interface{}
	query = strings.ToLower(query)

	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return c.searchInBucket(tx, b, string(name), query, &results, 0, 100) // Return at most 100 results
		})
	})

	return results, err
}

// searchInBucket recursively searches in bucket
func (c *ContainerdMetadataViewer) searchInBucket(tx *bolt.Tx, bucket *bolt.Bucket, path, query string, results *[]map[string]interface{}, found, maxResults int) error {
	if len(*results) >= maxResults {
		return nil
	}

	return bucket.ForEach(func(k, v []byte) error {
		keyName := string(k)
		currentPath := path
		if currentPath != "" {
			currentPath += "/"
		}
		currentPath += keyName

		if v == nil { // Sub-bucket
			subBucket := bucket.Bucket(k)
			if subBucket != nil {
				return c.searchInBucket(tx, subBucket, currentPath, query, results, len(*results), maxResults)
			}
		} else { // Key-value pair
			if strings.Contains(strings.ToLower(keyName), query) {
				kv := c.parseKeyValue(k, v)
				preview := kv.Preview
				if len(preview) > 200 {
					preview = preview[:200] + "..."
				}

				*results = append(*results, map[string]interface{}{
					"bucket":  path,
					"key":     keyName,
					"path":    currentPath,
					"type":    kv.ValueType,
					"size":    kv.ValueSize,
					"preview": preview,
				})

				if len(*results) >= maxResults {
					return nil
				}
			}
		}
		return nil
	})
}

// getDatabaseStats gets database statistics
func (c *ContainerdMetadataViewer) getDatabaseStats() (map[string]interface{}, error) {
	db, err := bolt.Open(c.dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	stats := db.Stats()

	// Get file information
	fileInfo, err := os.Stat(c.dbPath)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"database": map[string]interface{}{
			"path":         c.dbPath,
			"size":         fileInfo.Size(),
			"lastModified": fileInfo.ModTime(),
			"freePageN":    stats.FreePageN,
			"pendingPageN": stats.PendingPageN,
		},
		"transactions": map[string]interface{}{
			"txN":     stats.TxN,
			"openTxN": stats.OpenTxN,
		},
	}, nil
}

// Helper functions
func (c *ContainerdMetadataViewer) sendSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	response := APIResponse{
		Success: true,
		Data:    data,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		klog.Errorf("Failed to encode JSON response: %v", err)
	}
}

func (c *ContainerdMetadataViewer) sendError(w http.ResponseWriter, message string, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusInternalServerError)

	errorMsg := message
	if err != nil {
		errorMsg += ": " + err.Error()
	}

	response := APIResponse{
		Success: false,
		Error:   errorMsg,
	}

	if encodeErr := json.NewEncoder(w).Encode(response); encodeErr != nil {
		klog.Errorf("Failed to encode error response: %v", encodeErr)
	}
}

func main() {
	dbPath := "/var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db"

	// Check command line arguments
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		klog.Fatalf("Database file does not exist: %s", dbPath)
	}

	viewer := NewContainerdMetadataViewer(dbPath)

	port := 8081
	if portStr := os.Getenv("PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	klog.Fatal(viewer.StartServer(port))
}
