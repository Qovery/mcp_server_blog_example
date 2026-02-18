# Kubernetes Logs MCP Server

An MCP (Model Context Protocol) server that provides tools to fetch Kubernetes pod logs.

## Features

- Fetch logs from any Kubernetes pod
- Support for multi-container pods (specify container name)
- Configurable number of log lines to retrieve
- Optional log streaming (follow mode)
- Built with Rust using Axum and the Kube client

## Prerequisites

- Rust 1.70 or later
- Access to a Kubernetes cluster
- Valid `KUBECONFIG` configured, or running in-cluster with appropriate service account permissions

## Installation

```bash
cargo build --release
```

## Running the Server

```bash
cargo run
```

The server will start on `http://0.0.0.0:8080` with the following endpoints:

- `POST /mcp` - MCP protocol endpoint
- `GET /health` - Health check endpoint

## MCP Tool: get_kubernetes_logs

Fetches logs from a Kubernetes pod.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `namespace` | string | Yes | - | The Kubernetes namespace where the pod is located |
| `pod_name` | string | Yes | - | The name of the pod to fetch logs from |
| `container` | string | No | - | The specific container name within the pod. If not specified, logs from the first container will be fetched |
| `tail_lines` | integer | No | 100 | Number of lines to fetch from the end of the logs |
| `follow` | boolean | No | false | Whether to follow the logs (stream) |

### Example Usage

Using the MCP protocol to fetch logs:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "get_kubernetes_logs",
    "arguments": {
      "namespace": "default",
      "pod_name": "my-app-pod",
      "tail_lines": 50
    }
  },
  "id": 1
}
```

With container specification:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "get_kubernetes_logs",
    "arguments": {
      "namespace": "production",
      "pod_name": "my-app-pod",
      "container": "main-container",
      "tail_lines": 200
    }
  },
  "id": 1
}
```

## Configuration

The server uses the default Kubernetes client configuration:

1. In-cluster config (when running inside a Kubernetes pod)
2. `KUBECONFIG` environment variable
3. `~/.kube/config` file

## Error Handling

The server provides clear error messages for common issues:

- Kubernetes client connection failures
- Pod not found
- Container not found
- Permission issues
- Network errors

## Development

### Project Structure

- `src/main.rs` - Main server implementation with MCP tool definitions
- `Cargo.toml` - Dependencies and project configuration

### Key Dependencies

- `mcp-server-axum` - MCP protocol implementation for Axum
- `kube` - Kubernetes client library
- `k8s-openapi` - Kubernetes API types
- `axum` - Web framework
- `tokio` - Async runtime

## License

This project is provided as-is for demonstration purposes.
