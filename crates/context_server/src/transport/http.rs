use std::{pin::Pin, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::{AsyncBufReadExt, AsyncReadExt, Stream};
use gpui::AsyncApp;
use http_client::{AsyncBody, HttpClient, http};
use parking_lot::RwLock;
use reqwest_client::ReqwestClient;
use serde_json::Value;
use smol::channel;
use thiserror::Error;
use url::Url;

use crate::transport::Transport;

/// Errors that can occur during HTTP transport operations.
#[derive(Error, Debug)]
pub enum HttpTransportError {
    #[error("HTTP request failed with status {status}: {body}")]
    HttpRequestFailed {
        status: http::StatusCode,
        body: String,
    },

    #[error("Server-Sent Events connection failed: {0}")]
    SseConnectionFailed(String),

    #[error("JSON-RPC validation failed: {0}")]
    JsonRpcValidationFailed(String),

    #[error("Unexpected content type: expected {expected}, got {actual}")]
    UnexpectedContentType { expected: String, actual: String },

    #[error("Failed to send message through channel")]
    ChannelSendFailed,

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("HTTP client error: {0}")]
    HttpClientError(#[from] anyhow::Error),

    #[error("JSON parsing error: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("URL parsing error: {0}")]
    UrlParseError(#[from] url::ParseError),
}

/// Configuration for HTTP transport behavior.
#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Timeout for HTTP requests
    pub request_timeout: Duration,
    /// Custom origin header value
    pub origin: String,
    /// Buffer size for message channels
    pub channel_buffer_size: usize,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            origin: "zed://local".to_string(),
            channel_buffer_size: 1000,
        }
    }
}

/// Session information for MCP over HTTP
#[derive(Debug, Clone, Default)]
struct SessionInfo {
    session_id: Option<String>,
    protocol_version: Option<String>,
}

/// Transport mode - either standard MCP-over-HTTP or legacy SSE mode
#[derive(Debug, Clone)]
enum TransportMode {
    /// Standard MCP over HTTP
    Standard,
    /// Legacy SSE mode with discovered endpoint
    LegacySse { endpoint: Url },
}

/// HTTP transport for MCP (Model Context Protocol) servers.
///
/// This transport implementation provides robust HTTP communication with MCP servers,
/// supporting both standard MCP-over-HTTP and legacy Server-Sent Events (SSE) mode
/// for backwards compatibility.
///
/// ## Features
///
/// - **Standard MCP-over-HTTP**: Full support for the MCP HTTP transport specification
/// - **Legacy SSE fallback**: Automatic fallback to SSE mode for older servers
/// - **Session management**: Automatic handling of session IDs and protocol versions
/// - **Request/notification handling**: Proper differentiation between requests and notifications
/// - **Error handling**: Comprehensive error reporting with proper context
/// - **Configurable behavior**: Customizable timeouts, origins, and buffer sizes
///
/// ## Example
///
/// ```rust
/// use url::Url;
/// use crate::transport::{HttpTransport, HttpTransportConfig};
///
/// let url = Url::parse("http://localhost:8080/mcp").unwrap();
/// let config = HttpTransportConfig::default();
/// let transport = HttpTransport::with_config(url, config, &cx)?;
/// ```
pub struct HttpTransport {
    /// Base URL for the MCP server
    base_url: Url,
    /// HTTP client for making requests
    client: Arc<ReqwestClient>,
    /// Configuration for transport behavior
    config: HttpTransportConfig,
    /// Session information (session ID, protocol version)
    session: RwLock<SessionInfo>,
    /// Current transport mode
    mode: Arc<RwLock<TransportMode>>,
    /// Channel for sending messages to the client
    message_sender: channel::Sender<String>,
    /// Channel for receiving messages from the server
    message_receiver: channel::Receiver<String>,
    /// Channel for sending error messages
    error_sender: channel::Sender<String>,
    /// Channel for receiving error messages
    error_receiver: channel::Receiver<String>,
}

impl HttpTransport {
    /// Creates a new HTTP transport with default configuration.
    pub fn new(base_url: Url, _cx: &AsyncApp) -> Result<Self> {
        Self::with_config(base_url, HttpTransportConfig::default(), _cx)
    }

    /// Creates a new HTTP transport with custom configuration.
    pub fn with_config(base_url: Url, config: HttpTransportConfig, _cx: &AsyncApp) -> Result<Self> {
        let client = Arc::new(ReqwestClient::new());
        let (message_sender, message_receiver) = channel::bounded(config.channel_buffer_size);
    let (error_sender, error_receiver) = channel::bounded(config.channel_buffer_size);

        let transport = Self {
            base_url,
            client,
            config,
            session: RwLock::new(SessionInfo::default()),
            mode: Arc::new(RwLock::new(TransportMode::Standard)),
            message_sender,
            message_receiver,
            error_sender,
            error_receiver,
        };

        Ok(transport)
    }

    /// Gets the current session ID.
    pub fn session_id(&self) -> Option<String> {
        self.session.read().session_id.clone()
    }

    /// Sets the session ID.
    pub fn set_session_id(&self, session_id: Option<String>) {
        self.session.write().session_id = session_id;
    }

    /// Gets the current protocol version.
    pub fn protocol_version(&self) -> Option<String> {
        self.session.read().protocol_version.clone()
    }

    /// Sets the protocol version.
    pub fn set_protocol_version(&self, version: Option<String>) {
        self.session.write().protocol_version = version;
    }

    /// Validates an outbound JSON-RPC message for basic correctness.
    fn validate_jsonrpc_message(payload: &str) -> Result<(), HttpTransportError> {
        let value: Value = serde_json::from_str(payload)?;

        // Basic JSON-RPC 2.0 validation
        if value.get("jsonrpc") != Some(&Value::String("2.0".to_string())) {
            return Err(HttpTransportError::JsonRpcValidationFailed(
                "Missing or invalid jsonrpc version".to_string(),
            ));
        }

        if value.get("method").is_none() {
            return Err(HttpTransportError::JsonRpcValidationFailed(
                "Missing method field".to_string(),
            ));
        }

        if let Some(method) = value.get("method") {
            if !method.is_string() {
                return Err(HttpTransportError::JsonRpcValidationFailed(
                    "Method must be a string".to_string(),
                ));
            }
        }

        if let Some(id) = value.get("id") {
            if !id.is_string() && !id.is_number() {
                return Err(HttpTransportError::JsonRpcValidationFailed(
                    "ID must be a string or number".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Determines the target URL based on current transport mode.
    fn get_target_url(&self) -> Url {
        match &*self.mode.read() {
            TransportMode::Standard => self.base_url.clone(),
            TransportMode::LegacySse { endpoint } => endpoint.clone(),
        }
    }

    /// Builds an HTTP request with appropriate headers.
    fn build_request(
        &self,
        url: &Url,
        body: Vec<u8>,
    ) -> Result<http::Request<AsyncBody>, HttpTransportError> {
        let mut builder = http::Request::builder()
            .method(http::Method::POST)
            .uri(url.as_str())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .header("Content-Length", body.len().to_string())
            .header("Origin", &self.config.origin);

        // Add session headers if available
        let session = self.session.read();
        if let Some(session_id) = &session.session_id {
            builder = builder.header("Mcp-Session-Id", session_id);
        }
        if let Some(version) = &session.protocol_version {
            builder = builder.header("MCP-Protocol-Version", version);
        }

        builder.body(AsyncBody::from(body)).map_err(|e| {
            HttpTransportError::HttpClientError(anyhow::anyhow!(
                "Failed to build HTTP request: {}",
                e
            ))
        })
    }

    /// Determines if a JSON-RPC message is a request (has both id and method).
    fn is_request_message(payload: &[u8]) -> bool {
        serde_json::from_slice::<Value>(payload)
            .map(|v| v.get("id").is_some() && v.get("method").is_some())
            .unwrap_or(false)
    }

    /// Handles SSE connection for legacy mode.
    async fn handle_sse_connection(
        sse_url: Url,
        client: Arc<ReqwestClient>,
        message_sender: channel::Sender<String>,
        error_sender: channel::Sender<String>,
        mode: Arc<RwLock<TransportMode>>,
    ) -> Result<()> {
        log::debug!("Starting SSE connection to: {}", sse_url);

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(sse_url.as_str())
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .body(AsyncBody::empty())
            .map_err(|e| anyhow!("Failed to build SSE request: {}", e))?;

        let response = client
            .send(request)
            .await
            .map_err(|e| anyhow!("SSE connection failed: {}", e))?;

        if !response.status().is_success() {
            let msg = format!(
                "SSE connection failed with status: {}",
                response.status()
            );
            let _ = error_sender.send(msg.clone()).await; // best effort
            return Err(anyhow!(msg));
        }

        let body = response.into_body();
        let mut reader = futures::io::BufReader::new(body);
        let mut line = String::new();

        while {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => false,
                Ok(_) => true,
                Err(e) => {
                    log::debug!("SSE read error: {}", e);
                    false
                }
            }
        } {
            let trimmed = line.trim();

            // Skip empty lines and comments
            if trimmed.is_empty() || trimmed.starts_with(':') {
                continue;
            }

            if let Some(data) = trimmed.strip_prefix("data: ") {
                if let Ok(event_value) = serde_json::from_str::<Value>(data) {
                    // Handle endpoint discovery for legacy mode
                    if let Some(endpoint) = event_value.get("endpoint").and_then(|e| e.as_str()) {
                        if let Ok(endpoint_url) = Url::parse(endpoint) {
                            log::debug!("Discovered legacy SSE endpoint: {}", endpoint_url);
                            *mode.write() = TransportMode::LegacySse {
                                endpoint: endpoint_url,
                            };
                            continue;
                        }
                    }
                }

                // Send the message regardless of whether it's valid JSON
                if message_sender.send(data.to_string()).await.is_err() {
                    log::debug!("Message receiver dropped, stopping SSE connection");
                    break;
                }
            }
        }

        log::debug!("SSE connection ended");
        Ok(())
    }

    /// Sends an HTTP message to the MCP server.
    async fn send_http_message(&self, message: String) -> Result<()> {
        // Validate the JSON-RPC message
        if let Err(e) = Self::validate_jsonrpc_message(&message) {
            log::warn!("JSON-RPC validation warning: {:?}", e);
        }

        let body_bytes = message.into_bytes();
        let is_request = Self::is_request_message(&body_bytes);
        let target_url = self.get_target_url();

        log::trace!(
            "Sending HTTP message to {} (len={}, is_request={})",
            target_url,
            body_bytes.len(),
            is_request
        );

        let request = self.build_request(&target_url, body_bytes)?;
        let response = self.client.send(request).await?;

        // Update session information from response headers
        self.update_session_from_response(&response);

        // Handle non-success status codes
        if !response.status().is_success() {
            let status = response.status();
            let body = self.read_response_body(response).await?;

            // Check if we should fall back to legacy SSE mode
            if matches!(
                status,
                http::StatusCode::NOT_FOUND | http::StatusCode::METHOD_NOT_ALLOWED
            ) {
                if matches!(*self.mode.read(), TransportMode::Standard) {
                    log::debug!("Falling back to legacy SSE mode");
                    self.start_legacy_sse_mode().await?;
                }
            }
            let err_msg = format!("HTTP request failed: {} - {}", status, body);
            let _ = self.error_sender.send(err_msg.clone()).await; // best effort
            return Err(anyhow!(err_msg));
        }

        // Handle success responses
        if !is_request {
            // For notifications, expect 202 Accepted
            if response.status() != http::StatusCode::ACCEPTED {
                log::warn!(
                    "Expected 202 Accepted for notification, got {}",
                    response.status()
                );
            }
            return Ok(());
        }

        // For requests, handle the response content
        self.handle_response_content(response).await
    }

    /// Updates session information from HTTP response headers.
    fn update_session_from_response(&self, response: &http::Response<AsyncBody>) {
        let mut session = self.session.write();

        if let Some(session_id) = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
        {
            if session.session_id.as_deref() != Some(session_id) {
                session.session_id = Some(session_id.to_string());
                log::debug!("Updated session ID: {}", session_id);
            }
        }

        if let Some(version) = response
            .headers()
            .get("MCP-Protocol-Version")
            .and_then(|v| v.to_str().ok())
        {
            session.protocol_version = Some(version.to_string());
        }
    }

    /// Reads the entire response body as a string.
    async fn read_response_body(&self, response: http::Response<AsyncBody>) -> Result<String> {
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        body.read_to_end(&mut bytes).await?;
        Ok(String::from_utf8(bytes).unwrap_or_else(|_| "[Invalid UTF-8]".to_string()))
    }

    /// Handles response content based on content type.
    async fn handle_response_content(&self, response: http::Response<AsyncBody>) -> Result<()> {
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if content_type.starts_with("application/json") {
            let body = self.read_response_body(response).await?;
            self.message_sender
                .send(body)
                .await
                .map_err(|_| anyhow!("Failed to send message to channel"))?;
        } else if content_type.starts_with("text/event-stream") {
            self.handle_sse_response(response).await?;
        } else {
            let msg = format!(
                "Unexpected content type: expected application/json or text/event-stream, got {}",
                content_type
            );
            let _ = self.error_sender.send(msg.clone()).await; // best effort
            return Err(anyhow!(msg));
        }

        Ok(())
    }

    /// Handles Server-Sent Events response.
    async fn handle_sse_response(&self, response: http::Response<AsyncBody>) -> Result<()> {
        let body = response.into_body();
        let mut reader = futures::io::BufReader::new(body);
        let mut line = String::new();

        while {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => false,
                Ok(_) => true,
                Err(e) => {
                    log::debug!("SSE response read error: {}", e);
                    false
                }
            }
        } {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with(':') {
                continue;
            }

            if let Some(data) = trimmed.strip_prefix("data: ") {
                self.message_sender
                    .send(data.to_string())
                    .await
                    .map_err(|_| anyhow!("Failed to send SSE data to channel"))?;
            }
        }

        Ok(())
    }

    /// Starts legacy SSE mode for backwards compatibility.
    async fn start_legacy_sse_mode(&self) -> Result<()> {
        let sse_url = self.base_url.clone();
        let client = self.client.clone();
        let message_sender = self.message_sender.clone();
        let error_sender = self.error_sender.clone();
    let mode = self.mode.clone();

        smol::spawn(async move {
            if let Err(e) = Self::handle_sse_connection(
                sse_url,
                client,
                message_sender,
                error_sender,
                mode,
            )
            .await
            {
                log::debug!("Legacy SSE connection failed: {:?}", e);
            }
        })
        .detach();

        Ok(())
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, message: String) -> Result<()> {
        self.send_http_message(message).await
    }

    fn receive(&self) -> Pin<Box<dyn Stream<Item = String> + Send>> {
        Box::pin(self.message_receiver.clone())
    }

    fn receive_err(&self) -> Pin<Box<dyn Stream<Item = String> + Send>> {
        Box::pin(self.error_receiver.clone())
    }
}
