use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::AsyncBufReadExt;
use futures::stream::Stream;
use gpui::AsyncApp;
use http_client::{AsyncBody, HttpClient, http};
use parking_lot::Mutex;
use reqwest_client::ReqwestClient;
use serde_json::Value;
use smol::channel;
use url::Url;

use crate::transport::Transport;

#[derive(Debug, Clone)]
pub enum HttpTransportConfig {
    Sse { sse_url: Url, post_url: Option<Url> },
    StreamableHttp { url: Url },
}

pub struct HttpTransport {
    config: HttpTransportConfig,
    client: Arc<ReqwestClient>,
    _message_sender: channel::Sender<String>,
    message_receiver: channel::Receiver<String>,
    error_receiver: channel::Receiver<String>,
    post_endpoint: Arc<Mutex<Option<Url>>>,
}

impl HttpTransport {
    pub fn new(config: HttpTransportConfig, cx: &AsyncApp) -> Result<Self> {
        let client = Arc::new(ReqwestClient::new());
        let (message_sender, message_receiver) = channel::unbounded::<String>();
        let (error_sender, error_receiver) = channel::unbounded::<String>();

        let post_endpoint = Arc::new(Mutex::new(
            if let HttpTransportConfig::Sse {
                post_url: Some(url),
                ..
            } = &config
            {
                Some(url.clone())
            } else {
                None
            },
        ));

        match &config {
            HttpTransportConfig::Sse { sse_url, .. } => {
                let sse_url = sse_url.clone();
                let client = client.clone();
                let message_sender = message_sender.clone();
                let error_sender = error_sender.clone();
                let post_endpoint = post_endpoint.clone();

                cx.spawn(async move |_| {
                    Self::handle_sse_connection(
                        sse_url,
                        client,
                        message_sender,
                        error_sender,
                        post_endpoint,
                    )
                    .await
                    .unwrap_or_else(|e| {
                        log::error!("SSE connection error: {}", e);
                    });
                })
                .detach();
            }
            HttpTransportConfig::StreamableHttp { url } => {
                let url = url.clone();
                let client = client.clone();
                let message_sender = message_sender.clone();
                let error_sender = error_sender.clone();

                cx.spawn(async move |_| {
                    Self::handle_streamable_http_connection(
                        url,
                        client,
                        message_sender,
                        error_sender,
                    )
                    .await
                    .unwrap_or_else(|e| {
                        log::error!("Streamable HTTP connection error: {}", e);
                    });
                })
                .detach();
            }
        }

        Ok(Self {
            config,
            client,
            _message_sender: message_sender,
            message_receiver,
            error_receiver,
            post_endpoint,
        })
    }

    async fn handle_sse_connection(
        sse_url: Url,
        client: Arc<ReqwestClient>,
        message_sender: channel::Sender<String>,
        _error_sender: channel::Sender<String>,
        post_endpoint: Arc<Mutex<Option<Url>>>,
    ) -> Result<()> {
        log::debug!("Connecting to SSE endpoint: {}", sse_url);

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(sse_url.as_str())
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .body(AsyncBody::empty())?;

        let response = client.send(request).await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "SSE connection failed with status: {}",
                response.status()
            ));
        }

        let body = response.into_body();
        let mut reader = futures::io::BufReader::new(body);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break; // EOF
            }

            let line = line.trim();

            if line.is_empty() || line.starts_with(':') {
                continue;
            }

            if let Some(event_data) = line.strip_prefix("data: ") {
                if let Ok(event_value) = serde_json::from_str::<Value>(event_data) {
                    // Handle endpoint event
                    if let Some(endpoint) = event_value.get("endpoint").and_then(|e| e.as_str()) {
                        if let Ok(endpoint_url) = Url::parse(endpoint) {
                            *post_endpoint.lock() = Some(endpoint_url);
                            continue;
                        }
                    }
                    if let Ok(message) = serde_json::to_string(&event_value) {
                        if message_sender.send(message).await.is_err() {
                            break;
                        }
                    }
                } else {
                    if message_sender.send(event_data.to_string()).await.is_err() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_streamable_http_connection(
        url: Url,
        client: Arc<ReqwestClient>,
        message_sender: channel::Sender<String>,
        _error_sender: channel::Sender<String>,
    ) -> Result<()> {
        log::debug!("Connecting to Streamable HTTP endpoint: {}", url);

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(url.as_str())
            .header("Accept", "application/x-ndjson")
            .header("Content-Type", "application/x-ndjson")
            .body(AsyncBody::empty())?;

        let response = client.send(request).await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Streamable HTTP connection failed with status: {}",
                response.status()
            ));
        }

        let body = response.into_body();
        let mut reader = futures::io::BufReader::new(body);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break; // EOF
            }

            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            if message_sender.send(line.to_string()).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    async fn send_http_message(&self, message: String) -> Result<()> {
        let endpoint_url = match &self.config {
            HttpTransportConfig::Sse {
                post_url: Some(url),
                ..
            } => url.clone(),
            HttpTransportConfig::Sse { .. } => {
                let endpoint = self
                    .post_endpoint
                    .lock()
                    .clone()
                    .ok_or_else(|| anyhow!("No POST endpoint available from server"))?;
                endpoint
            }
            HttpTransportConfig::StreamableHttp { url } => url.clone(),
        };

        log::trace!("Sending HTTP message to {}: {}", endpoint_url, message);

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(endpoint_url.as_str())
            .header("Content-Type", "application/json")
            .body(AsyncBody::from(message.into_bytes()))?;

        let response = self.client.send(request).await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "HTTP request failed with status: {}",
                response.status()
            ));
        }

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
