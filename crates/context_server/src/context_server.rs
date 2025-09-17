pub mod client;
pub mod listener;
pub mod protocol;
#[cfg(any(test, feature = "test-support"))]
pub mod test;
pub mod transport;
pub mod types;

use std::path::Path;
use std::sync::Arc;
use std::{fmt::Display, path::PathBuf};

use anyhow::Result;
use client::Client;
use collections::HashMap;
use gpui::AsyncApp;
use parking_lot::RwLock;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;
use util::redact::should_redact;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContextServerId(pub Arc<str>);

impl Display for ContextServerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, JsonSchema)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum ContextServerCommand {
    Stdio {
        #[serde(rename = "command")]
        path: PathBuf,
        args: Vec<String>,
        env: Option<HashMap<String, String>>,

        timeout: Option<u64>,
    },

    HttpSse {
        sse_url: String,

        post_url: Option<String>,

        timeout: Option<u64>,
    },

    HttpStreamable {
        url: String,

        timeout: Option<u64>,
    },
}

impl ContextServerCommand {
    pub fn stdio(
        path: PathBuf,
        args: Vec<String>,
        env: Option<HashMap<String, String>>,
        timeout: Option<u64>,
    ) -> Self {
        Self::Stdio {
            path,
            args,
            env,
            timeout,
        }
    }

    pub fn http_sse(sse_url: String, post_url: Option<String>, timeout: Option<u64>) -> Self {
        Self::HttpSse {
            sse_url,
            post_url,
            timeout,
        }
    }

    pub fn http_streamable(url: String, timeout: Option<u64>) -> Self {
        Self::HttpStreamable { url, timeout }
    }

    pub fn timeout(&self) -> u64 {
        match self {
            Self::Stdio { timeout, .. } => timeout.unwrap_or(60000),
            Self::HttpSse { timeout, .. } => timeout.unwrap_or(60000),
            Self::HttpStreamable { timeout, .. } => timeout.unwrap_or(60000),
        }
    }
}

impl std::fmt::Debug for ContextServerCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdio {
                path,
                args,
                env,
                timeout,
            } => {
                let filtered_env = env.as_ref().map(|env| {
                    env.iter()
                        .map(|(k, v)| (k, if should_redact(k) { "[REDACTED]" } else { v }))
                        .collect::<Vec<_>>()
                });

                f.debug_struct("ContextServerCommand::Stdio")
                    .field("path", path)
                    .field("args", args)
                    .field("env", &filtered_env)
                    .field("timeout", timeout)
                    .finish()
            }
            Self::HttpSse {
                sse_url,
                post_url,
                timeout,
            } => f
                .debug_struct("ContextServerCommand::HttpSse")
                .field("sse_url", sse_url)
                .field("post_url", post_url)
                .field("timeout", timeout)
                .finish(),
            Self::HttpStreamable { url, timeout } => f
                .debug_struct("ContextServerCommand::HttpStreamable")
                .field("url", url)
                .field("timeout", timeout)
                .finish(),
        }
    }
}

enum ContextServerTransport {
    Stdio {
        path: PathBuf,
        args: Vec<String>,
        env: Option<HashMap<String, String>>,
        timeout: Option<u64>,
        working_directory: Option<PathBuf>,
    },
    HttpSse {
        sse_url: Url,
        post_url: Option<Url>,
        timeout: Option<u64>,
    },
    HttpStreamable {
        url: Url,
        timeout: Option<u64>,
    },
    Custom(Arc<dyn crate::transport::Transport>),
}

pub struct ContextServer {
    id: ContextServerId,
    client: RwLock<Option<Arc<crate::protocol::InitializedContextServerProtocol>>>,
    configuration: ContextServerTransport,
}

impl ContextServer {
    pub fn stdio(
        id: ContextServerId,
        command: ContextServerCommand,
        working_directory: Option<Arc<Path>>,
    ) -> Self {
        match command {
            ContextServerCommand::Stdio {
                path,
                args,
                env,
                timeout,
            } => Self {
                id,
                client: RwLock::new(None),
                configuration: ContextServerTransport::Stdio {
                    path,
                    args,
                    env,
                    timeout,
                    working_directory: working_directory.map(|directory| directory.to_path_buf()),
                },
            },
            _ => panic!("stdio() constructor requires ContextServerCommand::Stdio variant"),
        }
    }

    pub fn new(id: ContextServerId, transport: Arc<dyn crate::transport::Transport>) -> Self {
        Self {
            id,
            client: RwLock::new(None),
            configuration: ContextServerTransport::Custom(transport),
        }
    }

    pub fn http_sse(
        id: ContextServerId,
        sse_url: Url,
        post_url: Option<Url>,
        timeout: Option<u64>,
    ) -> Self {
        Self {
            id,
            client: RwLock::new(None),
            configuration: ContextServerTransport::HttpSse {
                sse_url,
                post_url,
                timeout,
            },
        }
    }

    pub fn http_streamable(id: ContextServerId, url: Url, timeout: Option<u64>) -> Self {
        Self {
            id,
            client: RwLock::new(None),
            configuration: ContextServerTransport::HttpStreamable { url, timeout },
        }
    }

    pub fn id(&self) -> ContextServerId {
        self.id.clone()
    }

    pub fn client(&self) -> Option<Arc<crate::protocol::InitializedContextServerProtocol>> {
        self.client.read().clone()
    }

    pub async fn start(&self, cx: &AsyncApp) -> Result<()> {
        self.initialize(self.new_client(cx)?).await
    }

    /// Starts the context server, making sure handlers are registered before initialization happens
    pub async fn start_with_handlers(
        &self,
        notification_handlers: Vec<(
            &'static str,
            Box<dyn 'static + Send + FnMut(serde_json::Value, AsyncApp)>,
        )>,
        cx: &AsyncApp,
    ) -> Result<()> {
        let client = self.new_client(cx)?;
        for (method, handler) in notification_handlers {
            client.on_notification(method, handler);
        }
        self.initialize(client).await
    }

    fn new_client(&self, cx: &AsyncApp) -> Result<Client> {
        Ok(match &self.configuration {
            ContextServerTransport::Stdio {
                path,
                args,
                env,
                timeout,
                working_directory,
            } => Client::stdio(
                client::ContextServerId(self.id.0.clone()),
                client::ModelContextServerBinary {
                    executable: path.clone(),
                    args: args.clone(),
                    env: env.clone(),
                    timeout: *timeout,
                },
                working_directory,
                cx.clone(),
            )?,
            ContextServerTransport::HttpSse {
                sse_url,
                post_url,
                timeout,
            } => Client::http_sse(
                client::ContextServerId(self.id.0.clone()),
                self.id().0,
                sse_url.clone(),
                post_url.clone(),
                timeout.map(std::time::Duration::from_millis),
                cx.clone(),
            )?,
            ContextServerTransport::HttpStreamable { url, timeout } => Client::http_streamable(
                client::ContextServerId(self.id.0.clone()),
                self.id().0,
                url.clone(),
                timeout.map(std::time::Duration::from_millis),
                cx.clone(),
            )?,
            ContextServerTransport::Custom(transport) => Client::new(
                client::ContextServerId(self.id.0.clone()),
                self.id().0,
                transport.clone(),
                None,
                cx.clone(),
            )?,
        })
    }

    async fn initialize(&self, client: Client) -> Result<()> {
        log::debug!("starting context server {}", self.id);
        let protocol = crate::protocol::ModelContextProtocol::new(client);
        let client_info = types::Implementation {
            name: "Zed".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let initialized_protocol = protocol.initialize(client_info).await?;

        log::debug!(
            "context server {} initialized: {:?}",
            self.id,
            initialized_protocol.initialize,
        );

        *self.client.write() = Some(Arc::new(initialized_protocol));
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        let mut client = self.client.write();
        if let Some(protocol) = client.take() {
            drop(protocol);
        }
        Ok(())
    }
}
