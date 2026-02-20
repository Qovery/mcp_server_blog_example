use std::error::Error;
use std::sync::LazyLock;
use anyhow::Result;
use futures::{stream, AsyncBufReadExt, StreamExt, TryStreamExt};
use futures::stream::FuturesUnordered;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::Resolver;
use hickory_resolver::proto::rr::{RData, RecordType};
use k8s_openapi::api::core::v1::Event;
use kube::{
    Api,
    api::{ApiResource, DynamicObject, GroupVersionKind, ListParams, LogParams},
    Client,
};
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::router::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::*,
    schemars, tool, tool_handler, tool_router,
    transport::stdio,
};
use tokio_rustls::rustls;
use tracing_subscriber::{self, EnvFilter};

#[derive(Clone)]
pub struct KubeServer {
    client: Client,
    tool_router: ToolRouter<Self>,
    dns_resolver: Resolver<TokioConnectionProvider>,
    http_client: reqwest::Client,
}

#[tool_router]
impl KubeServer {
    pub fn new(client: Client, dns_resolver: Resolver<TokioConnectionProvider>, http_client: reqwest::Client) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
            dns_resolver,
            http_client,
        }
    }

    /// Fetch Kubernetes events from a namespace.
    /// Returns the most recent events with their type, reason, message, and involved object.
    #[tool(description = "Fetch Kubernetes events from a namespace. Returns recent events with type, reason, message, and involved object.")]
    async fn get_kube_events(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let events: Api<Event> = Api::all(self.client.clone());
        let lp = ListParams::default();
        let event_list = events.list(&lp).await.map_err(|e| rmcp::ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: format!("Failed to list events: {e}").into(),
            data: None,
        })?;

        let mut output = String::new();
        for event in event_list.items {
            let namespace = event
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("unknown");
            let kind = event
                .involved_object
                .kind
                .as_deref()
                .unwrap_or("unknown");
            let name = event
                .involved_object
                .name
                .as_deref()
                .unwrap_or("unknown");
            let reason = event.reason.as_deref().unwrap_or("unknown");
            let message = event.message.as_deref().unwrap_or("");
            let event_type = event.type_.as_deref().unwrap_or("Normal");
            let timestamp = event
                .last_timestamp
                .as_ref()
                .map(|t| t.0)
                .unwrap_or_default();

            output.push_str(&format!(
                "[{timestamp}] [{event_type}] {namespace}/{kind}/{name}: {reason} - {message}\n"
            ));
        }

        if output.is_empty() {
            output = "No events found.".to_string();
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Fetch logs from cert-manager pods.
    /// Returns the logs from all cert-manager pods in the cert-manager namespace.
    #[tool(description = "Fetch logs from cert-manager pods in the cert-manager namespace. Returns recent log lines from all cert-manager pods.")]
    async fn get_cert_manager_logs(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let pods: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.client.clone(), "cert-manager");
        let lp = ListParams::default().labels("app.kubernetes.io/instance=cert-manager");

        let pod_list = pods.list(&lp).await.map_err(|e| rmcp::ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: format!("Failed to list cert-manager pods: {e}").into(),
            data: None,
        })?;

        let mut output = String::new();

        for pod in pod_list.items {
            let pod_name = pod.metadata.name.as_deref().unwrap_or("unknown");
            output.push_str(&format!("=== Pod: {pod_name} ===\n"));

            let log_params = LogParams {
                tail_lines: Some(100),
                ..Default::default()
            };

            match pods.log_stream(pod_name, &log_params).await {
                Ok(stream) => {
                    let mut lines = stream.lines();
                    while let Some(line) = lines.try_next().await.map_err(|e| rmcp::ErrorData {
                        code: ErrorCode::INTERNAL_ERROR,
                        message: format!("Failed to read log stream for {pod_name}: {e}").into(),
                        data: None,
                    })? {
                        output.push_str(&line);
                        output.push('\n');
                    }
                }
                Err(e) => {
                    output.push_str(&format!("Failed to get logs: {e}\n"));
                }
            }

            output.push('\n');
        }

        if output.is_empty() {
            output = "No cert-manager pods found.".to_string();
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Fetch the status of cert-manager CRDs: CertificateRequest, Order, and Challenge across all namespaces.
    #[tool(description = "Fetch the status of cert-manager CertificateRequests, Orders, and Challenges across all namespaces. Shows name, namespace, ready condition, and reason for each resource.")]
    async fn get_certificate_status(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        static CRDS: LazyLock<[GroupVersionKind; 3]> = LazyLock::new(|| [
            GroupVersionKind::gvk("cert-manager.io", "v1", "CertificateRequest"),
            GroupVersionKind::gvk("acme.cert-manager.io", "v1", "Order"),
            GroupVersionKind::gvk("acme.cert-manager.io", "v1", "Challenge"),
        ]);

        struct KubeObjectStatus {
            namespace: String,
            name: String,
            status: String,
            reason: String,
            message: String,
        }


        let mut api_calls: FuturesUnordered<_> = CRDS.iter().map(|gvk| {
            let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ApiResource::from_gvk(&gvk));
            async move {
                let kind = gvk.kind.clone();
                match api.list(&ListParams::default()).await {
                    Ok(result) => {
                        // Extract status.conditions to find Ready/Valid condition
                        let ret = result.items.into_iter().map(move |obj| {
                            let (status_str, reason, message) = extract_condition(&obj);
                            KubeObjectStatus {
                                namespace: obj.metadata.namespace.unwrap_or_default(),
                                name: obj.metadata.name.unwrap_or_default(),
                                status: status_str,
                                reason,
                                message,
                            }});
                        Ok((kind, ret))
                    }
                    Err(e) => Err((kind, e))
                }
            }
        }).collect();


        // Render
        let mut output = String::new();
        while let Some(object_status) =api_calls.next().await {
            match object_status {
                Err((kind, e)) => output.push_str(&format!("  Failed to list: {kind} due to {e}\n")),
                Ok((kind, objs)) => {
                    output.push_str(&format!("=== {kind} ===\n"));
                    objs.for_each(|status| {
                        output.push_str(&format!("  {}/{}: status={} reason={} message={}\n", status.namespace, status.name, status.status, status.reason, status.message))
                    })
                }
            }
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Query DNS records for a domain, following the CNAME chain and returning leaf A/AAAA records.
    #[tool(description = "Query DNS for a domain. Returns the full CNAME chain and the leaf A/AAAA records.")]
    async fn dns_lookup(
        &self,
        Parameters(params): Parameters<DnsLookupParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {

        let domain = params.domain.clone();
        let mut output = format!("DNS lookup for: {domain}\n\n");

        // Follow the CNAME chain
        let cname_resolver = stream::unfold((domain, self.dns_resolver.clone()), move |(domain, resolver)| async move {
            let lookup = resolver.lookup(&domain, RecordType::CNAME).await.ok()?;
            let cname = lookup.into_iter().find_map(|rdata| if let RData::CNAME(cname) = rdata { Some(cname) } else { None })?;
            let target_domain = cname.0.to_utf8();
            Some(((domain, target_domain.clone()), (target_domain, resolver)))
        });

        let mut cname_chain: Vec<(String, String)> = cname_resolver.collect().await;
        output.push_str("CNAME chain:\n");
        cname_chain.iter().for_each(|(from, to)| output.push_str(&format!("  {from} -> {to}\n")) );
        if cname_chain.is_empty() {
            output.push_str("none -> direct record\n");
        }
        output.push('\n');

        // Resolve A records on the final target
        let leaf_domain = cname_chain.pop().map(|(_, leaf)| leaf).unwrap_or(params.domain);
        output.push_str(&format!("A records for {leaf_domain}:\n"));
        match self.dns_resolver.ipv4_lookup(leaf_domain.as_str()).await {
            Ok(lookup) => lookup.into_iter().for_each(|a| output.push_str(&format!("  {}\n", a.0)) ),
            Err(e) => output.push_str(&format!("  (none: {e})\n")),
        }

        // Resolve AAAA records on the final target
        output.push_str(&format!("\nAAAA records for {leaf_domain}:\n"));
        match self.dns_resolver.ipv6_lookup(leaf_domain.as_str()).await {
            Ok(lookup) => lookup.into_iter().for_each(|a| output.push_str(&format!("  {}\n", a.0)) ),
            Err(e) => output.push_str(&format!("  (none: {e})\n")),
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Make an HTTP(S) request to a given URL and report success or failure reason.
    #[tool(description = "Make an HTTP GET request to a URL and report whether it succeeds (with status code) or fails (with the error reason). Supports both HTTP and HTTPS.")]
    async fn http_check(
        &self,
        Parameters(params): Parameters<HttpCheckParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let url = &params.url;

        let msg = match self.http_client.get(url.as_str()).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    format!("Success: {url} returned HTTP {status}")
                } else {
                    format!("HTTP error: {url} returned HTTP {status}")
                }
            }
            Err(e) => {
                format!("Request to {url} failed: {:?}", e.source())
            }
        };


        Ok(CallToolResult::success(vec![Content::text(msg)]))
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct DnsLookupParams {
    #[schemars(description = "The domain name to query (e.g. \"example.com\")")]
    domain: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct HttpCheckParams {
    #[schemars(description = "The URL to check (e.g. \"https://example.com\")")]
    url: String,
}

/// Extract the most relevant condition from a cert-manager dynamic object.
/// CertificateRequest uses "Ready", Order uses "Ready", Challenge uses "Ready" as well.
fn extract_condition(obj: &DynamicObject) -> (String, String, String) {
    let unknown = || {
        (
            "Unknown".to_string(),
            "".to_string(),
            "".to_string(),
        )
    };

    let status = match obj.data.get("status") {
        Some(s) => s,
        None => return unknown(),
    };

    let conditions = match status.get("conditions").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => {
            // For Order, the status might just have a "state" field
            if let Some(state) = status.get("state").and_then(|s| s.as_str()) {
                let reason = status
                    .get("reason")
                    .and_then(|r| r.as_str())
                    .unwrap_or("");
                return (state.to_string(), reason.to_string(), "".to_string());
            }
            return unknown();
        }
    };

    // Look for the "Ready" condition first
    for condition in conditions {
        let ctype = condition.get("type").and_then(|t| t.as_str()).unwrap_or("");
        if ctype == "Ready" {
            let cstatus = condition
                .get("status")
                .and_then(|s| s.as_str())
                .unwrap_or("Unknown");
            let reason = condition
                .get("reason")
                .and_then(|r| r.as_str())
                .unwrap_or("");
            let message = condition
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("");
            return (cstatus.to_string(), reason.to_string(), message.to_string());
        }
    }

    // Fallback: return the first condition
    if let Some(condition) = conditions.first() {
        let ctype = condition.get("type").and_then(|t| t.as_str()).unwrap_or("Unknown");
        let cstatus = condition
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("Unknown");
        let reason = condition
            .get("reason")
            .and_then(|r| r.as_str())
            .unwrap_or("");
        let message = condition
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("");
        return (
            format!("{ctype}={cstatus}"),
            reason.to_string(),
            message.to_string(),
        );
    }

    unknown()
}

#[tool_handler]
impl ServerHandler for KubeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "kube-mcp-server".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                title: None,
                description: None,
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Kubernetes MCP server providing tools to fetch cluster events and cert-manager logs."
                    .into(),
            ),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    tracing::info!("Starting kube MCP server");

    let client = Client::try_default().await?;
    let dns_resolver = Resolver::builder_tokio()?.build();
    let http_client = reqwest::Client::builder().tls_backend_rustls()
        .build()?;

    let service = KubeServer::new(client, dns_resolver, http_client).serve(stdio()).await?;
    service.waiting().await?;

    Ok(())
}
