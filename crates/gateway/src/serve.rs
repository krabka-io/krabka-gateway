//! Listener serving.
//!
//! The plaintext path accepts HTTP/1.1 and HTTP/2 prior knowledge (h2c) on the
//! same port. The rustls path runs through a manual accept loop that hands each
//! `TlsStream` to hyper and injects the mTLS peer principal, the cert subject
//! DN, into request extensions. TLS material is hot-reloadable through
//! `DynamicServerConfig`.

use std::{sync::Arc, time::Duration};

use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use krabka_security::{AuthMethod, DynamicServerConfig, Principal, TlsConfig};
use krabka_units::prelude::*;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// Serve `app` on `listener`.
///
/// With `tls = Some(..)`, this function terminates rustls for each connection.
/// Otherwise it serves plaintext. On cancellation it stops accepting and
/// drains active requests for up to eight seconds before closing connections.
///
/// # Errors
/// Propagates an I/O error from the listener accept loop.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<DynamicServerConfig>>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    match tls {
        None => serve_plaintext(listener, app, shutdown).await,
        Some(dynamic) => serve_tls(listener, app, dynamic, shutdown).await,
    }
}

async fn serve_plaintext(
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        let (tcp, peer) = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            _ = connections.join_next(), if !connections.is_empty() => continue,
            result = listener.accept() => match result {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(%error, "tcp accept failed");
                    continue;
                }
            },
        };
        let app = app.clone();
        let shutdown = shutdown.clone();
        connections.spawn(async move {
            let service = hyper::service::service_fn(
                move |mut request: hyper::Request<hyper::body::Incoming>| {
                    let app = app.clone();
                    async move {
                        request.extensions_mut().insert(peer);
                        app.oneshot(request).await
                    }
                },
            );
            let builder = auto::Builder::new(TokioExecutor::new());
            let connection = builder.serve_connection(TokioIo::new(tcp), service);
            tokio::pin!(connection);
            let result = tokio::select! {
                result = &mut connection => result,
                () = shutdown.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(error) = result {
                tracing::debug!(%error, "plaintext connection error");
            }
        });
    }
    drop(listener);
    drain_connections(&mut connections).await;
    Ok(())
}

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    dynamic: Arc<DynamicServerConfig>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        let (tcp, peer) = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            _ = connections.join_next(), if !connections.is_empty() => continue,
            res = listener.accept() => match res {
                Ok(v) => v,
                Err(e) => { tracing::warn!(error = %e, "tcp accept failed"); continue; }
            },
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(dynamic.current());
        let app = app.clone();
        let shutdown = shutdown.clone();
        connections.spawn(async move {
            let handshake = tokio::select! {
                () = shutdown.cancelled() => return,
                result = acceptor.accept(tcp) => result,
            };
            let tls = match handshake {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(error = %e, %peer, "tls handshake failed");
                    return;
                }
            };
            let principal = peer_principal(&tls);
            let io = TokioIo::new(tls);
            let svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    let app = app.clone();
                    let principal = principal.clone();
                    async move {
                        // Always inject the peer address so authz / audit handlers
                        // can do host-based ACL matching.  `peer_or_default` in
                        // `authz::auth_layer` returns `0.0.0.0:0` for plaintext
                        // connections that don't go through this TLS path.
                        req.extensions_mut().insert(peer);
                        if let Some(p) = principal {
                            req.extensions_mut().insert(p);
                        }
                        app.oneshot(req).await
                    }
                },
            );
            // HTTP/1.1 only — matches the gateway's Connect-over-h1 design (axum
            // is built with the `http1` feature; the plaintext `axum::serve`
            // path is h1 too). Connect unary + streaming work over h1; a future
            // h2/gRPC-over-TLS client would need `auto::Builder` + ALPN here.
            let connection = hyper::server::conn::http1::Builder::new().serve_connection(io, svc);
            tokio::pin!(connection);
            let result = tokio::select! {
                result = &mut connection => result,
                () = shutdown.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(e) = result {
                tracing::debug!(error = %e, "tls connection error");
            }
        });
    }
    drop(listener);
    drain_connections(&mut connections).await;
    Ok(())
}

async fn drain_connections(connections: &mut JoinSet<()>) {
    if tokio::time::timeout(Duration::from_secs(8), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tracing::warn!("connection drain deadline reached; closing remaining connections");
        connections.shutdown().await;
    }
}

/// Extract the mTLS peer principal, the cert subject DN, after a handshake.
fn peer_principal(tls: &tokio_rustls::server::TlsStream<TcpStream>) -> Option<Principal> {
    let (_, conn) = tls.get_ref();
    let cert = conn.peer_certificates()?.first()?;
    let name = krabka_security::extract_principal_from_cert(cert.as_ref())?;
    Some(Principal {
        name,
        auth_method: AuthMethod::MTls,
        groups: vec![],
    })
}

/// Build the hot-reloadable server config and spawn the reload watcher. Returns
/// the dynamic config to pass to [`serve`].
///
/// # Errors
/// Propagates `krabka_security::TlsError` if the initial config fails to build.
///
/// # Panics
///
/// Panics when `reload_interval` is not positive. Process configuration
/// validates this invariant before building the listener.
pub fn build_and_watch_tls(
    cfg: TlsConfig,
    reload_interval: Time,
    shutdown: CancellationToken,
) -> Result<Arc<DynamicServerConfig>, krabka_security::TlsError> {
    let tick = reload_tick(reload_interval);
    let dynamic = DynamicServerConfig::from_tls_config(&cfg)?;
    let watch = dynamic.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick);
        ticker.tick().await; // skip the immediate first tick (already loaded)
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = shutdown.cancelled() => return,
            }
            if let Err(e) = watch.reload_from(&cfg) {
                tracing::warn!(error = %e, "tls reload failed; keeping prior config");
            }
        }
    });
    Ok(dynamic)
}

/// The `tokio` tick for a validated reload interval. `tokio::time::interval`
/// panics on a zero period, so this function checks the extent first.
fn reload_tick(reload_interval: Time) -> Duration {
    assert2::assert!(reload_interval > secs(0));
    reload_interval.to_std()
}

#[cfg(test)]
mod policy_tests {
    use assert2::{assert, check};
    use krabka_units::prelude::*;

    use super::reload_tick;

    #[test]
    fn reload_tick_rejects_a_non_positive_interval() {
        for interval in [secs(0), Time::from_secs(-1)] {
            assert!(std::panic::catch_unwind(|| reload_tick(interval)).is_err());
        }
    }

    #[test]
    fn reload_tick_hands_tokio_the_configured_extent() {
        check!(reload_tick(secs(30)) == std::time::Duration::from_secs(30));
        check!(reload_tick(millis(250)) == std::time::Duration::from_millis(250));
    }

    #[tokio::test]
    async fn shutdown_drains_an_active_request() {
        use axum::{Router, routing::get};
        use tokio_util::sync::CancellationToken;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let started = CancellationToken::new();
        let release = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let app = Router::new().route(
            "/",
            get({
                let started = started.clone();
                let release = release.clone();
                move || async move {
                    started.cancel();
                    release.cancelled().await;
                    "accepted"
                }
            }),
        );
        let mut server = tokio::spawn(super::serve(listener, app, None, shutdown.clone()));
        let request = tokio::spawn(async move {
            reqwest::get(format!("http://{addr}/"))
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), started.cancelled())
            .await
            .unwrap();
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut server)
                .await
                .is_err()
        );
        release.cancel();
        check!(request.await.unwrap() == "accepted");
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_aborts_a_connection_after_the_drain_deadline() {
        let mut connections = tokio::task::JoinSet::new();
        connections.spawn(std::future::pending::<()>());
        tokio::time::timeout(
            std::time::Duration::from_secs(9),
            super::drain_connections(&mut connections),
        )
        .await
        .unwrap();
        check!(connections.is_empty());
    }
}
