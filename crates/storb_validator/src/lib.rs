use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Endpoint};
use rustls::client::danger::ServerCertVerifier;
use rustls::ClientConfig as RustlsClientConfig;
use rustls::{self};
use std::{error::Error, net::SocketAddr, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info};

#[derive(Clone)]
struct ValidatorState {
    miner_addr: SocketAddr,
    server_cert: Vec<u8>,
}

// struct SkipServerVerification;

// impl SkipServerVerification {
//     fn new() -> Arc<Self> {
//         Arc::new(Self)
//     }
// }

// impl std::fmt::Debug for SkipServerVerification {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "SkipServerVerification")
//     }
// }

// impl ServerCertVerifier for SkipServerVerification {
//     fn verify_server_cert(
//         &self,
//         _end_entity: &rustls::pki_types::CertificateDer<'_>,
//         _intermediates: &[rustls::pki_types::CertificateDer<'_>],
//         _server_name: &rustls::pki_types::ServerName<'_>,
//         _ocsp_response: &[u8],
//         _now: rustls::pki_types::UnixTime,
//     ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
//         Ok(rustls::client::danger::ServerCertVerified::assertion())
//     }

//     fn verify_tls12_signature(
//         &self,
//         _message: &[u8],
//         _cert: &rustls::pki_types::CertificateDer<'_>,
//         _dss: &rustls::DigitallySignedStruct,
//     ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
//         Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
//     }

//     fn verify_tls13_signature(
//         &self,
//         _message: &[u8],
//         _cert: &rustls::pki_types::CertificateDer<'_>,
//         _dss: &rustls::DigitallySignedStruct,
//     ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
//         Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
//     }

//     fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
//         vec![]
//     }
// }

#[derive(Debug)]
struct InsecureCertVerifier;

impl ServerCertVerifier for InsecureCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Always trust the server certificate.
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
        ]
    }
}

pub async fn run_validator(http_port: u16, miner_addr: SocketAddr) -> Result<()> {
    // Load server certificate
    let server_cert = std::fs::read("cert.der").context("Failed to read server certificate")?;
    let mut roots = rustls::RootCertStore::empty();

    let state = ValidatorState {
        miner_addr,
        server_cert,
    };

    let app = Router::new()
        .route("/upload", post(upload_file))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)) // 1GB
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], http_port));
    info!("Validator HTTP server listening on {}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr)
            .await
            .context("Failed to bind HTTP server")?,
        app,
    )
    .await
    .context("HTTP server failed")?;

    Ok(())
}

async fn upload_file(
    state: axum::extract::State<ValidatorState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut file_data = Vec::new();
    let mut file_name = String::new();

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        error!("Error reading multipart field: {}", e);
        (StatusCode::BAD_REQUEST, format!("Invalid request: {}", e))
    })? {
        if let Some(name) = field.file_name() {
            file_name = name.to_string();
        }
        file_data = field
            .bytes()
            .await
            .map_err(|e| {
                error!("Error reading file data: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read file".into(),
                )
            })?
            .to_vec(); // Convert Bytes to Vec<u8>
        break; // Assuming single file upload
    }

    // Send data to miner via QUIC
    let response = send_via_quic(&state.miner_addr, &file_data)
        .await
        .map_err(|e| {
            error!("QUIC transfer failed: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                format!("Failed to store file: {}", e),
            )
        })?;

    Ok((StatusCode::OK, format!("File stored: {}", response)))
}

async fn send_via_quic(addr: &SocketAddr, data: &[u8]) -> Result<String> {
    let client = make_client_endpoint(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        0,
    ))
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let connection = create_quic_client(&client, *addr).await?;

    let (mut send_stream, mut rcv_stream) = connection.open_bi().await?;

    send_stream.write_all(data).await?;
    send_stream.finish();

    let mut response = String::new();
    rcv_stream.read_to_string(&mut response).await?;

    Ok(response)
}
fn configure_client() -> Result<ClientConfig, Box<dyn Error + Send + Sync + 'static>> {
    // let mut client_crypto = rustls::ClientConfig::builder()
    //     // .with_custom_certificate_verifier(SkipServerVerification::new())
    //     .with_no_client_auth();

    let mut tls_config = RustlsClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    // client_crypto.alpn_protocols = vec![b"hq-interop".to_vec()];
    // For local development or testing only: skip server cert verification
    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(InsecureCertVerifier));

    let quic_config = QuicClientConfig::try_from(tls_config).unwrap_or_else(|e| {
        panic!("Failed to create QUIC client config: {:?}", e);
    });
    let client_config = ClientConfig::new(Arc::new(quic_config));
    Ok(client_config)
}

fn make_client_endpoint(
    bind_addr: SocketAddr,
) -> Result<Endpoint, Box<dyn Error + Send + Sync + 'static>> {
    let client_cfg = configure_client()?;
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_cfg);
    Ok(endpoint)
}

async fn create_quic_client(client: &Endpoint, addr: SocketAddr) -> Result<quinn::Connection> {
    info!("Creating QUIC client endpoint");
    // Bind to some local UDP address/port (0.0.0.0:0 picks a random free port).
    let mut endpoint = Endpoint::client(addr)?;

    // Build the base rustls config.
    let mut tls_config = RustlsClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    // For local development or testing only: skip server cert verification
    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(InsecureCertVerifier));

    let quic_config = QuicClientConfig::try_from(tls_config).unwrap_or_else(|e| {
        panic!("Failed to create QUIC client config: {:?}", e);
    });
    let client_config = ClientConfig::new(Arc::new(quic_config));

    endpoint.set_default_client_config(client_config);

    let connection = client.connect(addr, "localhost")?.await?;

    Ok(connection)
}

async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    run_validator(3000, "127.0.0.1:5000".parse().unwrap())
        .await
        .unwrap()
}

pub fn run() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main())
}

// fn main() {
//     let mut client_crypto = ClientConfig::builder()
//         .with_safe_defaults()
//         .with_custom_certificate_verifier(SkipServerVerification::new())
//         .with_no_client_auth();

//     client_crypto.alpn_protocols = vec![b"hq-interop".to_vec()];

//     let client_config = QuicClientConfig::new(Arc::new(client_crypto));
//     let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap()).unwrap();
//     endpoint.set_default_client_config(client_config);

//     // Continue with the client setup
// }
