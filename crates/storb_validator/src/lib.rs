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
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::client::danger::ServerCertVerifier;
use rustls::ClientConfig as RustlsClientConfig;
use rustls::{self};
use std::{error::Error, net::SocketAddr, path::Path, sync::Arc};
use tracing::{error, info};

#[derive(Clone)]
struct ValidatorState {
    miner_addr: SocketAddr,
    server_cert: Vec<u8>,
}

async fn ensure_certificate_exists() -> Result<Vec<u8>> {
    let cert_path = Path::new("cert.der");

    if !cert_path.exists() {
        // Generate new certificate
        let subject_alt_names = vec!["localhost".to_string()];
        let CertifiedKey { cert, key_pair: _ } = generate_simple_self_signed(subject_alt_names)?;

        // Save certificate
        let cert_der = cert.der().to_vec();
        std::fs::write(cert_path, &cert_der)?;

        Ok(cert_der)
    } else {
        // Read existing certificate
        std::fs::read(cert_path).context("Failed to read server certificate")
    }
}

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
    // Load or generate server certificate
    let server_cert = ensure_certificate_exists().await?;

    let state = ValidatorState {
        miner_addr,
        server_cert,
    };

    let app = Router::new()
        .route("/upload", post(upload_file))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024 * 1024)) // 1TiB
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
    let mut _file_name = String::new();

    // Get the first field from multipart
    let field = multipart.next_field().await.map_err(|e| {
        error!("Error reading multipart field: {}", e);
        (StatusCode::BAD_REQUEST, format!("Invalid request: {}", e))
    })?;

    // Check if there's a field
    if let Some(field) = field {
        if let Some(name) = field.file_name() {
            _file_name = name.to_string();
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
            .to_vec();
    } else {
        return Err((StatusCode::BAD_REQUEST, "No file provided".into()));
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

    Ok((StatusCode::OK, format!("File hashes: {}", response)))
}

async fn send_via_quic(addr: &SocketAddr, data: &[u8]) -> Result<String> {
    info!("Creating QUIC client endpoint for address: {}", addr);
    let client = make_client_endpoint(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        0,
    ))
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    info!("Establishing QUIC connection");
    let connection = create_quic_client(&client, *addr).await?;

    info!("Opening bidirectional stream");
    let (mut send_stream, mut rcv_stream) = connection.open_bi().await?;

    const CHUNK_SIZE: usize = 1024 * 64; // 64KB chunks
    info!("Sending file in chunks of {} bytes", CHUNK_SIZE);

    // First send total size as u64
    let total_size = data.len() as u64;
    send_stream.write_all(&total_size.to_be_bytes()).await?;

    // Send data in chunks
    for chunk in data.chunks(CHUNK_SIZE) {
        send_stream.write_all(chunk).await?;
        info!("Sent chunk of {} bytes", chunk.len());
    }

    info!("Finishing send stream");
    send_stream
        .finish()
        .map_err(|e| anyhow::anyhow!("Failed to finish stream: {}", e))?;

    // Read and collect all hashes
    let mut all_hashes = Vec::new();
    let mut current_hash = String::new();
    let mut buf = [0u8; 1];

    while let Ok(Some(n)) = rcv_stream.read(&mut buf).await {
        if n == 0 {
            break;
        }

        if buf[0] == b'\n' {
            if !current_hash.is_empty() {
                all_hashes.push(current_hash.clone());
                current_hash.clear();
            }
        } else {
            current_hash.push(buf[0] as char);
        }
    }

    if !current_hash.is_empty() {
        all_hashes.push(current_hash);
    }

    // Format the response
    let response = all_hashes.join("\n");
    info!("Received {} hashes", all_hashes.len());

    // Close the connection
    connection.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");

    Ok(response)
}

fn configure_client() -> Result<ClientConfig, Box<dyn Error + Send + Sync + 'static>> {
    let mut tls_config = RustlsClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(InsecureCertVerifier));

    let quic_config = QuicClientConfig::try_from(tls_config).unwrap_or_else(|e| {
        panic!("Failed to create QUIC client config: {:?}", e);
    });

    let client_config = ClientConfig::new(Arc::new(quic_config));
    // let client_config: ClientConfig = ClientConfig::with_platform_verifier();
    Ok(client_config)
}

fn make_client_endpoint(
    bind_addr: SocketAddr,
) -> Result<Endpoint, Box<dyn Error + Send + Sync + 'static>> {
    let client_cfg = configure_client()?;

    let mut attempts = 0;
    let max_attempts = 5;

    while attempts < max_attempts {
        match Endpoint::client(bind_addr) {
            Ok(mut endpoint) => {
                endpoint.set_default_client_config(client_cfg.clone());
                return Ok(endpoint);
            }
            Err(_e) if attempts < max_attempts - 1 => {
                attempts += 1;
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err("Failed to create endpoint after maximum attempts".into())
}

async fn create_quic_client(client: &Endpoint, addr: SocketAddr) -> Result<quinn::Connection> {
    info!("Creating QUIC client connection");
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
