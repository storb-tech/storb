use std::net::{IpAddr, SocketAddr};

use libp2p::{multiaddr::Protocol, Multiaddr};

/// Convert Multiaddr to SocketAddr.
/// The multiaddr format might be something like: `/ip4/127.0.0.1/udp/9000`.
pub fn multiaddr_to_socketaddr(addr: &Multiaddr) -> Option<SocketAddr> {
    let mut iter = addr.iter();

    // Look for IP protocol component
    let ip = match iter.next()? {
        Protocol::Ip4(ip) => IpAddr::V4(ip),
        Protocol::Ip6(ip) => IpAddr::V6(ip),
        _ => return None,
    };

    // Look for TCP protocol component with port
    let port = match iter.next()? {
        Protocol::Tcp(port) => port,
        Protocol::Udp(port) => port,
        _ => return None,
    };

    // Construct and return the SocketAddr
    Some(SocketAddr::new(ip, port))
}

pub fn is_valid_external_addr(addr: &Multiaddr) -> bool {
    let mut components = addr.iter();
    match components.next() {
        Some(Protocol::Ip4(ip)) => !ip.is_loopback() && !ip.is_unspecified() && !ip.is_private(),
        Some(Protocol::Ip6(ip)) => !ip.is_loopback() && !ip.is_unspecified(), // Add more sophisticated IPv6 checks if needed (e.g., ULA)
        Some(Protocol::Dns(_))
        | Some(Protocol::Dns4(_))
        | Some(Protocol::Dns6(_))
        | Some(Protocol::Dnsaddr(_)) => true, // Assume DNS is externally resolvable
        _ => false, // Not an IP or DNS address we can easily validate as external
    }
}
