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
