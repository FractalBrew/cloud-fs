//! Networking specific types.

use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Host {
    Name(String),
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Host::Name(addr) => addr.fmt(f),
            Host::Ipv4(addr) => addr.fmt(f),
            Host::Ipv6(addr) => addr.fmt(f),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Address {
    pub host: Host,
    pub port: Option<u16>,
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.host.fmt(f)?;

        if let Some(p) = self.port {
            write!(f, ":{}", p)
        } else {
            Ok(())
        }
    }
}

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Address {
        match addr {
            SocketAddr::V4(addr4) => Address {
                host: Host::Ipv4(addr4.ip().to_owned()),
                port: Some(addr4.port()),
            },
            SocketAddr::V6(addr6) => Address {
                host: Host::Ipv6(addr6.ip().to_owned()),
                port: Some(addr6.port()),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Auth {
    pub username: String,
    pub password: String,
}
