use crate::util::{hexdump, read_n, read_or_eof};
use std::io::BufReader;
use std::net::{Ipv4Addr, TcpStream};

pub const PCAP_MAGIC: u32 = 0xa1b2c3d4;
pub const PCAP_MAGIC_NANO: u32 = 0xa1b23c4d;

/// The DB server endpoint to match against. Determines packet direction.
pub enum DbServer {
    /// Port-only: matches any IP with this port.
    Port(u16),
    /// IP:port: matches exactly this address and port.
    AddrPort(Ipv4Addr, u16),
}

impl DbServer {
    pub fn parse(s: &str) -> Result<Self, String> {
        if let Some((ip_str, port_str)) = s.rsplit_once(':') {
            let ip: Ipv4Addr = ip_str.parse().map_err(|e| format!("bad IP in --db_server '{ip_str}': {e}"))?;
            let port: u16 = port_str.parse().map_err(|e| format!("bad port in --db_server '{port_str}': {e}"))?;
            Ok(Self::AddrPort(ip, port))
        } else {
            let port: u16 = s.parse().map_err(|e| format!("bad port in --db_server '{s}': {e}"))?;
            Ok(Self::Port(port))
        }
    }

    pub fn is_incoming(&self, pkt: &Parsed) -> bool {
        match self {
            Self::Port(p) => pkt.dst_port == *p,
            Self::AddrPort(ip, p) => pkt.dst_port == *p && pkt.dst_ip == *ip,
        }
    }

    pub fn is_outgoing(&self, pkt: &Parsed) -> bool {
        match self {
            Self::Port(p) => pkt.src_port == *p,
            Self::AddrPort(ip, p) => pkt.src_port == *p && pkt.src_ip == *ip,
        }
    }
}

impl std::fmt::Display for DbServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Port(p) => write!(f, ":{p}"),
            Self::AddrPort(ip, p) => write!(f, "{ip}:{p}"),
        }
    }
}

/// Packet direction relative to the DB server.
#[derive(PartialEq, Clone, Copy)]
pub enum Direction {
    Incoming,
    Outgoing,
}

/// One request/response exchange.
pub struct Exchange {
    pub incoming: Vec<u8>,
    pub outgoing: Vec<u8>,
}

/// PCAP field byte-order helper.
struct Endian(bool);

impl Endian {
    fn u32(&self, b: &[u8]) -> u32 {
        let a = [b[0], b[1], b[2], b[3]];
        if self.0 { u32::from_be_bytes(a) } else { u32::from_le_bytes(a) }
    }
}

pub struct Parsed<'a> {
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
    pub payload: &'a [u8],
}

/// Parse a PCAP stream into exchanges. Closes the stream when done.
pub fn parse_pcap(stream: TcpStream, db_server: &DbServer, verbose: bool) -> Result<Vec<Exchange>, Box<dyn std::error::Error>> {
    let mut r = BufReader::new(stream);

    let ghdr = read_n(&mut r, 24)?;
    let raw: [u8; 4] = [ghdr[0], ghdr[1], ghdr[2], ghdr[3]];
    let magic_le = u32::from_le_bytes(raw);
    let magic_be = u32::from_be_bytes(raw);

    let big_endian = if magic_le == PCAP_MAGIC || magic_le == PCAP_MAGIC_NANO {
        false
    } else if magic_be == PCAP_MAGIC || magic_be == PCAP_MAGIC_NANO {
        true
    } else {
        return Err(format!("not a pcap stream (magic: {raw:02x?})").into());
    };

    let pcap = Endian(big_endian);
    let link_type = pcap.u32(&ghdr[20..]);

    let link_hdr_len: usize = match link_type {
        0 => 4,    // DLT_NULL (loopback)
        1 => 14,   // DLT_EN10MB (Ethernet)
        113 => 16, // DLT_LINUX_SLL (cooked v1)
        276 => 20, // DLT_LINUX_SLL2 (cooked v2)
        _ => return Err(format!("unsupported link type: {link_type}").into()),
    };

    eprintln!("link_type={link_type}");

    let mut exchanges: Vec<Exchange> = Vec::new();
    let mut cur_dir: Option<Direction> = None;
    let mut cur_incoming: Vec<u8> = Vec::new();
    let mut cur_outgoing: Vec<u8> = Vec::new();
    let mut n: u64 = 0;
    let mut n_empty: u64 = 0;
    let mut n_incoming: u64 = 0;
    let mut n_outgoing: u64 = 0;
    let mut n_unmatched: u64 = 0;
    let mut n_unparsed: u64 = 0;

    while let Some(phdr) = read_or_eof(&mut r, 16)? {
        let incl_len = pcap.u32(&phdr[8..]) as usize;
        let pkt = read_n(&mut r, incl_len)?;

        if let Some(parsed) = parse_packet(link_type, link_hdr_len, &pkt) {
            if parsed.payload.is_empty() {
                n_empty += 1;
                n += 1;
                continue;
            }

            let dir = if db_server.is_incoming(&parsed) {
                n_incoming += 1;
                Direction::Incoming
            } else if db_server.is_outgoing(&parsed) {
                n_outgoing += 1;
                Direction::Outgoing
            } else {
                n_unmatched += 1;
                if verbose {
                    eprintln!(
                        "  packet {n}: unmatched {}:{} -> {}:{} ({} bytes)",
                        parsed.src_ip,
                        parsed.src_port,
                        parsed.dst_ip,
                        parsed.dst_port,
                        parsed.payload.len()
                    );
                }
                n += 1;
                continue;
            };

            if verbose {
                let label = match dir {
                    Direction::Incoming => ">>incoming",
                    Direction::Outgoing => "<<outgoing",
                };
                println!("--- {label} packet {n} ({} bytes) ---", parsed.payload.len());
                hexdump(parsed.payload);
            }

            if dir == Direction::Incoming && cur_dir == Some(Direction::Outgoing) {
                exchanges.push(Exchange {
                    incoming: std::mem::take(&mut cur_incoming),
                    outgoing: std::mem::take(&mut cur_outgoing),
                });
            }

            match dir {
                Direction::Incoming => cur_incoming.extend_from_slice(parsed.payload),
                Direction::Outgoing => cur_outgoing.extend_from_slice(parsed.payload),
            }
            cur_dir = Some(dir);
        } else {
            n_unparsed += 1;
        }
        n += 1;
    }

    if !cur_incoming.is_empty() || !cur_outgoing.is_empty() {
        exchanges.push(Exchange { incoming: cur_incoming, outgoing: cur_outgoing });
    }

    drop(r);

    eprintln!(
        "{n} packets: {n_incoming} incoming, {n_outgoing} outgoing, \
         {n_empty} empty, {n_unmatched} unmatched, {n_unparsed} unparsed"
    );
    eprintln!("{} exchanges", exchanges.len());

    if exchanges.is_empty() && n_unmatched > 0 {
        eprintln!(
            "hint: {n_unmatched} packets had data but did not match --db_server {db_server}. \
             Check the port/address."
        );
    }

    Ok(exchanges)
}

/// Parse link-layer, IP, and transport headers; return IPs, ports, and payload.
fn parse_packet<'a>(link_type: u32, link_hdr_len: usize, pkt: &'a [u8]) -> Option<Parsed<'a>> {
    if pkt.len() < link_hdr_len {
        return None;
    }

    let is_ipv4 = match link_type {
        0 => {
            if pkt.len() < 4 {
                return None;
            }
            u32::from_ne_bytes([pkt[0], pkt[1], pkt[2], pkt[3]]) == 2
        }
        1 => {
            if pkt.len() < 14 {
                return None;
            }
            u16::from_be_bytes([pkt[12], pkt[13]]) == 0x0800
        }
        113 => {
            if pkt.len() < 16 {
                return None;
            }
            u16::from_be_bytes([pkt[14], pkt[15]]) == 0x0800
        }
        276 => {
            if pkt.len() < 20 {
                return None;
            }
            u16::from_be_bytes([pkt[0], pkt[1]]) == 0x0800
        }
        _ => return None,
    };

    if !is_ipv4 {
        return None;
    }

    let ip = &pkt[link_hdr_len..];
    if ip.len() < 20 {
        return None;
    }

    let ihl = (ip[0] & 0x0f) as usize * 4;
    if ihl < 20 || ip.len() < ihl {
        return None;
    }

    let src_ip = Ipv4Addr::new(ip[12], ip[13], ip[14], ip[15]);
    let dst_ip = Ipv4Addr::new(ip[16], ip[17], ip[18], ip[19]);

    let protocol = ip[9];
    let transport = &ip[ihl..];

    if transport.len() < 4 {
        return None;
    }
    let src_port = u16::from_be_bytes([transport[0], transport[1]]);
    let dst_port = u16::from_be_bytes([transport[2], transport[3]]);

    let hdr_len = match protocol {
        6 => {
            if transport.len() < 13 {
                return None;
            }
            ((transport[12] >> 4) as usize) * 4
        }
        17 => 8,
        _ => return None,
    };

    if transport.len() < hdr_len {
        return None;
    }

    Some(Parsed {
        src_ip,
        dst_ip,
        src_port,
        dst_port,
        payload: &transport[hdr_len..],
    })
}
