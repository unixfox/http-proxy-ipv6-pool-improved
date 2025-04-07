use hyper::{
    client::HttpConnector,
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
    Body, Client, Method, Request, Response, Server,
};
use rand::Rng;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpSocket,
};

pub async fn start_proxy(
    listen_addr: SocketAddr,
    (ipv6, prefix_len): (Ipv6Addr, u8),
) -> Result<(), Box<dyn std::error::Error>> {
    let make_service = make_service_fn(move |_: &AddrStream| async move {
        Ok::<_, hyper::Error>(service_fn(move |req| {
            Proxy {
                ipv6: ipv6.into(),
                prefix_len,
            }
            .proxy(req)
        }))
    });

    Server::bind(&listen_addr)
        .http1_preserve_header_case(true)
        .http1_title_case_headers(true)
        .serve(make_service)
        .await
        .map_err(|err| err.into())
}

#[derive(Clone)]
pub(crate) struct Proxy {
    pub ipv6: u128,
    pub prefix_len: u8,
}

impl Proxy {
    pub(crate) async fn proxy(self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        match if req.method() == Method::CONNECT {
            self.process_connect(req).await
        } else {
            self.process_request(req).await
        } {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    async fn process_connect(self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        tokio::task::spawn(async move {
            let remote_addr = req.uri().authority().map(|auth| auth.to_string()).unwrap();
            let mut upgraded = hyper::upgrade::on(req).await.unwrap();
            self.tunnel(&mut upgraded, remote_addr).await
        });
        Ok(Response::new(Body::empty()))
    }

    async fn process_request(self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let bind_addr = get_rand_ipv6(self.ipv6, self.prefix_len);
        let mut http = HttpConnector::new();
        http.set_local_address(Some(bind_addr));
        println!("{} via {bind_addr}", req.uri().host().unwrap_or_default());

        let client = Client::builder()
            .http1_title_case_headers(true)
            .http1_preserve_header_case(true)
            .build(http);
        let res = client.request(req).await?;
        Ok(res)
    }

    async fn tunnel<A>(self, upgraded: &mut A, addr_str: String) -> std::io::Result<()>
    where
        A: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        match addr_str.to_socket_addrs() {
            Ok(addrs) => {
                let addrs: Vec<_> = addrs.collect();
                for addr in addrs {
                    let socket = match TcpSocket::new_v6() {
                        Ok(s) => s,
                        Err(e) => {
                            println!("Failed to create IPv6 socket for {addr_str}: {e}");
                            continue;
                        }
                    };

                    let bind_addr = get_rand_ipv6_socket_addr(self.ipv6, self.prefix_len);
                    
                    match socket.bind(bind_addr) {
                        Ok(()) => {
                            println!("{addr_str} via {bind_addr}");
                            
                            match socket.connect(addr).await {
                                Ok(server) => {
                                    let (mut client_read, mut client_write) = tokio::io::split(&mut *upgraded);
                                    let (mut server_read, mut server_write) = tokio::io::split(server);

                                    let client_to_server = tokio::io::copy(&mut client_read, &mut server_write);
                                    let server_to_client = tokio::io::copy(&mut server_read, &mut client_write);

                                    match tokio::join!(client_to_server, server_to_client) {
                                        (Ok(_), Ok(_)) => return Ok(()),
                                        (Err(e), _) | (_, Err(e)) => {
                                            if e.kind() == std::io::ErrorKind::ConnectionReset {
                                                println!("Connection reset for {addr_str}, trying next address");
                                                continue;
                                            }
                                            println!("Connection error for {addr_str}: {e}");
                                        }
                                    }
                                }
                                Err(e) => {
                                    if e.kind() == std::io::ErrorKind::InvalidInput {
                                        println!("Invalid IPv6 address for {addr_str}, trying next address");
                                        continue;
                                    }
                                    println!("Failed to connect to {addr_str}: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            println!("Failed to bind to {bind_addr} for {addr_str}: {e}");
                            continue;
                        }
                    }
                }
                
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to establish any connection after trying all addresses",
                ))
            }
            Err(e) => {
                println!("DNS resolution error for {addr_str}: {e}");
                Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            }
        }
    }
}

fn get_rand_ipv6_socket_addr(ipv6: u128, prefix_len: u8) -> SocketAddr {
    let mut rng = rand::thread_rng();
    SocketAddr::new(get_rand_ipv6(ipv6, prefix_len), rng.gen::<u16>())
}

fn get_rand_ipv6(mut ipv6: u128, prefix_len: u8) -> IpAddr {
    let rand: u128 = rand::thread_rng().gen();
    let net_part = (ipv6 >> (128 - prefix_len)) << (128 - prefix_len);
    let host_part = (rand << prefix_len) >> prefix_len;
    ipv6 = net_part | host_part;
    IpAddr::V6(ipv6.into())
}
