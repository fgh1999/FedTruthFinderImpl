tonic::include_proto!("grpc.examples.echo");
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status};

type EchoResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send + Sync>>;

#[derive(Default)]
pub struct EchoServer;

#[tonic::async_trait]
impl echo_server::Echo for EchoServer {
    async fn unary_echo(&self, request: Request<EchoRequest>) -> EchoResult<EchoResponse> {
        if let Some(certs) = request.peer_certs() {
            println!("Got {} peer certs!", certs.len());
        }

        let message = request.into_inner().message;
        Ok(Response::new(EchoResponse { message }))
    }

    type ServerStreamingEchoStream = ResponseStream;

    async fn server_streaming_echo(
        &self,
        _: Request<EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn client_streaming_echo(
        &self,
        _: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<EchoResponse>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type BidirectionalStreamingEchoStream = ResponseStream;

    async fn bidirectional_streaming_echo(
        &self,
        _: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::BidirectionalStreamingEchoStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

use super::config;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, ServerTlsConfig};

#[allow(dead_code)]
pub async fn init_client(
    config_path: &str,
) -> Result<(std::net::SocketAddr, Channel), Box<dyn std::error::Error>> {
    let configuration = config::load_client_config(config_path);

    // for security
    let server_root_ca_cert = tokio::fs::read(configuration.keys.ca_path).await?;
    let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);

    let client_cert = tokio::fs::read(configuration.keys.pk_path).await?;
    let client_key = tokio::fs::read(configuration.keys.sk_path).await?;
    let client_identity = Identity::from_pem(client_cert, client_key);

    // init
    let tls = ClientTlsConfig::new()
        .domain_name(configuration.addrs.domain_name)
        .ca_certificate(server_root_ca_cert)
        .identity(client_identity);

    let channel = Channel::from_static(Box::leak(configuration.addrs.remote_addr.into_boxed_str()))
        .tls_config(tls)?
        .connect()
        .await?;
    let mailbox_addr = configuration.addrs.mailbox_addr.parse()?;

    Ok((mailbox_addr, channel))
}

///Init the server with a given config file
#[allow(dead_code)]
pub async fn init_server(
    config_path: &str,
) -> Result<(std::net::SocketAddr, ServerTlsConfig), Box<dyn std::error::Error>> {
    let configuration = config::load_server_config(config_path);
    let cert = tokio::fs::read(configuration.keys.pk_path).await?;
    let key = tokio::fs::read(configuration.keys.sk_path).await?;
    let server_identity = Identity::from_pem(cert, key);
    let client_ca_cert = tokio::fs::read(configuration.keys.ca_path).await?;
    let client_ca_cert = Certificate::from_pem(client_ca_cert);

    let addr = configuration.addr.parse().unwrap();
    let tls = ServerTlsConfig::new()
        .identity(server_identity)
        .client_ca_root(client_ca_cert);
    Ok((addr, tls))
}

///Construct a echo service instance
#[allow(dead_code)]
pub fn new_echo_servive() -> echo_server::EchoServer<EchoServer> {
    let server_instance = EchoServer::default();
    echo_server::EchoServer::new(server_instance)
}
