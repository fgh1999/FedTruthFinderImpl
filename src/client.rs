use fed_truth_finder::echo::{self, echo_client::EchoClient, EchoRequest};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mailbox_addr, server_channel) = echo::init_client("config/client1.json").await?;
    let server = tokio::spawn(async move {
        println!("server running...");
        Server::builder()
            .add_service(echo::new_echo_servive())
            .serve(mailbox_addr)
            .await
            .unwrap();
    });

    let mut client = EchoClient::new(server_channel);

    // main part
    let request = tonic::Request::new(EchoRequest {
        message: "hello".into(),
    });

    let response = client.unary_echo(request).await?;

    println!("RESPONSE={:?}", response);
    server.await?;
    Ok(())
}
