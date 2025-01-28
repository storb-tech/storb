use anyhow::Result;
use storb_validator::ValidatorServiceClient;
use tarpc::serde_transport::tcp;
use tarpc::{client, context};
use tokio_serde::formats::Json; // The generated client from your `#[tarpc::service]`

use capnp::{message::ReaderOptions, message::TypedBuilder, serialize};
use storb_schemas::schemas::message_capnp;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:5000"; // The same port your server is listening on
    let transport = tcp::connect(addr, Json::default).await?;
    let client = ValidatorServiceClient::new(client::Config::default(), transport).spawn();

    // Build a HelloRequest
    let mut request_builder = TypedBuilder::<message_capnp::hello_request::Owned>::new_default();
    request_builder.init_root().set_name("Manual Test user");
    let mut request_bytes = Vec::new();
    let message = request_builder.into_inner();
    serialize::write_message(&mut request_bytes, &message)?;

    // Send it
    let response_bytes = client.validate(context::current(), request_bytes).await?;

    // Read the response
    let response_reader =
        serialize::read_message(&mut &response_bytes[..], ReaderOptions::default())?;
    let response = response_reader.get_root::<message_capnp::hello_response::Reader>()?;
    let reply_text = response.get_reply()?;

    println!("Got server reply: {:?}", reply_text);

    Ok(())
}
