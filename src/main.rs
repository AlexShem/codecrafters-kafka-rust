mod requests;

use crate::requests::Request;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092")?;

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                if let Err(e) = handle_connection(_stream) {
                    eprintln!("Connection error: {e}")
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_connection(stream: TcpStream) -> anyhow::Result<()> {
    let read_stream = stream.try_clone()?;
    let mut reader = BufReader::new(read_stream);
    let mut writer = BufWriter::new(stream);

    let request = Request::parse_request(&mut reader)?;
    let correlation_id = request.correlation_id;
    let message_size_response: i32 = 0;

    writer.write_all(&message_size_response.to_be_bytes())?;
    writer.write_all(&correlation_id.to_be_bytes())?;
    if request.request_api_version > 4 {
        let unsupported_version: i16 = 35;
        writer.write_all(&unsupported_version.to_be_bytes())?;
    }
    writer.flush()?;

    Ok(())
}
