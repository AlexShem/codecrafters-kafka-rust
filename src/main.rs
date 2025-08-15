mod api_keys;
mod requests;
mod responses;

use crate::requests::Request;
use crate::responses::Response;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092")?;

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                thread::spawn(move || {
                    if let Err(e) = handle_connection(_stream) {
                        eprintln!("Connection error: {e}")
                    }
                });
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

    loop {
        match Request::parse_request(&mut reader) {
            Ok(request) => {
                let response = Response::generate_response(request);
                writer.write_all(&response)?;
                writer.flush()?;
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
}
