#![allow(unused_imports)]

use std::io::{BufWriter, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                handle_connection(_stream).expect("Failed to handle connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(stream: TcpStream) -> anyhow::Result<()> {
    let mut writer = BufWriter::new(stream);

    let message_size: i32 = 0;
    let correlation_id: i32 = 7;

    writer.write_all(message_size.to_be_bytes().as_slice())?;
    writer.write_all(correlation_id.to_be_bytes().as_slice())?;

    Ok(())
}
