mod request;
mod response;

use clap::Parser;
use rand::{Rng, SeedableRng};
use request::write_to_stream;
use response::read_from_stream;
use std::collections::HashSet;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time;

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[clap(about = "Fun with load balancing")]
struct CmdOptions {
    #[clap(
        short,
        long,
        about = "IP/port to bind to",
        default_value = "0.0.0.0:1100"
    )]
    bind: String,
    #[clap(short, long, about = "Upstream host to forward requests to")]
    upstream: Vec<String>,
    #[clap(
        long,
        about = "Perform active health checks on this interval (in seconds)",
        // default_value = "10"
        default_value = "1"
    )]
    active_health_check_interval: usize,
    #[clap(
        long,
        about = "Path to send request to for active health checks",
        default_value = "/"
    )]
    active_health_check_path: String,
    #[clap(
        long,
        about = "Maximum number of requests to accept per IP per minute (0 = unlimited)",
        default_value = "0"
    )]
    max_requests_per_minute: usize,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
#[derive(Clone)]
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    #[allow(dead_code)]
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
    /// Indexes of dead servers.
    dead_indexes: Vec<usize>,
}

// balancebeam::setup will executes the binary compiled from this file to
// start up a balancebeam instance.
// the test suites will also start up several echo server instances which act as
// the upstream/application servers.
// the test suites than send mock client requests to the balancebeam and let it
// forwards the requests to one of the echo servers.
// when the balancebeam receives reponses from an echo server, it forwards them
// back to the client, i.e. one of the test suites.
fn main() {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let listener = match TcpListener::bind(&options.bind) {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections
    let mut state = ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        dead_indexes: Vec::new(),
    };

    // create a channel for message passing between the main thread and the active health check thread.
    let (tx, rx) = mpsc::channel();
    // we want the thread not interfer with the main thread, so we clone the state and
    // move it into this thread.
    let state_clone = state.to_owned();
    // spawn a thread doing active health check periodically.
    thread::spawn(move || active_health_check(state_clone, tx));

    for stream in listener.incoming() {
        // try_recv will return Err if the channel is closed or there's no
        // pending values in the channel buffer.
        let mut exit = false;
        match rx.try_recv() {
            Ok(dead_indexes) => {
                log::debug!("recv dead_indexes = {:?}", dead_indexes);
                state.dead_indexes = dead_indexes;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                log::info!("channel closed");
                exit = true;
            }
        }
        if exit {
            break;
        }

        if let Ok(stream) = stream {
            // Handle the connection!
            handle_connection(stream, &state);
        }
    }
}

fn active_health_check(state: ProxyState, tx: mpsc::Sender<Vec<usize>>) {
    log::info!("start active health check thread");

    let check_interval = time::Duration::from_secs(state.active_health_check_interval as u64);
    let mut last_check_time = time::Instant::now();

    // this thread runs in an infinite loop and exits when the main thread exits.
    loop {
        // do active health check by sending a request to the active health check path of each
        // application server. If it responds with 200 status, the application server is alive.
        // Otherwise, we assume it's dead.

        // the indexes of the dead upstream servers in the state.upstream_addresses array.
        let mut dead_indexes = Vec::new();

        for (i, ip) in state.upstream_addresses.iter().enumerate() {
            if let Ok(mut stream) = TcpStream::connect(ip) {
                // construct an active health check request.
                // @note HTTP HEAD vs. GET methods.
                // https://reqbin.com/Article/HttpHead
                let request = http::Request::builder()
                    .method("HEAD")
                    .uri(state.active_health_check_path.as_str())
                    .header("Host", ip.as_str())
                    .body(Vec::new())
                    .expect("failed to build an active health check request");

                // send the active health check request to the upstream server through the stream.
                if let Err(err) = write_to_stream(&request, &mut stream) {
                    log::error!("encounters error {} when writing to stream connected with upstream server {}", err.to_string(), i);
                    dead_indexes.push(i);
                    continue;
                }

                // read the response of the upstream server from the stream.
                // read_from_stream will block until a valid response is read completely.
                match read_from_stream(&mut stream, &http::Method::HEAD) {
                    Ok(response) => {
                        // if the server responds with a status code which not the expected 200,
                        // we assume the application server is currently unable to serve the
                        // requests. So we mark it as dead.
                        if response.status().as_u16() != 200 {
                            dead_indexes.push(i);
                        }
                        // although the test suite says that it replaces a good server with
                        // a server only returns status 500, it seems we cannot successfully
                        // establish a connection with this new server and hence the only possible
                        // status code we receive at here is 200, aka. OK.
                        log::debug!(
                            "upstream server {} returns status {}",
                            ip,
                            response.status().as_u16()
                        );
                    }
                    Err(_) => {
                        log::error!("encounters error when writing to stream connected with upstream server {}", i);
                        dead_indexes.push(i);
                    }
                }
                // the stream is closed when leaving this scope.
            } else {
                log::error!(
                    "unable to establish a connection with the upstream server {}",
                    i
                );
                dead_indexes.push(i);
            }
        }

        // send to the main thread the dead indexes.
        log::debug!("send dead_indexes = {:?}", dead_indexes);
        if let Err(err) = tx.send(dead_indexes) {
            log::error!("active health check thread exits with error {}", err);
            break;
        }

        // start a new check round if timeouts.
        // I think we cannot simply use thread::sleep(check_interval) since this thread may be woken up
        // for some reasons. So I apply a while loop to ensure at least check_inverval time has passed
        // since the last round of health check.
        while let Some(remaining_time) = check_interval.checked_sub(last_check_time.elapsed()) {
            thread::sleep(remaining_time);
        }
        // update last check time.
        last_check_time = time::Instant::now();
    }
    log::info!("active health check thread exits normally");
}

fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, std::io::Error> {
    let mut rng = rand::rngs::StdRng::from_entropy();

    // load balancing strategy: randomly select an available upstream server.

    // collect all alive upstream addresses as candidates to this dispatch.
    let dead_indexes: HashSet<_> = state.dead_indexes.iter().collect();
    let mut candidates = Vec::new();
    for (i, ip) in state.upstream_addresses.iter().enumerate() {
        if !dead_indexes.contains(&i) {
            candidates.push(ip);
        }
    }
    log::debug!("candidates = {:?}", candidates);

    // loop inv: there're candidates to be examined.
    while !candidates.is_empty() {
        // randomly select a candidate.
        let idx = rng.gen_range(0, candidates.len());
        let upstream_ip = candidates.remove(idx);
        // if it's okay to connect with it, return the connection stream.
        // otherwise, try to examine another candidate.
        if let Ok(stream) = TcpStream::connect(upstream_ip) {
            return Ok(stream);
        }
    }
    // post cond: all candidates are examined and no one is selected.
    log::error!("All upstream servers are dead currently");
    // FIXME: don't know how to set the io::Error, so I just temporarily use a foo error.
    let foo_err = std::io::Error::from_raw_os_error(22);
    Err(foo_err)
}

fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn) {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

fn handle_connection(mut client_conn: TcpStream, state: &ProxyState) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let mut upstream_conn = match connect_to_upstream(state) {
        Ok(stream) => stream,
        Err(_error) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }
    };
    let upstream_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn) {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response);
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        // The X-Forwarded-For (XFF) HTTP header field is a common method for identifying
        // the originating IP address of a client connecting to a web server through an
        // HTTP proxy or load balancer.
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn) {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()) {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response);
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response);
        log::debug!("Forwarded response to client");
    }
}
