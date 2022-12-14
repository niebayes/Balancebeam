mod active_health_check;
mod cache;
mod load_balancing;
mod rate_limit_check;
mod connection_pool;
mod request;
mod response;
mod cache_replacer;

use active_health_check::active_health_check;
use clap::Parser;
use rand::{Rng, SeedableRng};
use rate_limit_check::WindowTracker;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use threadpool::ThreadPool;

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
pub struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    #[allow(dead_code)]
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_request_rate: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
    /// Status of upstream servers. True if the corresponding server is alive.
    upstream_status: RwLock<Vec<bool>>,
    /// Sliding window tracker for the sliding window rate limiting algorithm.
    window_tracker: Mutex<WindowTracker>,
    /// Alive connection tracker for the Power of Two Random Choices load balancing algorithm.
    alive_conns: Mutex<HashMap<String, usize>>,
}

// window size of the sliding window rate limiting algorithm.
// i.e. the length of a time bucket.
const WINDOW_SIZE: u64 = 60;
// invoke the sliding_window_ticker every TICK_INTERVAL secs
// to slide the window to the current timestamp.
const TICK_INTERVAL: usize = 1;
// size of the thread pool.
const NUM_THREADS: usize = 4;

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

    // init alive connections mapping.
    let mut alive_conns = HashMap::new();
    for addr in options.upstream.iter() {
        alive_conns.insert(addr.clone(), 0);
    }

    // to represent the state of the proxy/load balancer.
    let state = ProxyState {
        // dynamic fields.
        upstream_status: RwLock::new(vec![true; options.upstream.len()]),
        window_tracker: Mutex::new(WindowTracker::new(WINDOW_SIZE)),
        alive_conns: Mutex::new(HashMap::new()),
        // static fields.
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_request_rate: options.max_requests_per_minute,
        upstream_addresses: options.upstream,
    };
    // each thread shares the single state instance.
    let state = Arc::new(state);

    // spawn a thread to do active health check periodically.
    let shared_state = state.clone();
    thread::spawn(move || active_health_check(shared_state));

    // no need to do rate limiting if max request rate is 0.
    if state.max_request_rate > 0 {
        let shared_state = state.clone();
        // spawn a thread to slide the window periodically.
        thread::spawn(move || rate_limit_check::sliding_window_ticker(shared_state, TICK_INTERVAL));
    }

    // create a thread pool to handle connections.
    let _thread_pool = ThreadPool::new(NUM_THREADS);

    // note, it seems the tcp lib is not compatible with mac OS and hence you have to
    // run this program in linux to make it work.
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            log::debug!("start handle a connection");

            // Handle the connection!
            // dispatch this job to an idle worker in the thread pool.
            // if there's no idle workers currently, the job is pushed
            // into the waiting queue and might be scheduled when a worker
            // becomes idle.
            let shared_state = state.clone();
            // thread_pool.execute(move || handle_connection(stream, shared_state));
            handle_connection(stream, shared_state);
        } else {
            log::debug!("error in the incoming stream");
        }
    }
}

fn collect_alive_servers(state: &ProxyState) -> Vec<&String> {
    let status = state.upstream_status.read().unwrap();
    let mut candidates = Vec::new();
    for (i, ip) in state.upstream_addresses.iter().enumerate() {
        if *status.get(i).unwrap() == true {
            candidates.push(ip);
        }
    }
    candidates
}

fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, std::io::Error> {
    // collect all alive upstream addresses as candidates to this dispatch.
    // active health check works on here
    let mut candidates = collect_alive_servers(state);
    log::debug!("candidates = {:?}", candidates);

    // select an upstream to handle the connection.
    load_balancing::select_upstream(state, &mut candidates)
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

fn handle_connection(mut client_conn: TcpStream, state: Arc<ProxyState>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let mut upstream_conn = match connect_to_upstream(state.as_ref()) {
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

        // do rate limiting only if the max_request_rate is greater than 0.
        if state.max_request_rate > 0 {
            // if exceeds the limited request rate, i.e. processing this request would exceed the rate limit, refuse to
            // proxy it to the upstream server.
            let mut window_tracker = state.window_tracker.lock().unwrap();
            if !window_tracker.rate_limit_check(state.as_ref()) {
                // refuse this request.
                log::debug!("rate limit check fails");

                // send a response to the client to tell this request is refused due to
                // too many requests.
                let response = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
                if let Err(err) = response::write_to_stream(&response, &mut client_conn) {
                    log::error!("encounters error {:?} when writing to stream", err);
                }
                break;
            }
            // accept this request.
            window_tracker.incre_counter();
            log::debug!(
                "curr_bucket_counter = {}",
                window_tracker.curr_bucket_counter
            );
        }

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

    // the connection ends, decrement #alive connections.
    let mut alive_conns = state.alive_conns.lock().unwrap();
    let cnt = alive_conns.get_mut(&upstream_ip).unwrap();
    *cnt -= 1;
    assert!(cnt >= &mut 0);
}
