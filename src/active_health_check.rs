use crate::request;
use crate::response;
use crate::ProxyState;

use std::collections::HashSet;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time;

pub fn active_health_check(state: Arc<ProxyState>) {
    log::debug!("start active health check thread");

    let check_interval = time::Duration::from_secs(state.active_health_check_interval as u64);

    // this thread runs in an infinite loop and exits when the main thread exits.
    loop {
        // do active health check by sending a request to the active health check path of each
        // application server. If it responds with 200 status, the application server is alive.
        // Otherwise, we assume it's dead.
        let last_check_time = time::Instant::now();

        // the indexes of the dead upstream servers in the state.upstream_addresses array.
        let mut dead_indexes = HashSet::new();

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
                if let Err(err) = request::write_to_stream(&request, &mut stream) {
                    log::error!("encounters error {} when writing to stream connected with upstream server {}", err.to_string(), i);
                    dead_indexes.insert(i);
                    continue;
                }

                // read the response of the upstream server from the stream.
                // read_from_stream will block until a valid response is read completely.
                match response::read_from_stream(&mut stream, &http::Method::HEAD) {
                    Ok(response) => {
                        // if the server responds with a status code which is not the expected 200,
                        // we assume the application server is currently unable to serve the
                        // requests. So we mark it as dead.
                        if response.status().as_u16() != 200 {
                            dead_indexes.insert(i);
                        }
                        // although the test suite says that it replaces a good server with
                        // a server only returns status 500, it seems we cannot successfully
                        // establish a connection with this new server and hence the only possible
                        // status code we receive at here is 200, aka. OK.
                        log::debug!(
                            "active health check: server {} returns status {}",
                            ip,
                            response.status().as_u16()
                        );
                    }
                    Err(_) => {
                        log::error!("encounters error when writing to stream connected with upstream server {}", i);
                        dead_indexes.insert(i);
                    }
                }
                // the stream is closed when leaving this scope.
            } else {
                log::error!(
                    "unable to establish a connection with the upstream server {}",
                    i
                );
                dead_indexes.insert(i);
            }
        }

        // update upstream server status according to the collected dead indexes.
        // create a critical section to invoke the lock's RAII.
        {
            log::debug!("dead_indexes = {:?}", dead_indexes);
            let mut status = state.upstream_status.write().unwrap();
            for (i, alive) in status.iter_mut().enumerate() {
                if dead_indexes.contains(&i) {
                    *alive = false;
                } else {
                    *alive = true;
                }
            }
        }
        log::debug!("successfully updated the status");

        // start a new check round if timeouts.
        // I think we cannot simply use thread::sleep(check_interval) since this thread may be woken up
        // for some unknown reasons (??). So I apply a while loop to ensure that at least check_inverval time
        // has passed since the last round of health check.
        while let Some(remaining_time) = check_interval.checked_sub(last_check_time.elapsed()) {
            thread::sleep(remaining_time);
        }
    }
}
