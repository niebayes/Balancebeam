use crate::ProxyState;

use rand::{Rng, SeedableRng};
use std::net::TcpStream;

// Power of Two Random Choices load balancing algorithm.
// repeatedly randomly choose two alive upstream servers, first try to connect with
// the one with lower #alive connections, then try to connect with the other one.
// if neither of the two can be connected, try to select another randome two.
pub fn select_upstream(
    state: &ProxyState,
    candidates: &mut Vec<&String>,
) -> Result<TcpStream, std::io::Error> {
    // FIXME: don't know how to set the io::Error, so I just temporarily use a foo error.
    let foo_err = std::io::Error::from_raw_os_error(22);

    // randome number generator.
    let mut rng = rand::rngs::StdRng::from_entropy();
    // loop inv: there're at least two candidates to be examined.
    while candidates.len() >= 2 {
        // randomly choose two candidates.
        let mut upstream_addrs = Vec::new();
        for _ in 0..2 {
            // randomly choose one candidate.
            let idx = rng.gen_range(0, candidates.len());
            upstream_addrs.push(candidates.remove(idx));
        }
        // select the one with lower #alive connections.
        let mut alive_conns = state.alive_conns.lock().unwrap();
        let cnt0 = alive_conns.get_mut(upstream_addrs[0]).unwrap();
        let cnt1 = alive_conns.get_mut(upstream_addrs[1]).unwrap();
        if cnt0 <= cnt1 {
            // passive health check works on here, i.e. if fails to connect with one server, try another one.
            // try to connect with the first one.
            if let Ok(stream) = TcpStream::connect(upstream_addrs[0]) {
                // increment #alive connections.
                *cnt0 += 1;
                return Ok(stream);
            }
            // try to connect with the second one.
            if let Ok(stream) = TcpStream::connect(upstream_addrs[1]) {
                // increment #alive connections.
                *cnt1 += 1;
                return Ok(stream);
            }
        } else {
            // try to connect with the second one.
            if let Ok(stream) = TcpStream::connect(upstream_addrs[1]) {
                // increment #alive connections.
                *cnt1 += 1;
                return Ok(stream);
            }
            // try to connect with the first one.
            if let Ok(stream) = TcpStream::connect(upstream_addrs[0]) {
                // increment #alive connections.
                *cnt0 += 1;
                return Ok(stream);
            }
        }
    }
    // post cond: #candidates is less than 2, i.e. 1 or 0.
    if let Some(&addr) = candidates.first() {
        if let Ok(stream) = TcpStream::connect(addr) {
            // successfully connected with the only one candidate.
            // increment #alive connections.
            let mut alive_conns = state.alive_conns.lock().unwrap();
            let cnt = alive_conns.get_mut(addr).unwrap();
            *cnt += 1;
            return Ok(stream);
        }
    }
    // no candidates or failed to connect with the only one candidate.
    Err(foo_err)
}
