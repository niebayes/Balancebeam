use crate::cache_replacer::LRUKReplacer;
use httparse::Response;
use std::collections::{HashMap, LinkedList, VecDeque};
use std::sync::{Arc, RwLock};
use std::thread;

// number of frames in the cache.
const NUM_FRAMES: usize = 10;
// max number of bytes a frame could store.
const FRAME_CAPACITY: usize = 4096;
// the period to do clean up work. unit: seconds.
const CLEAN_INTERVAL: usize = 1;
// the initial lease for a new cache object. unit: seconds.
const OBJECT_LEASE: usize = 10;

// the cache consists of NUM_FRAMES frames. That's the capacity of the cache and won't be expanded.
// each frame is a buffer to hold cache object. A frame has a fixed-size FRAME_CAPACITY capacity
// and cannot be expanded.
// the cache has an associated LRU-2 replacer.
// the cache manages three list of frames (actually, it's the frame ids are keep tracked of).
//     free list: clean frames, i.e. not accessed recently.
//     cold list: frames that were accessed once recently.
//     hot list: frames that were accessed more than once recently.
// note, the cache and replacer are very different from the buffer pool and its associated replacer.
// when a cache entry is inserted into the cache, following operations will be performed:
// (1) check if it's already cache by looking up the hash map.
// (2) if it's cached, then it must in the cold list or the hot list, we can safely move it into the
//     back of the hot list.
// (3) if it's not cached, then we try to allocate a frame to hold it. The frame might be free frame
//     or a evicted frame. In summary, we try pop out the front from the free list, cold list and
//     hot list, in order. The old cache entry is discard (if any) and the content of the frame is
//     replaced by the new cache entry.
// the reason why we trying to pop out from the cold list first, is that we want to keep hot
// data longer.
// the reason why we use a cold list, i.e. why not basic LRU, is that LRU suffers from the problem
// of cache corruption in that the cache is filled with objects only accessed once and will never
// get accessed again, i.e. cache thrashing.
// to optimize the performance, a hash map is used to keep track of which list each frame is in.

// note, a cache is used to make the proxy server more efficient but it also raises the probability
// to serve stale content for the clients.

pub type FrameId = usize;

// proxy cache.
pub struct Cache {
    frames: [Frame; NUM_FRAMES],
    // key: cache key, value: the id of the frame holding the cache object.
    objects: HashMap<String, FrameId>,
    free_list: VecDeque<FrameId>,
    replacer: LRUKReplacer,
}

pub enum FrameStatus {
    FREE,
    COLD,
    HOT,
}

pub struct Frame {
    status: FrameStatus,
    content: String, // response body.
}

impl Cache {
    // construct a cache instance.
    pub fn new() -> Cache {

        // spawn a thread to clean up stale cache objects.
    }

    // return true if a response can be cached.
    // cache restrictions, i.e. objects are cached only if all of the following are true:
    // (1) the size of the response body does not exceed MAX_OBJECT_SIZE.
    // (2) the response from the upstream server is `200 OK`.
    // (3) the response does not have a `Vary` header
    // (4) the response does not have a `Cache-Control: no-cache` header
    // these cache restrictions are copied from https://www.haproxy.com/documentation/hapee/latest/load-balancing/caching/
    pub fn cacheable(response: &Response<Vec<u8>>) -> bool {}

    // insert a new cache entry into the cache.
    pub fn insert(&mut self, entry: CacheEntry) {}

    // fetch a cache object given the cache key.
    pub fn fetch(&self, key: &String) -> String {

        // on each successful fetching, the object lease is refreshed.
    }
}

// periodically clean up stale cache objects according to their leases.
fn cache_cleaner(cache: Arc<Cache>) {}
