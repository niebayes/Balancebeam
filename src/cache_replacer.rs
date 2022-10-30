use crate::cache::{Cache, Frame, FrameId};

use std::collections::{LinkedList, HashMap};

// LRU-K replacer.
pub struct LRUKReplacer {
    cold_list: LinkedList<FrameId>,
    hot_list: LinkedList<FrameId>,
}
