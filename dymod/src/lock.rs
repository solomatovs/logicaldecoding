use std::collections::{hash_map, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread, ThreadId};

pub struct LockByName {
    inner: Arc<Mutex<Inner>>,
}
#[must_use = "if unused the lock will immediately unlock"]
pub struct NamedGuard {
    inner: Arc<Mutex<Inner>>,
    key: String,
}

struct Inner {
    map: HashMap<String, Entry>,
}
struct Entry {
    current: ThreadId,
    queue: VecDeque<Thread>,
}

impl LockByName {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                map: HashMap::new(),
            })),
        }
    }

    pub fn lock(&self, name: &str) -> NamedGuard {
        let me = thread::current();

        // Add myself to queue.
        {
            let mut lock = self.inner.lock().unwrap();
            match lock.map.entry(name.to_string()) {
                hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().queue.push_back(me.clone());
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(Entry {
                        current: me.id(),
                        queue: VecDeque::new(),
                    });

                    return NamedGuard {
                        inner: self.inner.clone(),
                        key: name.to_string(),
                    };
                }
            }
        }

        // Wait until its my turn.
        loop {
            std::thread::park();

            let mut lock = self.inner.lock().unwrap();
            let entry = lock.map.get_mut(name).unwrap();

            if entry.current == me.id() {
                return NamedGuard {
                    inner: self.inner.clone(),
                    key: name.to_string(),
                };
            }
        }
    }
}

impl Drop for NamedGuard {
    fn drop(&mut self) {
        let mut lock = self.inner.lock().unwrap();
        let entry = lock.map.get_mut(&self.key).unwrap();

        if let Some(next) = entry.queue.pop_front() {
            entry.current = next.id();
            drop(lock);
            next.unpark();
        } else {
            lock.map.remove(&self.key);
        }
    }
}

// fn main() {
//     let locks = LockByName::new();
//     let _lok1 = locks.lock("abc");
//     println!("Got abc");
//     let _lok2 = locks.lock("def");
//     println!("Got def");
//     let _lok3 = locks.lock("abc"); // should block
//     println!("Should not get here.")
// }
