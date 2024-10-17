use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub struct Channel<F>
where
    F: Send,
{
    size: Arc<AtomicU64>,
    buffer: Mutex<VecDeque<F>>,
    condvar: Condvar
}


pub fn new_channel<F>() -> (Tx<F>, Arc<AtomicU64>, Rx<F>)
where
    F: Send,
{
    let channel = Arc::new(
        Channel {
            size: Arc::new(AtomicU64::default()),
            buffer: Mutex::new(VecDeque::new()),
            condvar: Condvar::new()
        });

    let rx = Rx { channel: Arc::clone(&channel) };
    let tx = Tx { channel: Arc::clone(&channel) };
    (tx, Arc::clone(&channel.size), rx)
}


#[derive(Clone)]
pub struct Rx<F>
where
    F: Send,
{
    channel: Arc<Channel<F>>,
}

impl<F> Rx<F>
where
    F: Send,
{
    pub(crate) fn recv(&self) -> Result<F, String> {
        let mut vec = self.channel.buffer.lock().unwrap();
        loop {
            if let Some(element) = vec.pop_front(){
                self.channel.size.fetch_sub(1, Ordering::SeqCst);
                return Ok(element);
            }
            vec = self.channel.condvar.wait(vec).unwrap()
        }
    }
    pub(crate) fn try_recv(&self) -> Result<F, String> {
        let mut vec = self.channel.buffer.lock().unwrap();
        if !vec.is_empty() {
            let element = vec.pop_front();
            match element {
                None => Err("Could not get error".to_string()),
                Some(e) => {
                    self.channel.size.fetch_sub(1, Ordering::SeqCst);
                    Ok(e)
                }
            }
        } else {
            Err("Could not get error".to_string())
        }
    }
}


#[derive(Clone)]
pub struct Tx<F>
where
    F: Send,
{
    channel: Arc<Channel<F>>,
}

impl<F> Tx<F>
where
    F: Send,
{
    pub(crate) fn send(&self, msg: F) -> Result<(), String> {
        match self.channel.buffer.lock() {
            Ok(mut b) => {
                self.channel.size.fetch_add(1, Ordering::SeqCst);
                b.push_back(msg);
                self.channel.condvar.notify_one();
                Ok(())
            }
            Err(e) => Err(e.to_string())
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread::spawn;
    use std::time::Instant;

    use rand::random;

    use crate::util::channel::new_channel;

    #[test]
    fn receive() {
        let value = 3i64;
        let (tx, _counter, rx) = new_channel();

        tx.send(value.clone()).unwrap();

        assert_eq!(3, rx.recv().unwrap())
    }

    #[test]
    fn keep_track() {
        let (tx, counter, rx) = new_channel();

        tx.send(3i64).unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 1u64);

        let _ = rx.recv().unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0u64);
    }

    #[test]
    fn keep_track_hugh() {
        let (tx, counter, rx) = new_channel();

        let num = 1_000_000u64;
        let mut size = 1_000_000u64;

        for _ in 0..num {
            tx.send(3i64).unwrap();
        }


        for _ in 0..num {
            if random() {
                tx.send(3i64).unwrap();
                size += 1;
            } else {
                let _ = rx.recv().unwrap();
                size -= 1;
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), size);
    }

    #[test]
    fn keep_track_multi_thread() {
        let (tx, counter, rx) = new_channel();

        let num = 1_000_000u64;
        let size = Arc::new(AtomicU64::new(1_000_000u64));

        for _ in 0..num {
            tx.send(3i64).unwrap();
        }

        let mut ths = vec![];


        for _ in 0..100 {
            let clone_tx = tx.clone();
            let clone_rx = rx.clone();
            let clone_size = Arc::clone(&size);
            let handler = spawn(move || {
                for _num in 0..num {
                    if random() {
                        clone_tx.send(3i64).unwrap();
                        clone_size.fetch_add(1, Ordering::SeqCst);
                    } else {
                        let _ = clone_rx.recv().unwrap();
                        clone_size.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            });
            ths.push(handler);
        }
        ths.into_iter().for_each(|t| t.join().unwrap());


        assert_eq!(counter.load(Ordering::SeqCst), size.load(Ordering::SeqCst));
    }

    #[test]
    fn performance() {
        let (tx, rc) = channel();

        let mut instant = Instant::now();
        for _ in 0..1_000_000 {
            tx.send(3).unwrap();
            let _val = rc.recv().unwrap();
        }
        let std = instant.elapsed();

        let (tx, _counter, rc) = new_channel();

        instant = Instant::now();
        for _ in 0..1_000_000 {
            tx.send(3).unwrap();
            rc.recv().unwrap();
        }
        let new_time = instant.elapsed();


        println!("std: {}ms vs counted: {}ms", std.as_millis(), new_time.as_millis());

        assert!((5 * std.as_millis()) >= new_time.as_millis())
    }
}