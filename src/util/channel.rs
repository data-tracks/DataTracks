use crossbeam::channel::{unbounded, Receiver, Sender};


pub fn new_channel<Msg: Send>() -> (Tx<Msg>, Rx<Msg>) {
    let (tx, rx) = unbounded::<Msg>();

    (Tx(tx), Rx(rx))
}

#[derive(Clone)]
pub struct Rx<D>(Receiver<D>);

impl<F> Rx<F>
where
    F: Send,
{
    pub fn len(&self) -> usize { self.0.len() }
    pub fn recv(&self) -> Result<F, String> {
        self.0.recv().map_err(|e| e.to_string())
    }
    pub(crate) fn try_recv(&self) -> Result<F, String> {
        self.0.try_recv().map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
pub struct Tx<T>(Sender<T>);

impl<F> Tx<F>
where
    F: Send,
{
    pub(crate) fn send(&self, msg: F) -> Result<(), String> {
        self.0.send(msg).map_err(|e| e.to_string())
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
        let (tx, rx) = new_channel();

        tx.send(value.clone()).unwrap();

        assert_eq!(3, rx.recv().unwrap())
    }

    #[test]
    fn keep_track() {
        let (tx, rx) = new_channel();

        tx.send(3i64).unwrap();

        assert_eq!(tx.0.len(), 1);

        let _ = rx.recv().unwrap();

        assert_eq!(tx.0.len(), 0);
    }

    #[test]
    fn keep_track_hugh() {
        let (tx, rx) = new_channel();

        let num = 1_000_000u64;
        let mut size = 1_000_000;

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

        assert_eq!(tx.0.len(), size);
    }

    #[test]
    fn keep_track_multi_thread() {
        let (tx, rx) = new_channel();

        let num = 1_000_000;
        let size = Arc::new(AtomicU64::new(1_000_000));

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

        assert_eq!(tx.0.len(), size.load(Ordering::SeqCst) as usize);
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

        let (tx, rx) = new_channel();

        instant = Instant::now();
        for _ in 0..1_000_000 {
            tx.send(3).unwrap();
            rx.recv().unwrap();
        }
        let new_time = instant.elapsed();

        println!(
            "std: {}ms vs counted: {}ms",
            std.as_millis(),
            new_time.as_millis()
        );

        assert!((8 * std.as_millis()) >= new_time.as_millis())
    }
}
