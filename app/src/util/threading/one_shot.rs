use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use std::time::Duration;

pub(crate) fn new_channel<Msg: Send + Clone, S: AsRef<str>>(
    name: S,
) -> (SingleTx<Msg>, SingleRx<Msg>) {
    let (tx, rx) = unbounded::<Msg>();

    let uuid = uuid::Uuid::new_v4();

    (
        SingleTx(tx, rx.clone(), name.as_ref().to_string(), uuid),
        SingleRx(rx, name.as_ref().to_string(), uuid),
    )
}

#[derive(Clone)]
pub struct SingleRx<D>(Receiver<D>, String, pub(crate) uuid::Uuid);

impl<F> SingleRx<F> {
    pub fn recv_timeout(&self, duration: Duration) -> Result<F, RecvTimeoutError> {
        self.0.recv_timeout(duration)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn recv(&self) -> Result<F, String> {
        self.0.recv().map_err(|e| e.to_string())
    }

    pub(crate) fn try_recv(&self) -> Result<F, String> {
        self.0.try_recv().map_err(|e| e.to_string())
    }

    pub fn name(&self) -> String {
        self.1.clone()
    }
}

#[derive(Clone)]
pub struct SingleTx<T>(Sender<T>, Receiver<T>, String, pub(crate) uuid::Uuid);

impl<F: Send> SingleTx<F> {
    pub(crate) fn subscribe(&self) -> SingleRx<F> {
        SingleRx(self.1.clone(), self.name(), self.3)
    }

    pub(crate) fn send(&self, msg: F) -> Result<(), String> {
        self.0.send(msg).map_err(|e| e.to_string())
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub fn name(&self) -> String {
        self.2.clone()
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread::spawn;

    use crate::util::threading::one_shot::new_channel;
    use rand::random;

    #[test]
    fn receive() {
        let value = 3i64;
        let (tx, rx) = new_channel("test");

        let _ = tx.send(value);

        assert_eq!(3, rx.recv().unwrap())
    }

    #[test]
    fn keep_track() {
        let (tx, rx) = new_channel("test");

        tx.send(3i64).unwrap();

        assert_eq!(tx.0.len(), 1);

        let _ = rx.recv().unwrap();

        assert_eq!(tx.0.len(), 0);
    }

    #[test]
    fn keep_track_hugh() {
        let (tx, rx) = new_channel("test");

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
        let (tx, rx) = new_channel("test");

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
}
