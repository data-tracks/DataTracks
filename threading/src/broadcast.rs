use crate::one_shot;
use crate::one_shot::{SingleRx, SingleTx};
use parking_lot::Mutex;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use error::error::TrackError;

#[derive(Clone)]
pub struct BroadcastTx<T: Send + Clone + 'static> {
    name: String,
    inner: Arc<Mutex<Vec<SingleTx<T>>>>,
}

impl<T: Clone + Send + 'static> Deref for BroadcastRx<T> {
    type Target = SingleRx<T>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl<T: Clone + Send + 'static> DerefMut for BroadcastRx<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

pub struct BroadcastRx<T: Send + Clone + 'static> {
    name: String,
    receiver: SingleRx<T>,
    sender_ref: Arc<Mutex<Vec<SingleTx<T>>>>,
}

impl<T: Clone + Send + 'static> BroadcastTx<T> {
    pub fn new_empty_channel<S: AsRef<str>>(name: S) -> Self {
        let inner = Arc::new(Mutex::new(vec![]));

        BroadcastTx {
            inner,
            name: String::from(name.as_ref()),
        }
    }

    pub fn new_channel<S: AsRef<str>>(name: S) -> (Self, BroadcastRx<T>) {
        let tx = Self::new_empty_channel(name);
        let rx = tx.subscribe();
        (tx, rx)
    }

    pub(crate) fn name(&self) -> String {
        self.name.clone()
    }

    pub(crate) fn subscribe(&self) -> BroadcastRx<T> {
        let (tx, rx) = one_shot::new_channel(self.name.clone());
        self.inner.lock().push(tx);
        BroadcastRx {
            name: self.name.clone(),
            receiver: rx,
            sender_ref: self.inner.clone(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock().iter().map(|s| s.len()).sum()
    }

    pub fn send(&self, value: T) -> Result<(), TrackError> {
        let inner = self.inner.lock();
        inner.iter().try_for_each(|tx| tx.send(value.clone()))
    }
}

impl<T: Clone + Send + 'static> Clone for BroadcastRx<T> {
    fn clone(&self) -> Self {
        let (tx, rx) = one_shot::new_channel(self.name.clone());
        self.sender_ref.lock().push(tx);
        BroadcastRx {
            name: self.name.clone(),
            receiver: rx,
            sender_ref: self.sender_ref.clone(),
        }
    }
}

impl<T: Clone + Send + 'static> Drop for BroadcastRx<T> {
    fn drop(&mut self) {
        // inefficient but should only happen rarely
        self.sender_ref.lock().retain(|s| s.3 != self.receiver.2)
    }
}

#[cfg(test)]
mod tests {
    use crate::broadcast::BroadcastTx;

    #[test]
    fn test_broadcast() {
        let (tx, rx1) = BroadcastTx::new_channel("test");

        let rx2 = rx1.clone();
        let rx3 = rx2.clone();

        tx.send(3).unwrap();

        assert_eq!(rx1.recv().unwrap(), 3);
        assert_eq!(rx2.recv().unwrap(), 3);
        assert_eq!(rx3.recv().unwrap(), 3);
    }

    #[test]
    fn test_drop() {
        let (tx, rx1) = BroadcastTx::<i32>::new_channel("test");

        let rx2 = rx1.clone();
        let rx3 = rx2.clone();

        drop(rx1);
        tx.send(3).unwrap();

        assert_eq!(rx2.recv().unwrap(), 3);
        assert_eq!(rx3.recv().unwrap(), 3);
    }
}
