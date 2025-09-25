use error::error::TrackError;
use crate::channel::Tx;

#[derive(Clone)]
pub struct MultiSender<T: Clone + Send + Sync + 'static> {
    pub outs: Vec<Tx<T>>,
}

impl<T: Clone + Send + Sync + 'static> MultiSender<T> {
    pub fn new(outs: Vec<Tx<T>>) -> MultiSender<T> {
        MultiSender { outs }
    }

    pub fn send(&self, msg: T) -> Result<(), TrackError> {
        self.outs.iter().try_for_each(|out| out.send(msg.clone()))
    }
}

impl<T: Clone + Send + Sync + 'static> From<Vec<Tx<T>>> for MultiSender<T> {
    fn from(outs: Vec<Tx<T>>) -> MultiSender<T> {
        MultiSender { outs }
    }
}
