use crossbeam::channel::{Receiver, Sender};

#[derive(Clone)]
pub(crate) struct Channel<F>
where
    F: Send,
{
    sender_in: Sender<F>,
    receiver_in: Receiver<F>,
    sender_out: Sender<F>,
    receiver_out: Receiver<F>,
}

impl<F> Channel<F> {
    pub(crate) fn operate() {}
}