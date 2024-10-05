use crate::algebra::BoxedIterator;
use crate::processing::train::MutWagonsFunc;

pub struct Puller {
    func: MutWagonsFunc,
}

impl Puller {
    pub(crate) fn pull(&self) -> BoxedIterator {
        todo!()
    }
}

