use error::error::TrackError;
#[cfg(test)]
use threading::channel::new_broadcast;
#[cfg(test)]
use threading::channel::Tx;
use threading::multi::MultiSender;
use threading::pool::HybridThreadPool;
use value::train::Train;

pub trait ChangeDataCapture {
    #[cfg(test)]
    fn listen_test(&mut self) -> Result<(HybridThreadPool, Tx<Train>), TrackError> {
        let tx = new_broadcast("test");
        let pool = HybridThreadPool::new();
        let _id = self.listen(0, vec![tx.clone()].into(), pool.clone())?;
        Ok((pool, tx))
    }

    fn listen(
        &mut self,
        id: usize,
        outs: MultiSender<Train>,
        pool: HybridThreadPool,
    ) -> Result<usize, TrackError>;
}
