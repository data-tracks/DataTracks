use std::sync::atomic::{AtomicI64, Ordering};

use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_ID: IdBuilder = IdBuilder::new(0);
}


pub struct IdBuilder {
    id: AtomicI64,
}

impl IdBuilder {
    fn new(id: i64) -> Self {
        IdBuilder { id: AtomicI64::new(id) }
    }

    pub fn new_id(&self) -> i64 {
        self.id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[cfg(test)]
mod tests {
    use crate::util::id::IdBuilder;

    #[test]
    fn not_same() {
        let builder = IdBuilder::new(0);

        let mut ids = vec![];

        for _ in 0..1000 {
            let id = builder.new_id();
            if ids.contains(&(id)) {
                assert!(false, "overlapping ids")
            }
            ids.push(id)
        }
    }
}
