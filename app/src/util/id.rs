use std::sync::atomic::{AtomicUsize, Ordering};

static GLOBAL_ID: AtomicUsize = AtomicUsize::new(0);

pub fn new_id() -> usize {
    GLOBAL_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use crate::util::new_id;

    #[test]
    fn not_same() {
        let mut ids = vec![];

        for _ in 0..1000 {
            let id = new_id();
            if ids.contains(&(id)) {
                panic!("overlapping ids")
            }
            ids.push(id)
        }
    }
}
