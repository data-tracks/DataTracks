#[cfg(test)]
mod tests {
    use rusqlite::fallible_iterator::FallibleIterator;
    use rusty_tracks::Client;
    use crate::tpc::TpcSource;


    #[test]
    fn complete_replay() {
        let mut source = TpcSource::new(Some("localhost"), 3535);

        // send some values

        let (id, pool) = source.operate_test();

        let client = Client::new("localhost", 3535);

        let connection = client.connect().unwrap();

        //receive the values
    }
}