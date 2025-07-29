#[cfg(test)]
mod test {
    use crate::processing::Plan;
    use crate::processing::tests::plan_test::tests::test_single_in_out;
    use rand::random_range;
    use rusty_tracks::Client;
    use value::Value;

    #[test]
    fn word_count_test() {
        let values = vec![vec!["Hey there".into(), "how are you".into()]];
        let res: Vec<Vec<Value>> = vec![vec![
            "Hey".into(),
            "there".into(),
            "how".into(),
            "are".into(),
            "you".into(),
        ]];
        let source = 1;
        let destination = 5;

        test_single_in_out(
            "1{sql|SELECT * FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0)}",
            values.clone(),
            res.clone(),
            source,
            destination,
            false,
        );
    }

    #[test]
    fn word_count_group_test() {
        let values = vec![vec!["Hey Hallo".into(), "Hey".into()]];
        let res: Vec<Vec<Value>> = vec![vec![
            vec!["Hey".into(), 2.into()].into(),
            vec!["Hallo".into(), 1.into()].into(),
        ]];
        let source = 1;
        let destination = 5;

        test_single_in_out(
            "1{sql|SELECT unwind, COUNT(*) FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0) GROUP BY unwind}",
            values.clone(),
            res.clone(),
            source,
            destination,
            false,
        );
    }

    #[test]
    fn word_count_group_large_test() {
        let offset = random_range(0..1000);
        let in_port = 6767 + offset;
        let offset = random_range(0..1000);
        let out_port = 6565 + offset;

        let mut plan = Plan::parse(&format!(
            "\
            0--{query}--2\n\
            \n\
            In\n\
            Tpc{{\"url\":\"127.0.0.1\",\"port\":{in_port}}}:0\n\
            Out\n\
            Tpc{{\"url\":\"127.0.0.1\",\"port\":{out_port}}}:2",
            query = "1{sql|SELECT unwind, COUNT(*) FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0) GROUP BY unwind}",
        )).unwrap();

        plan.operate().unwrap();

        let client = Client::new("127.0.0.1", in_port);
        let mut connection = client.connect().unwrap();

        for _ in 0..10_000 {
            connection.send("This is a test.").unwrap();
        }

        let client = Client::new("127.0.0.1", out_port);
        let _connection = client.connect().unwrap();
    }

}
