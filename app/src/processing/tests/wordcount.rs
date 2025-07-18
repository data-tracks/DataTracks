#[cfg(test)]
mod test {
    use crate::processing::Plan;
    use crate::processing::tests::plan_test::tests::test_single_in_out;
    use rusty_tracks::Client;
    use std::thread::sleep;
    use std::time::Duration;
    use value::Value;

    #[test]
    fn wordcount_test() {
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
    fn wordcount_group_test() {
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
    fn wordcount_group_large_test() {
        let source = 1;
        let destination = 5;

        let mut plan = Plan::parse(&format!(
            "\
            0--{query}--2\n\
            \n\
            In\n\
            Tpc{{\"url\":\"127.0.0.1\",\"port\":6767}}:0\n\
            Out\n\
            Tpc{{\"url\":\"127.0.0.1\",\"port\":6565}}:2",
            query = "1{sql|SELECT unwind, COUNT(*) FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0) GROUP BY unwind}",
        )).unwrap();

        plan.operate().unwrap();

        sleep(Duration::from_secs(5));

        let client = Client::new("127.0.0.1", 6767);
        let mut connection = client.connect().unwrap();

        for _ in 0..10_000_000 {
            connection.send("This is a test.").unwrap();
        }
    }
}
