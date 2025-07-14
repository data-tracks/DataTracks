#[cfg(test)]
mod test{
    use value::Value;
    use crate::processing::Plan;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::tests::plan_test::tests::{dump, test_single_in_out};

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

        test_single_in_out("1{sql|SELECT unwind, COUNT(*) FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0) GROUP BY unwind}", values.clone(), res.clone(), source, destination, false);
    }

}