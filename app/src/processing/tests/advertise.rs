#[cfg(test)]
pub mod advertise_tests {
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::tests::plan_test;
    use crate::processing::transform::Transform;
    use crate::processing::Plan;
    use std::collections::HashMap;

    #[test]
    fn test_aggregate_from_resource() {
        let values = vec![vec![
            "companyA".into(),
            "companyB".into(),
            "companyC".into(),
            "companyA".into(),
        ]];
        let result = vec![vec![1.into(), 2.into(), 3.into(), 1.into()]];

        let mapping: HashMap<_, _> = values
            .get(0)
            .unwrap()
            .clone()
            .into_iter()
            .zip(result.get(0).unwrap().clone().into_iter())
            .collect();

        let source_id = 3;
        let destination_id = 4;

        let stencil = format!(
            "\
            3{{sql|SELECT $comp FROM $comp($0)}}\n\
        In\n\
        Dummy{{\"id\": {source_id}, \"delay\":{delay},\"values\":{values}}}:3\n\
        Out\n\
        Dummy{{\"id\": {destination_id},\"result_size\":{size}}}:3\n\
        Transform\n\
        $comp:DummyDb{{\"query\":\"SELECT id FROM company WHERE id = $\"}}",
            delay = 10,
            values = plan_test::tests::dump(&values),
            size = values.len(),
            source_id = source_id,
            destination_id = destination_id,
        );

        let mut plan = Plan::parse(&stencil).unwrap();

        let control = plan.control_receiver();

        let transformation: &mut _ = plan.get_transformation("comp").unwrap();

        match transformation {
            Transform::DummyDB(d) => {
                d.add_mapping(mapping);
            }
            _ => panic!(),
        }

        let clone = plan.get_result(destination_id);

        plan.operate().unwrap();

        // start dummy source
        plan.send_control(&source_id, Ready(3));

        // source ready + stop, destination ready + stop
        for _command in vec![Stop(3), Stop(3)] {
            control.recv().unwrap();
        }

        let results = clone.lock().unwrap();
        for train in results.clone() {
            let vals = train.values;
            assert_eq!(vals.clone(), *result.get(0).unwrap());
            assert_ne!(vals, vec!["companyA".into(), "companyB".into()]);
        }
    }
}
