#[cfg(test)]
pub mod tests {
    use crate::processing::Plan;
    use cdc::{Container, Manager, MongoDbCdc, MongoIdentifier, PostgresCdc, PostgresIdentifier};
    use r#macro::limited;
    use postgres::types::ToSql;
    use serde::{Deserialize, Serialize};
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use threading::command::Command::{Ready, Stop};
    use tokio::runtime::Runtime;
    use tracing::{debug, info};
    use tracing_test::traced_test;
    use value::Value;
    use crate::tests::plan_test::tests;

    pub fn start_docker_postgres<S: AsRef<str>>(
        name: S,
        port: u16,
        table: Option<PostgresIdentifier>,
    ) -> Result<PostgresCdc, String> {
        let manager = Manager::new()?;
        manager.init_and_reset_container(name.as_ref(), Container::postgres("127.0.0.1", port))?;

        PostgresCdc::new("127.0.0.1", port, table)
    }

    pub fn start_docker_mongo<S: AsRef<str>>(
        name: S,
        port: u16,
        entity: MongoIdentifier,
    ) -> Result<MongoDbCdc, String> {
        let manager = Manager::new()?;
        manager.init_and_reset_container(name.as_ref(), Container::mongo_db("127.0.0.1", port))?;

        MongoDbCdc::new("127.0.0.1", port, entity)
    }

    #[limited(s = 50)]
    #[test]
    #[traced_test]
    fn test_postgres_source() {
        let container = "postgres_repl_source";
        start_docker_postgres(
            container,
            6353,
            Some(PostgresIdentifier {
                schema: None,
                table: "test".to_string(),
            }),
        )
        .unwrap();

        let values: Vec<Value> = vec!["Hey".into(), "there".into()];
        let destination = 5;

        let mut plan = Plan::parse(&format!(
            "\
            0--1\n\
            \n\
            In\n\
            Postgres{{\"url\":\"localhost\", \"port\":6353, \"user\": \"postgres\"}}:0\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{size}}}:1",
            size = values.len(),
            destination = destination
        ))
        .unwrap();

        // get result arc
        let result = plan.get_result(destination);

        plan.operate().unwrap();

        println!("after operate");

        plan.send_control(&destination, Ready(0)).unwrap();

        let control = plan.control_receiver();

        let mut cdc = init_postgres_table(6353);

        for value in values {
            debug!("before {} ", value);
            let res = cdc
                .client
                .execute(
                    "INSERT INTO test VALUES($1);",
                    &[(&value) as &(dyn ToSql + Sync)],
                )
                .map_err(|err| format!("values insert {}", err.to_string()));
            match res {
                Ok(o) => println!("Inserted {}", o),
                Err(err) => println!("{}", err),
            }

            sleep(Duration::from_secs(1));
        }

        // wait for startup else whe risk grabbing the lock too early
        let command = control.recv().unwrap();
        assert!(matches!(command, Stop(_)));

        drop(plan);

        let lock = result.lock().unwrap();
        let trains = lock.clone();
        drop(lock);

        debug!("Trains: {:?}", trains);

        assert_eq!(trains.len(), 2);

        let mgt = Manager::new().unwrap();
        mgt.stop_container(container).unwrap();
        mgt.remove_container(container).unwrap();
    }

    pub fn init_postgres_table(port: u16) -> PostgresCdc {
        let mut cdc = PostgresCdc::connect("testing", "localhost", port, None).unwrap();

        cdc.client
            .execute("CREATE TABLE test (val TEXT)", &[])
            .map_err(|err| format!("create {}", err.to_string()))
            .unwrap();
        cdc
    }

    #[limited(s = 50)]
    #[test]
    #[traced_test]
    fn test_mongodb_source() {
        let container = "mongodb_repl_source";
        start_docker_mongo(
            container,
            7373,
            MongoIdentifier {
                database: Some("test_db".to_string()),
                collection: Some("test_col".to_string()),
            },
        )
        .unwrap();

        let values: Vec<Value> = vec!["Hey".into(), "there".into()];
        let destination = 5;

        let mut plan = Plan::parse(&format!(
            "\
            0--1\n\
            \n\
            In\n\
            MongoDb{{\"url\":\"localhost\", \"port\":7373}}:0\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{size}}}:1",
            size = values.len(),
            destination = destination
        ))
        .unwrap();

        // get result arc
        let result = plan.get_result(destination);

        match plan.operate() {
            Ok(_) => {}
            Err(err) => panic!("err {}", err),
        }

        plan.send_control(&destination, Ready(0)).unwrap();

        let control = plan.control_receiver();

        let cdc = MongoDbCdc::new("localhost", 7373, MongoIdentifier::new(None, None)).unwrap();

        let runtime = Runtime::new().unwrap();

        #[derive(Clone, Debug, Deserialize, Serialize)]
        struct Item {
            name: Value,
        }

        runtime.block_on(async {
            let client = cdc.get_client().await.unwrap();

            let db = client.database("test_db");

            let col = db.collection::<Item>("test_col");

            for value in values {
                debug!("before {} ", value);
                col.insert_one(Item { name: value }).await.unwrap();

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        // wait for startup else whe risk grabbing the lock too early
        let command = control.recv().unwrap();
        assert!(matches!(command, Stop(_)));

        drop(plan);

        let lock = result.lock().unwrap();
        let trains = lock.clone();
        drop(lock);

        debug!("Trains: {:?}", trains);

        assert_eq!(trains.len(), 2);

        let mgt = Manager::new().unwrap();
        mgt.stop_container(container).unwrap();
        mgt.remove_container(container).unwrap();
    }

    #[test]
    #[limited(s = 120)]
    #[traced_test]
    fn test_mongodb_to_postgres_source() {
        let container_mongo = "mongodb_to_mongo_repl_source";
        start_docker_mongo(
            container_mongo,
            7878,
            MongoIdentifier {
                database: Some("test_db".to_string()),
                collection: Some("test_col".to_string()),
            },
        )
        .unwrap();
        let container_post = "mongodb_to_post_repl_source";
        start_docker_postgres(
            container_post,
            7777,
            Some(PostgresIdentifier {
                schema: None,
                table: "test".to_string(),
            }),
        )
        .unwrap();

        let mut cdc = init_postgres_table(7777);

        let values: Vec<Value> = vec![
            Value::text("Peter"),
            Value::text("Hans"),
        ];
        let source_id = 1;

        let mut plan = Plan::parse(&format!(
            "\
            0--1\n\
            2--3{{sql|SELECT $2.value FROM $2 WHERE $2.type = 'insert'}}--4\n\
            \n\
            In\n\
            Dummy{{\"id\": {source_id}, \"delay\":{delay},\"values\":{values}}}:0\n\
            MongoDb{{\"url\":\"localhost\", \"port\":7878, \"database\": \"test_db\"}}:2\n\
            Out\n\
            MongoDb{{\"url\":\"localhost\", \"port\":7878, \"query\":\"db.test_col.insert($1)\", \"database\": \"test_db\"}}:1\n\
            Postgres{{\"url\":\"localhost\", \"port\":7777, \"user\": \"postgres\", \"query\": \"INSERT INTO test VALUES ($0)\", \"database\":\"postgres\"}}:4",
            source_id = source_id,
            delay = 10,
            values = tests::dump(std::slice::from_ref(&values))
        ))
        .unwrap();

        match plan.operate() {
            Ok(_) => {}
            Err(err) => panic!("err {}", err),
        }

        plan.send_control(&source_id, Ready(0)).unwrap();

        let instant = Instant::now();
        let mut successful = false;
        while instant.elapsed() < Duration::from_secs(20) {
            let res = cdc.client.query("SELECT COUNT(*) AS count FROM test;", &[]).unwrap();

            if let Some(first) = res.first()
                && let Ok(count) = first.try_get::<_, i64>("count")
                && count == 2 {
                    successful = true;
                    break;

            }
            sleep(Duration::from_secs(1));
        }
        info!("duration: {:?}", instant.elapsed());

        assert!(successful);

        drop(plan);

        let mgt = Manager::new().unwrap();
        mgt.stop_container(container_mongo).unwrap();
        mgt.remove_container(container_mongo).unwrap();

        mgt.stop_container(container_post).unwrap();
        mgt.remove_container(container_post).unwrap();
    }
}
