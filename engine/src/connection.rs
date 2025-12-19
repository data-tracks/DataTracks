use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};

#[derive(Clone, Debug, PartialEq)]
pub struct PostgresConnection {
    pub(crate) url: String,
    pub(crate) port: u16,
    pub(crate) db: String,
    pub(crate) user: String,
    pub(crate) password: String,
}

impl PostgresConnection {
    pub fn new<S1: AsRef<str>, S2: AsRef<str>, S3: AsRef<str>, S4: AsRef<str>>(
        url: S1,
        port: u16,
        db: S2,
        user: S3,
        password: S4,
    ) -> Self {
        PostgresConnection {
            url: url.as_ref().to_string(),
            port,
            db: db.as_ref().to_string(),
            user: user.as_ref().to_string(),
            password: password.as_ref().to_string(),
        }
    }

    pub async fn connect(
        &self,
        join: &mut JoinSet<()>,
    ) -> Result<Client, Box<dyn Error + Send + Sync>> {
        let connection_string = format!(
            "dbname={db} host={host} port={port} user={user} password={password}",
            db = self.db,
            host = self.url,
            port = self.port,
            user = self.user,
            password = self.password
        );

        for _ in 0..3 {
            let res = tokio_postgres::connect(&connection_string, NoTls).await;

            match res {
                Ok((client, connection)) => {
                    join.spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("connection error: {}", e);
                        }
                    });

                    return Ok(client);
                }
                Err(_) => {
                    sleep(Duration::from_secs(3)).await;
                }
            }
        }
        Err(Box::from("timeout while connecting to postgres"))
    }
}
