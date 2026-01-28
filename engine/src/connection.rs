use std::time::Duration;
use anyhow::bail;
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
    pub async fn connect(
        &self,
        join: &mut JoinSet<()>,
    ) -> anyhow::Result<Client> {
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
        bail!("timeout while connecting to postgres")
    }
}
