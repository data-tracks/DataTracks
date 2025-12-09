use std::error::Error;
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

    pub async fn connect(&self) -> Result<Client, Box<dyn Error + Send + Sync>> {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "dbname={db} host={host} port={port} user={user} password={password}",
                db = self.db,
                host = self.url,
                port = self.port,
                user = self.user,
                password = self.password
            ),
            NoTls,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(client)
    }
}
