use postgres::{Client, NoTls};
use crate::Manager;
use crate::util::Container;

pub struct PostgresCdc {
    pub client: Client,
}

impl PostgresCdc {
    pub fn new<S: AsRef<str>>(name: S) -> Result<Self, String> {
        let manager = Manager::new()?;
        manager.init_and_reset_container(name, Container::postgres())?;

        Self::connect("localhost", 5432 )
    }

    fn connect<S: AsRef<str>>(url: S, port: u16) -> Result<Self, String> {
        let client = Client::connect(
            &format!("host={} port={} user=postgres password=postgres replication=database", url.as_ref(), port),
            NoTls,
        ).map_err(|err| err.to_string())?;

        Ok(PostgresCdc {
            client
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::postgres::replication::PostgresCdc;

    #[test]
    fn test_struct() {
        PostgresCdc::new("test").unwrap();
    }
}