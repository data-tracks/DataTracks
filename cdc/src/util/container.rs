use crate::util::Container::MongoDb;
use crate::util::container::Container::Postgres;
use bollard::Docker;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::models::{
    ContainerCreateBody, ContainerSummaryStateEnum, HostConfig, ImageSummary, Mount, PortBinding,
    PortMap,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
    ListImagesOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptions,
    StopContainerOptionsBuilder,
};
use futures_util::TryStreamExt;
use postgres::{Client, NoTls};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tracing::{debug, info, warn};

pub struct Manager {
    docker: Docker,
    runtime: Runtime,
}

impl Manager {
    pub fn new() -> Result<Self, String> {
        let docker = Self::connect()?;
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|s| format!("docker {} ", s.to_string()))?;
        Ok(Manager { docker, runtime })
    }

    pub fn connect() -> Result<Docker, String> {
        Docker::connect_with_local_defaults().map_err(|e| e.to_string())
    }

    pub fn init_and_reset_container<S: AsRef<str>>(
        &self,
        name: S,
        mut container: Container,
    ) -> Result<(), String> {
        let contains = self
            .list_containers_by_name()?
            .contains(&name.as_ref().to_string());

        if contains {
            self.remove_container(name.as_ref())?;
        }

        self.create_container(name.as_ref(), &mut container)?;
        self.start_container(name.as_ref())?;
        container.wait_ready(name.as_ref(), self);
        container.after_start(name.as_ref(), self)?;
        Ok(container.wait_ready(name.as_ref(), self))
    }

    pub fn load_image(&self, image: &Image) -> Result<(), String> {
        let rt = &self.runtime;

        rt.block_on(async {
            let mut stream = self.docker.create_image(
                Some(
                    CreateImageOptionsBuilder::new()
                        .from_image(&image.image_name())
                        .build(),
                ),
                None,
                None,
            );

            while let Some(msg) = stream.try_next().await.unwrap() {
                if let Some(status) = msg.status {
                    info!("{}", status);
                }
            }
            Ok(())
        })
    }

    pub fn exec_command<S: AsRef<str>>(
        &self,
        name: S,
        command: Vec<String>,
    ) -> Result<Option<String>, String> {
        let rt = &self.runtime;

        rt.block_on(async {
            let exec_options = CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(command),
                privileged: Some(true),
                ..Default::default()
            };

            let exec_create_result = self
                .docker
                .create_exec(name.as_ref(), exec_options)
                .await
                .map_err(|e| e.to_string())?;
            let exec_id = exec_create_result.id;

            match self
                .docker
                .start_exec(
                    &exec_id,
                    Some(StartExecOptions {
                        detach: false,
                        tty: false,
                        output_capacity: Some(100_000),
                    }),
                )
                .await
                .map_err(|e| e.to_string())?
            {
                StartExecResults::Attached { mut output, .. } => {
                    let mut out = String::from("");
                    while let Some(log) = output.try_next().await.map_err(|e| e.to_string())? {
                        if let LogOutput::StdOut { message } = log {
                            out +=
                                &format!("Output from exec: {}", String::from_utf8_lossy(&message));
                        }
                    }
                    return Ok(Some(out));
                }
                other => warn!("other cmd: {:?}", other),
            }

            Ok(None)
        })
    }

    pub fn list_images(&self) -> Result<Vec<ImageSummary>, String> {
        let rt = &self.runtime;

        rt.block_on(async {
            self.docker
                .list_images(Some(ListImagesOptionsBuilder::default().all(true).build()))
                .await
                .map_err(|e| e.to_string())
        })
    }

    pub fn list_images_by_names(&self) -> Result<Vec<String>, String> {
        Ok(self.list_images()?.iter().map(|i| i.id.clone()).collect())
    }

    pub fn list_containers(&self) -> Result<Vec<ContainerSummary>, String> {
        let rt = &self.runtime;

        rt.block_on(async {
            let options = ListContainersOptionsBuilder::new().all(true).build();
            Ok(self
                .docker
                .list_containers(Some(options))
                .await
                .map_err(|err| err.to_string())?
                .iter()
                .map(ContainerSummary::from)
                .collect())
        })
    }

    pub fn list_containers_by_name(&self) -> Result<Vec<String>, String> {
        Ok(self
            .list_containers()?
            .iter()
            .map(|c| c.name.clone())
            .collect())
    }

    pub fn container_status<S: AsRef<str>>(&self, name: S) -> Option<ContainerSummary> {
        let containers = self.list_containers().unwrap();

        containers.into_iter().find(|c| c.name == name.as_ref())
    }

    pub fn create_container<S: AsRef<str>>(
        &self,
        name: S,
        container: &mut Container,
    ) -> Result<(), String> {
        // check used ports
        let ports: Vec<_> = self
            .list_containers()?
            .into_iter()
            .map(|c| c.ports)
            .flatten()
            .collect();

        info!("{:?}", ports);

        // check available images
        let has_image = self
            .list_images_by_names()?
            .contains(&container.image().image_name());

        if !has_image {
            // load image
            self.load_image(&container.image())?
        }

        let rt = &self.runtime;

        rt.block_on(async {
            // create container
            let options = Some(
                CreateContainerOptionsBuilder::new()
                    .name(name.as_ref())
                    .build(),
            );

            let host_config = Some(HostConfig {
                port_bindings: container.port_mappings(ports),
                mounts: container.mounts(),
                ..Default::default()
            });

            let config = ContainerCreateBody {
                image: Some(container.image().image_name()),
                env: container.env(),
                cmd: container.cmds(),
                hostname: Some(name.as_ref().to_string()),
                host_config,
                ..Default::default()
            };
            self.docker
                .create_container(options, config)
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        })
    }

    pub fn start_container(&self, name: &str) -> Result<(), String> {
        let rt = &self.runtime;

        rt.block_on(async {
            self.docker
                .start_container(name, None::<StartContainerOptions>)
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        })
    }

    pub fn stop_container(&self, name: &str) -> Result<(), String> {
        let rt = &self.runtime;
        rt.block_on(async {
            self.docker
                .stop_container(name, Some(StopContainerOptionsBuilder::default().build()))
                .await
                .map_err(|err| err.to_string())
        })
    }

    pub fn remove_container<S: AsRef<str>>(&self, name: S) -> Result<(), String> {
        let rt = &self.runtime;
        rt.block_on(async {
            self.docker
                .remove_container(
                    name.as_ref(),
                    Some(RemoveContainerOptionsBuilder::default().force(true).v(true).build()),
                )
                .await
                .map_err(|err| err.to_string())
        })
    }
}

#[derive(Debug)]
pub struct ContainerSummary {
    pub name: String,
    pub running: bool,
    pub ports: Vec<u16>,
}

impl ContainerSummary {}

impl From<&bollard::models::ContainerSummary> for ContainerSummary {
    fn from(c: &bollard::models::ContainerSummary) -> Self {
        ContainerSummary {
            name: c
                .names
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|w| w.chars().skip(1).collect())
                .collect::<Vec<String>>()
                .join(", "),
            ports: c
                .ports
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|p| p.private_port)
                .collect(),
            running: c
                .state
                .map(|c| matches!(c, ContainerSummaryStateEnum::RUNNING))
                .unwrap_or_default(),
        }
    }
}

pub enum Image {
    Postgres,
    Mongo,
}

impl Image {
    pub(crate) fn port(&self) -> u16 {
        match self {
            Image::Postgres => 5432,
            Image::Mongo => 27017,
        }
    }

    pub fn image_name(&self) -> String {
        match self {
            Image::Postgres => "postgres:17".to_string(),
            Image::Mongo => "mongo:8.0.12-noble".to_string(),
        }
    }
}

pub enum Container {
    Postgres(PostgresContainer),
    MongoDb(MongoDbContainer),
}

impl Container {
    pub(crate) fn after_start<S: AsRef<str>>(
        &self,
        name: S,
        manager: &Manager,
    ) -> Result<(), String> {
        match self {
            Postgres(p) => p.after_start(name, manager),
            MongoDb(m) => m.after_start(name, manager),
        }
    }

    pub fn postgres<S: AsRef<str>>(url: S, port: u16) -> Self {
        Postgres(PostgresContainer::new(url.as_ref(), port))
    }

    pub fn mongo_db<S: AsRef<str>>(url: S, port: u16) -> Self {
        MongoDb(MongoDbContainer::new(url.as_ref(), port))
    }

    fn image(&self) -> Image {
        match self {
            Postgres(p) => p.image(),
            MongoDb(m) => m.image(),
        }
    }

    pub(crate) fn env(&self) -> Option<Vec<String>> {
        match self {
            Postgres(p) => p.env(),
            MongoDb(m) => m.env(),
        }
    }

    fn port_mappings(&mut self, ports: Vec<u16>) -> Option<PortMap> {
        match self {
            Postgres(p) => p.port_mappings(ports),
            MongoDb(m) => m.port_mappings(ports),
        }
    }

    fn mounts(&self) -> Option<Vec<Mount>> {
        None
    }

    fn cmds(&self) -> Option<Vec<String>> {
        match self {
            Postgres(p) => p.cmds(),
            MongoDb(m) => m.cmds(),
        }
    }

    pub(crate) fn wait_ready<S: AsRef<str>>(&self, name: S, mgr: &Manager) {
        match self {
            Postgres(p) => p.wait_ready(),
            MongoDb(m) => m.wait_ready(name, mgr),
        }
    }
}

pub struct MongoDbContainer {
    pub url: String,
    pub port: u16,
    pub image: Image,
}

impl MongoDbContainer {
    pub fn new<S: AsRef<str>>(url: S, port: u16) -> Self {
        MongoDbContainer {
            url: url.as_ref().to_string(),
            port,
            image: Image::Mongo,
        }
    }

    pub(crate) fn cmds(&self) -> Option<Vec<String>> {
        Some(vec![
            "mongod".to_string(),
            "--replSet".to_string(),
            "repl".to_string(),
            "--bind_ip_all".to_string(),
            "--port".to_string(),
            self.image.port().to_string(),
        ])
    }

    pub(crate) fn after_start<S: AsRef<str>>(
        &self,
        name: S,
        manager: &Manager,
    ) -> Result<(), String> {
        let init_command = format!(
            "mongosh --eval \"rs.initiate({{ _id: '{}', members: [{{_id: 0, host: '{}:{}'}}] }})\"",
            "repl",
            "localhost",
            self.image.port()
        );

        let commands = vec!["sh".to_string(), "-c".to_string(), init_command];
        manager
            .exec_command(name, commands)
            .map(|o| o.map(|o| o.to_string()))
            // only need a response
            .and_then(|v| match v {
                None => Err("No response from exec".to_string()),
                Some(msg) => {
                    info!("response: {:?}", msg);
                    Ok(())
                }
            })
    }

    pub(crate) fn wait_ready<S: AsRef<str>>(&self, name: S, manager: &Manager) {
        let now = Instant::now();
        while !self.probe_running(name.as_ref(), manager) && now.elapsed().as_secs() < 3 * 60 {
            sleep(Duration::from_millis(100));
        }
    }

    pub(crate) fn probe_running<S: AsRef<str>>(&self, name: S, manager: &Manager) -> bool {
        let command = vec![
            "mongosh".to_string(),
            "--eval".to_string(),
            "db.runCommand({ ping: 1 })".to_string(),
        ];
        let output = match manager.exec_command(name.as_ref(), command) {
            Ok(m) => m,
            Err(_) => return false,
        };

        debug!("output: {:?}", output);
        output.is_some() && !output.unwrap().is_empty() // too early retrieves ""
    }

    pub(crate) fn port_mappings(&mut self, ports: Vec<u16>) -> Option<PortMap> {
        let mut map = PortMap::new();

        self.port = if ports.contains(&self.port) {
            let mut port = self.port;
            while ports.contains(&port) {
                port += 1;
            }
            port
        } else {
            self.port
        };

        map.insert(
            format!("{}/tcp", self.image.port()),
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: Some(self.port.to_string()),
            }]),
        );
        Some(map)
    }

    pub(crate) fn image(&self) -> Image {
        Image::Mongo
    }

    pub(crate) fn env(&self) -> Option<Vec<String>> {
        None
    }
}

pub struct PostgresContainer {
    pub url: String,
    pub port: u16,
    pub image: Image,
}

impl PostgresContainer {
    pub fn new<S: AsRef<str>>(url: S, port: u16) -> Self {
        PostgresContainer {
            url: url.as_ref().to_string(),
            port,
            image: Image::Postgres,
        }
    }

    pub(crate) fn after_start<S: AsRef<str>>(
        &self,
        name: S,
        manager: &Manager,
    ) -> Result<(), String> {
        // need apt-get instead of apt here
        let command = "apt-get update && apt-get install postgresql-17-wal2json -y".to_string();
        let command = vec!["sh".to_string(), "-c".to_string(), command.to_string()];

        match manager.exec_command(name.as_ref(), command) {
            Ok(res) => info!("{:?}", res),
            Err(err) => return Err(format!("No response from exec :{}", err)),
        }

        let content = r#"# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    all             pass_user       0.0.0.0/0            password
host    all             md5_user        0.0.0.0/0            md5
host    all             scram_user      0.0.0.0/0            scram-sha-256
host    all             pass_user       ::0/0                password
host    all             md5_user        ::0/0                md5
host    all             scram_user      ::0/0                scram-sha-256

# IPv4 local connections:
host    all             postgres        0.0.0.0/0            trust
host    replication     postgres        0.0.0.0/0            trust
# IPv6 local connections:
host    all             postgres        ::0/0                trust
# Unix socket connections:
local   all             postgres                             trust"#;

        let command = format!("echo '{}' > /var/lib/postgresql/data/pg_hba.conf", content);
        let command = vec!["sh".to_string(), "-c".to_string(), command];

        match manager.exec_command(name.as_ref(), command) {
            Ok(res) => info!("{:?}", res),
            Err(err) => return Err(format!("No response from exec :{}", err)),
        }

        manager.stop_container(name.as_ref())?;
        manager.start_container(name.as_ref())?;

        Ok(())
    }

    pub(crate) fn env(&self) -> Option<Vec<String>> {
        Some(vec![
            "POSTGRES_PASSWORD=postgres".to_string(),
            "POSTGRES_USER=postgres".to_string(),
        ])
    }

    pub(crate) fn cmds(&self) -> Option<Vec<String>> {
        Some(vec![
            "postgres".to_string(),
            "-c".to_string(),
            "wal_level=logical".to_string(),
            "-c".to_string(),
            "max_replication_slots=5".to_string(),
            "-c".to_string(),
            "max_wal_senders=5".to_string(),
            "-c".to_string(),
            "listen_addresses=*".to_string(),
        ])
    }

    pub fn image(&self) -> Image {
        Image::Postgres
    }

    pub fn wait_ready(&self) {
        let now = Instant::now();
        while !self.probe_running() && now.elapsed().as_secs() < 3 * 60 {
            sleep(Duration::from_millis(100));
        }
    }

    pub fn probe_running(&self) -> bool {
        let mut client = match Client::connect(
            &format!(
                "host={} port={} user=postgres password=postgres",
                self.url, self.port
            ),
            NoTls,
        ) {
            Ok(client) => client,
            Err(_) => return false,
        };
        client.query("SELECT 1;", &[]).is_ok()
    }

    pub fn port_mappings(&mut self, ports: Vec<u16>) -> Option<PortMap> {
        let mut map = PortMap::new();

        self.port = if ports.contains(&self.port) {
            let mut port = self.port;
            while ports.contains(&port) {
                port += 1;
            }
            port
        } else {
            self.port
        };

        map.insert(
            format!("{}/tcp", self.image.port()),
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: Some(self.port.to_string()),
            }]),
        );
        Some(map)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::util::container::{Container, Manager};
    use tracing_test::traced_test;

    #[test]
    fn test_start() {
        Manager::new().unwrap();
    }

    #[test]
    fn test_list_containers() {
        let mgr = Manager::new().unwrap();
        println!("{:?}", mgr.list_containers().unwrap())
    }

    #[test]
    fn test_start_container() {
        let mgr = create_container("test_start", 3252);
        mgr.start_container("test_start").unwrap();

        assert!(
            mgr.list_containers()
                .unwrap()
                .into_iter()
                .any(|c| c.running)
        );
        mgr.remove_container("test_start").unwrap()
    }

    #[test]
    fn test_create_container() {
        let mgr = create_container("test_create", 2353);
        assert!(
            mgr.list_containers()
                .unwrap()
                .into_iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .contains(&"test_create".to_string())
        );
        mgr.remove_container("test_create").unwrap()
    }

    fn create_container<S: AsRef<str>>(name: S, port: u16) -> Manager {
        let mgr = Manager::new().unwrap();

        let contains = mgr
            .list_containers_by_name()
            .unwrap()
            .contains(&name.as_ref().to_string());

        if contains {
            mgr.remove_container(name.as_ref()).unwrap();
        }

        assert!(
            !mgr.list_containers_by_name()
                .unwrap()
                .contains(&name.as_ref().to_string())
        );

        mgr.create_container(name, &mut Container::postgres("127.0.0.1", port))
            .unwrap();
        mgr
    }

    #[test]
    #[traced_test]
    fn init_container() {
        let mgr = Manager::new().unwrap();
        mgr.init_and_reset_container("init_container", Container::postgres("127.0.0.1", 2454))
            .unwrap();

        mgr.remove_container("init_container").unwrap()
    }
}
