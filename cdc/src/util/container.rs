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
use tracing::info;

pub struct Manager {
    docker: Docker,
    runtime: Runtime,
}

impl Manager {
    pub fn new() -> Result<Self, String> {
        let docker = Self::connect()?;
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|s| s.to_string())?;
        Ok(Manager { docker, runtime })
    }

    pub fn connect() -> Result<Docker, String> {
        Docker::connect_with_local_defaults().map_err(|e| e.to_string())
    }

    pub fn init_and_reset_container<S: AsRef<str>>(
        &self,
        name: S,
        container: Container,
    ) -> Result<(), String> {
        let contains = self
            .list_containers_by_name()?
            .contains(&name.as_ref().to_string());

        if contains {
            self.remove_container(name.as_ref())?;
        }

        self.create_container(name.as_ref(), &container)?;
        self.start_container(name.as_ref())?;
        container.wait_ready(name.as_ref(), self);
        container.after_start(name.as_ref(), self)
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
                .start_exec(&exec_id, Some(StartExecOptions::default()))
                .await
                .map_err(|e| e.to_string())?
            {
                StartExecResults::Attached { mut output, .. } => {
                    while let Some(log) = output.try_next().await.map_err(|e| e.to_string())? {
                        let mut out = String::from("");
                        if let LogOutput::StdOut { message } = log {
                            out +=
                                &format!("Output from exec: {}", String::from_utf8_lossy(&message));
                        }
                        return Ok(Some(out));
                    }
                }
                _ => {}
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
        container: &Container,
    ) -> Result<(), String> {
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
                port_bindings: container.port_mappings(),
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
                    Some(RemoveContainerOptionsBuilder::default().force(true).build()),
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
}

impl ContainerSummary {}

impl From<&bollard::models::ContainerSummary> for ContainerSummary {
    fn from(c: &bollard::models::ContainerSummary) -> Self {
        println!("{:?}", c);
        ContainerSummary {
            name: c
                .names
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|w| w.chars().skip(1).collect())
                .collect::<Vec<String>>()
                .join(", "),
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
    pub fn image_name(&self) -> String {
        match self {
            Image::Postgres => "postgres:14.18".to_string(),
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

    pub fn postgres() -> Self {
        Postgres(PostgresContainer::new())
    }

    pub fn mongo_db() -> Self {
        MongoDb(MongoDbContainer::new())
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

    fn port_mappings(&self) -> Option<PortMap> {
        match self {
            Postgres(p) => p.port_mappings(),
            MongoDb(m) => m.port_mappings(),
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
    pub version: String,
    pub url: String,
    pub port: usize,
}

impl MongoDbContainer {
    pub fn new() -> Self {
        MongoDbContainer {
            version: "8.0.12-noble".to_string(),
            url: "127.0.0.1".to_string(),
            port: 27017,
        }
    }

    pub(crate) fn cmds(&self) -> Option<Vec<String>> {
        Some(vec![
            "mongod".to_string(),
            "--replSet".to_string(),
            "repl".to_string(),
            "--bind_ip_all".to_string(),
            "--port".to_string(),
            self.port.to_string(),
        ])
    }

    pub(crate) fn after_start<S: AsRef<str>>(
        &self,
        name: S,
        manager: &Manager,
    ) -> Result<(), String> {
        let init_command = format!(
            "mongosh --eval \"rs.initiate({{ _id: '{}', members: [{{_id: 0, host: '{}:{}'}}] }})\"",
            "repl", "localhost", self.port
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
                },
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
        manager
            .exec_command(name.as_ref(), command)
            .iter()
            .flatten()
            .next()
            .is_some()
    }

    pub(crate) fn port_mappings(&self) -> Option<PortMap> {
        let mut map = PortMap::new();

        map.insert(
            format!("{}/tcp", self.port),
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
    pub version: String,
    pub url: String,
    pub port: usize,
}

impl PostgresContainer {
    pub fn new() -> Self {
        PostgresContainer {
            version: "postgres:14.18".to_string(),
            url: "127.0.0.1".to_string(),
            port: 5432,
        }
    }

    pub(crate) fn after_start<S: AsRef<str>>(
        &self,
        _name: S,
        _manager: &Manager,
    ) -> Result<(), String> {
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
        Client::connect(
            &format!(
                "host=postgresql://{}@{}:{}, password=postgres",
                "postgres", self.url, self.port
            ),
            NoTls,
        )
        .is_ok()
    }

    pub fn port_mappings(&self) -> Option<PortMap> {
        let mut map = PortMap::new();

        map.insert(
            format!("{}/tcp", self.port),
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
        let mgr = create_container("test");
        mgr.start_container("test").unwrap();

        assert!(
            mgr.list_containers()
                .unwrap()
                .into_iter()
                .any(|c| c.running)
        )
    }

    #[test]
    fn test_create_container() {
        let mgr = create_container("test");
        assert!(
            mgr.list_containers()
                .unwrap()
                .into_iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .contains(&"test".to_string())
        )
    }

    fn create_container<S: AsRef<str>>(name: S) -> Manager {
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

        mgr.create_container(name, &Container::postgres()).unwrap();
        mgr
    }

    #[test]
    fn init_container() {
        let mgr = Manager::new().unwrap();
        mgr.init_and_reset_container("test", Container::postgres())
            .unwrap();
    }
}
