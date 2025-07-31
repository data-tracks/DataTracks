use crate::util::container::Container::Postgres;
use bollard::Docker;
use bollard::models::{ContainerCreateBody, ContainerSummaryStateEnum, HostConfig, ImageSummary, Mount, MountPoint, MountPointTypeEnum, MountTypeEnum, PortBinding, PortMap};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
    ListImagesOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptions,
    StopContainerOptionsBuilder,
};
use futures_util::TryStreamExt;
use postgres::{Client, NoTls};
use std::thread::sleep;
use std::time::{Duration, Instant};
use postgres::types::IsNull::No;
use tokio::runtime::Runtime;
use tracing::info;

pub struct Manager {
    docker: Docker,
}

impl Manager {
    pub fn new() -> Result<Self, String> {
        let docker = Self::connect()?;
        Ok(Manager { docker })
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
        container.wait_ready();

        Ok(())
    }

    pub fn load_image(&self, image: &Image) -> Result<(), String> {
        let rt = Runtime::new().map_err(|e| e.to_string())?;

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

    pub fn list_images(&self) -> Result<Vec<ImageSummary>, String> {
        let rt = Runtime::new().map_err(|e| e.to_string())?;

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
        let rt = Runtime::new().map_err(|e| e.to_string())?;
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

        let rt = Runtime::new().map_err(|e| e.to_string())?;

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
        let rt = Runtime::new().map_err(|e| e.to_string())?;

        rt.block_on(async {
            self.docker
                .start_container(name, None::<StartContainerOptions>)
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        })
    }

    pub fn stop_container(&self, name: &str) -> Result<(), String> {
        let rt = Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(async {
            self.docker
                .stop_container(name, Some(StopContainerOptionsBuilder::default().build()))
                .await
                .map_err(|err| err.to_string())
        })
    }

    pub fn remove_container<S: AsRef<str>>(&self, name: S) -> Result<(), String> {
        let rt = Runtime::new().map_err(|e| e.to_string())?;
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
}

impl Image {
    pub fn image_name(&self) -> String {
        match self {
            Image::Postgres => "postgres:14.18".to_string(),
        }
    }
}

pub enum Container {
    Postgres(PostgresContainer),
}

impl Container {
    pub fn postgres() -> Self {
        Postgres(PostgresContainer::new())
    }

    fn image(&self) -> Image {
        match self {
            Postgres(p) => p.image(),
        }
    }

    pub(crate) fn env(&self) -> Option<Vec<String>> {
        Some(vec![
            "POSTGRES_PASSWORD=postgres".to_string(),
            "POSTGRES_USER=postgres".to_string(),
        ])
    }

    fn port_mappings(&self) -> Option<PortMap> {
        match self {
            Postgres(p) => p.port_mappings(),
        }
    }

    fn mounts(&self) -> Option<Vec<Mount>>{
        None
    }

    fn cmds(&self) -> Option<Vec<String>> {
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

    pub(crate) fn wait_ready(&self) {
        match self {
            Postgres(p) => p.wait_ready(),
        }
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
