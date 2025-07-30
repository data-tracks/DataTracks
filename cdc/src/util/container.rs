use bollard::Docker;
use bollard::models::{ContainerCreateBody, ContainerSummaryStateEnum};
use bollard::query_parameters::{
    CreateContainerOptions, ListContainersOptionsBuilder, StartContainerOptions,
};
use tokio::runtime::Runtime;

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

    pub fn create_container(&self, name: String, images: Images) -> Result<(), String> {
        let rt = Runtime::new().map_err(|e| e.to_string())?;

        rt.block_on(async {
            let options = CreateContainerOptions {
                name: Some(name),
                platform: "".to_string(),
            };
            let config = ContainerCreateBody {
                image: Some(images.image_name()),
                ..Default::default()
            };
            self.docker
                .create_container(Some(options), config)
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
}

#[derive(Debug)]
pub struct ContainerSummary {
    name: String,
    running: bool,
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

pub enum Images {
    Postgres,
}

impl Images {
    pub fn image_name(&self) -> String {
        match self {
            Images::Postgres => "postgres".into(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::util::container::{Images, Manager};

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
    fn test_create_container() {
        let mgr = Manager::new().unwrap();
        mgr.create_container("test".to_string(), Images::Postgres)
            .unwrap();

        assert!(
            mgr.list_containers()
                .unwrap()
                .into_iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .contains(&"test".to_string())
        )
    }
}
