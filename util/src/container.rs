use std::collections::HashMap;
use std::error::Error;
use bollard::Docker;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::models::{
    ContainerCreateBody, ContainerSummaryStateEnum, HostConfig, ImageSummary, Mount, PortBinding,
    PortMap,
};
use bollard::query_parameters::{CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder, ListImagesOptionsBuilder, RemoveContainerOptions, RemoveContainerOptionsBuilder, StartContainerOptions, StopContainerOptions, StopContainerOptionsBuilder};
use futures_util::TryStreamExt;

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

    pub fn load_image(&self, image_name: &str) -> Result<(), String> {
        let rt = &self.runtime;

        rt.block_on(async {
            let mut stream = self.docker.create_image(
                Some(
                    CreateImageOptionsBuilder::new()
                        .from_image(image_name)
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

pub struct Mapping{
    pub container: u16,
    pub host: u16
}

pub async fn start_container(
    name: &str,
    image: &str,
    mappings: Vec<Mapping>,
    env_vars: Option<Vec<String>>,
) -> Result<(), Box<dyn Error>> {
    let docker = match Manager::connect() {
        Ok(d) => d,
        Err(e) => {
            return if e.contains("HyperLegacyError") {
                Err(Box::from("Docker is probably not running"))
            } else {
                Err(Box::from(e))
            }
        }
    };

    let option = ListContainersOptionsBuilder::new().all(true).build();

    let list = docker.list_containers(Some(option)).await?;

    if list.into_iter().any(|c| {
        c.names
            .unwrap()
            .first()
            .into_iter()
            .any(|n| n.to_lowercase() == format!("/{}",name))
    }) {
        stop(name).await?;
    }

    let options = CreateImageOptionsBuilder::new().from_image(image).build();

    let mut status = docker.create_image(Some(options), None, None);

    while let Some(msg) = status.try_next().await? {
        if let Some(status) = msg.status {
            info!("Pull Status: {}", status);
        }
    }

    let mut exposed_ports = HashMap::new();
    let mut port_bindings = HashMap::new();

    for mapping in mappings {
        // 1. Define the specific Host Port and IP
        let binding = PortBinding {
            // Listen on all host interfaces
            host_ip: Some("127.0.0.1".to_string()),
            // Map to host port 5432
            host_port: Some(mapping.host.to_string()),
        };

        // 2. Create the HostConfig's PortBindings map
        port_bindings.insert(
            format!("{}/{}", mapping.host.to_string(), "tcp"),
            Some(vec![binding]),
        );

        // 4. Create the ExposedPorts map for the container config
        exposed_ports.insert(
            format!("{}/{}", mapping.container.to_string(), "tcp"),
            HashMap::new(),
        );
    }

    // 3. Create the HostConfig
    let host_config = HostConfig {
        port_bindings: Some(port_bindings),
        ..Default::default()
    };


    let options = CreateContainerOptionsBuilder::new().name(name).build();

    let config = ContainerCreateBody {
        image: Some(image.to_string()),
        exposed_ports: Some(exposed_ports),
        host_config: Some(host_config),
        env: env_vars,
        ..Default::default()
    };

    docker.create_container(Some(options), config).await?;

    docker
        .start_container(name, None::<StartContainerOptions>)
        .await?;
    Ok(())
}

pub async fn stop(name: &str) -> Result<(), Box<dyn Error>> {
    let docker = Manager::connect()?;
    docker
        .stop_container(name, None::<StopContainerOptions>)
        .await?;
    docker
        .remove_container(name, None::<RemoveContainerOptions>)
        .await?;
    info!("⏸️ Stopped container {}", name);
    Ok(())
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


