use crate::management::Storage;
use crate::management::api::Status::Error;
use crate::management::permission::ApiPermission;
use flatbuffers::FlatBufferBuilder;
use std::sync::{Arc, Mutex};
use tracing::field::debug;
use tracing::{debug, info, warn};
use track_rails::message_generated::protocol;
use track_rails::message_generated::protocol::{
    BindRequest, BindRequestArgs, Catalog, CatalogArgs, CreatePlanRequest, CreatePlanResponse,
    CreatePlanResponseArgs, DeletePlanRequest, DeletePlanResponse, DeletePlanResponseArgs,
    ErrorStatus, ErrorStatusArgs, FilterType, GetPlansRequest, Message, MessageArgs, OkStatus,
    OkStatusArgs, Payload, Plans, PlansArgs, RegisterRequest, RegisterResponse,
    RegisterResponseArgs, StartPlanRequest, StartPlanResponse, StartPlanResponseArgs,
    Status as ProtStatus, StopPlanRequest, StopPlanResponse, StopPlanResponseArgs,
};

#[derive(Debug, Default)]
pub struct Api {
    clients: Vec<isize>,
    count: isize,
    permissions: Vec<ApiPermission>,
}

impl Api {
    pub(crate) fn admin() -> Api {
        Api {
            permissions: vec![ApiPermission::Admin],
            ..Default::default()
        }
    }

    pub fn handle_message(
        storage: Arc<Mutex<Storage>>,
        api: Arc<Mutex<Api>>,
        msg: Message,
    ) -> Result<Vec<u8>, Vec<u8>> {
        match msg.data_type() {
            Payload::NONE => {
                debug!("Received a NONE");
                Self::empty_msg()
            }
            Payload::CreatePlanRequest => {
                info!("Received a CREATE PLAN");
                handle_create_plan(&storage, msg.data_as_create_plan_request().unwrap())
            }
            Payload::DeletePlanRequest => {
                debug("Received a DELETE PLAN");
                handle_delete_plan(&storage, msg.data_as_delete_plan_request().unwrap())
            }
            Payload::RegisterRequest => {
                debug!("Received a REGISTER");
                handle_register(msg.data_as_register_request().unwrap(), storage, api)
            }
            Payload::StartPlanRequest => {
                debug!("Received a START PLAN");
                handle_start_plan(msg.data_as_start_plan_request().unwrap(), storage, api)
            }
            Payload::StopPlanRequest => {
                debug!("Received a STOP PLAN");
                handle_stop_plan(msg.data_as_stop_plan_request().unwrap(), storage, api)
            }
            Payload::GetPlansRequest => {
                debug!("Received a GET");
                handle_get_plans(&storage, msg.data_as_get_plans_request().unwrap())
            }
            Payload::GetPlanRequest => {
                todo!()
            }
            Payload::Train => {
                debug!("Received a Train");
                let _values = msg.data_as_train().unwrap();
                todo!()
            }
            Payload::BindRequest => match msg.data_as_bind_request() {
                None => {
                    warn!("Received a BIND request");
                    build_status_response(Error(String::from("Incorrect Request")))
                }
                Some(b) => {
                    let mut storage = storage.lock().unwrap();
                    let (data_port, watermark_port) =
                        storage.attach(usize::MAX, b.plan_id() as usize, b.stop_id() as usize)?;
                    drop(storage);
                    Self::build_bind_response(data_port as usize, watermark_port as usize)
                }
            },
            Payload::UnbindRequest => match msg.data_as_unbind_request() {
                None => build_status_response(Status::Error(String::from("Incorrect Request"))),
                Some(u) => {
                    let mut storage = storage.lock().unwrap();
                    storage.detach(0, u.plan_id() as usize, u.stop_id() as usize);
                    drop(storage);
                    Self::empty_msg()
                }
            },
            _ => build_status_response(Status::Error(String::from("Invalid Request"))),
        }
    }

    fn empty_msg<'a>() -> Result<Vec<u8>, Vec<u8>> {
        build_status_response(Status::Error("Empty message".to_string()))
    }

    fn build_bind_response(data_port: usize, watermark_port: usize) -> Result<Vec<u8>, Vec<u8>> {
        let mut builder = FlatBufferBuilder::new();

        let bind = BindRequest::create(
            &mut builder,
            &BindRequestArgs {
                plan_id: data_port as u64,
                stop_id: watermark_port as u64,
            },
        );

        let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

        let msg = Message::create(
            &mut builder,
            &MessageArgs {
                data_type: Payload::BindRequest,
                data: Some(bind.as_union_value()),
                status_type: ProtStatus::OkStatus,
                status: Some(status),
            },
        );
        builder.finish(msg, None);
        Ok(builder.finished_data().to_vec())
    }
}

fn handle_stop_plan(
    rx: StopPlanRequest,
    storage: Arc<Mutex<Storage>>,
    _api: Arc<Mutex<Api>>,
) -> Result<Vec<u8>, Vec<u8>> {
    let id = rx.id() as usize;
    let mut storage = storage.lock().unwrap();
    storage.stop_plan(id);

    let mut builder = FlatBufferBuilder::new();

    let start = StopPlanResponse::create(
        &mut builder,
        &StopPlanResponseArgs {
            already_stopped: false,
        },
    );
    let status = OkStatus::create(&mut builder, &OkStatusArgs {});

    let msg = Message::create(
        &mut builder,
        &MessageArgs {
            data_type: Payload::StopPlanResponse,
            data: Some(start.as_union_value()),
            status_type: ProtStatus::OkStatus,
            status: Some(status.as_union_value()),
        },
    );

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}

fn handle_start_plan(
    rx: StartPlanRequest,
    storage: Arc<Mutex<Storage>>,
    _api: Arc<Mutex<Api>>,
) -> Result<Vec<u8>, Vec<u8>> {
    let id = rx.id() as usize;
    let mut storage = storage.lock().unwrap();
    storage.start_plan(id);

    let mut builder = FlatBufferBuilder::new();

    let start = StartPlanResponse::create(
        &mut builder,
        &StartPlanResponseArgs {
            already_running: false,
        },
    );
    let status = OkStatus::create(&mut builder, &OkStatusArgs {});

    let msg = Message::create(
        &mut builder,
        &MessageArgs {
            data_type: Payload::StartPlanResponse,
            data: Some(start.as_union_value()),
            status_type: ProtStatus::OkStatus,
            status: Some(status.as_union_value()),
        },
    );

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}

fn handle_get_plans(
    storage: &Arc<Mutex<Storage>>,
    rx: GetPlansRequest,
) -> Result<Vec<u8>, Vec<u8>> {
    let filter = rx.name().unwrap();
    match filter.filter_type_type() {
        FilterType(_) => {
            let by_name = filter.filter_type_as_by_name().unwrap();

            let plans = storage
                .lock()
                .unwrap()
                .get_plans_by_name(by_name.name().unwrap_or("*"));

            let mut builder = FlatBufferBuilder::new();
            let plans = plans
                .into_iter()
                .map(|p| p.flatterize(&mut builder))
                .collect::<Vec<_>>();
            let plans = builder.create_vector(&plans);

            let plans = Plans::create(&mut builder, &PlansArgs { plans: Some(plans) });

            let catalog = Catalog::create(&mut builder, &CatalogArgs { plans: Some(plans) });

            let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

            let msg = Message::create(
                &mut builder,
                &MessageArgs {
                    data_type: Payload::Catalog,
                    data: Some(catalog.as_union_value()),
                    status_type: ProtStatus::OkStatus,
                    status: Some(status),
                },
            );

            builder.finish(msg, None);
            Ok(builder.finished_data().to_vec())
        }
    }
}

fn handle_create_plan(
    storage: &Arc<Mutex<Storage>>,
    create: CreatePlanRequest,
) -> Result<Vec<u8>, Vec<u8>> {
    match storage.lock().unwrap().create_plan(create) {
        Ok(id) => {
            let mut builder = FlatBufferBuilder::new();

            let create =
                CreatePlanResponse::create(&mut builder, &CreatePlanResponseArgs { id: id as u64 })
                    .as_union_value();

            let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

            let message = protocol::Message::create(
                &mut builder,
                &MessageArgs {
                    data: Some(create),
                    status_type: ProtStatus::OkStatus,
                    data_type: Payload::CreatePlanResponse,
                    status: Some(status),
                },
            );

            builder.finish(message, None);
            Ok(builder.finished_data().to_vec())
        }
        Err(err) => build_status_response(Error(err)),
    }
}

fn build_status_response(status: Status) -> Result<Vec<u8>, Vec<u8>> {
    let mut builder = FlatBufferBuilder::new();

    let msg = match status {
        Status::Ok => {
            let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();
            Message::create(
                &mut builder,
                &MessageArgs {
                    data_type: Default::default(),
                    data: None,
                    status_type: ProtStatus::OkStatus,
                    status: Some(status),
                },
            )
        }
        Status::Error(err) => {
            let msg = builder.create_string(&err);
            let status = ErrorStatus::create(
                &mut builder,
                &ErrorStatusArgs {
                    code: 0,
                    msg: Some(msg),
                },
            )
            .as_union_value();
            Message::create(
                &mut builder,
                &MessageArgs {
                    data_type: Default::default(),
                    data: None,
                    status_type: ProtStatus::ErrorStatus,
                    status: Some(status),
                },
            )
        }
    };

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}

fn handle_delete_plan(
    storage: &Arc<Mutex<Storage>>,
    rx: DeletePlanRequest,
) -> Result<Vec<u8>, Vec<u8>> {
    match storage.lock().unwrap().delete_plan(rx.id() as usize) {
        Ok(_) => {
            let mut builder = FlatBufferBuilder::new();

            let create = DeletePlanResponse::create(&mut builder, &DeletePlanResponseArgs {})
                .as_union_value();

            let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

            let message = Message::create(
                &mut builder,
                &MessageArgs {
                    data: Some(create),
                    status_type: ProtStatus::OkStatus,
                    data_type: Payload::DeletePlanResponse,
                    status: Some(status),
                },
            );

            builder.finish(message, None);
            Ok(builder.finished_data().to_vec())
        }
        Err(err) => build_status_response(Error(err)),
    }
}

fn handle_register(
    _request: RegisterRequest,
    storage: Arc<Mutex<Storage>>,
    api: Arc<Mutex<Api>>,
) -> Result<Vec<u8>, Vec<u8>> {
    let mut api = api.lock().unwrap();
    let id = api.count;
    api.count += 1;
    api.clients.push(id);
    let mut builder = FlatBufferBuilder::new();

    let storage = storage.lock().unwrap();
    let plans = storage
        .plans
        .lock()
        .unwrap()
        .values()
        .map(|plan| plan.flatterize(&mut builder))
        .collect::<Vec<_>>();

    let plans = builder.create_vector(&plans);

    let plans = Plans::create(&mut builder, &PlansArgs { plans: Some(plans) });
    let catalog = Catalog::create(&mut builder, &CatalogArgs { plans: Some(plans) });

    let permissions = api
        .permissions
        .iter()
        .map(|p| {
            let str = p.to_string();
            builder.create_string(&str)
        })
        .collect::<Vec<_>>();

    let permissions = builder.create_vector(&permissions);

    let register = RegisterResponse::create(
        &mut builder,
        &RegisterResponseArgs {
            id: Some(id as u64),
            permissions: Some(permissions),
            catalog: Some(catalog),
        },
    )
    .as_union_value();

    let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

    let msg = Message::create(
        &mut builder,
        &MessageArgs {
            data_type: Payload::RegisterResponse,
            data: Some(register),
            status_type: ProtStatus::OkStatus,
            status: Some(status),
        },
    );

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}

enum Status {
    Ok,
    Error(String),
}
