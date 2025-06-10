use crate::management::Storage;
use flatbuffers::FlatBufferBuilder;
use schemas::message_generated::protocol::{
    BindRequest, BindRequestArgs, Catalog, CatalogArgs, CreateType, GetType, Message, MessageArgs,
    Payload, Plans, PlansArgs, RegisterRequest, RegisterRequestArgs, Status, StatusArgs,
};
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

#[derive(Debug, Default)]
pub struct Api {
    clients: Vec<isize>,
    count: isize,
}

impl Api {
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
            Payload::Create => {
                debug!("Received a CREATE");
                let create = msg.data_as_create().unwrap();
                match create.create_type_type() {
                    CreateType::NONE => {
                        info!("Received a NONE");
                        Self::build_status_response("No response".to_string())
                    }
                    CreateType(2_u8..=u8::MAX) => todo!(),
                    CreateType::CreatePlanRequest => {
                        info!("Received a CREATE PLAN");
                        match storage.lock().unwrap().create_plan(create) {
                            Ok(_) => Self::build_status_response("Created plan".to_string()),
                            Err(err) => Self::build_status_response(err),
                        }
                    }
                }
            }
            Payload::RegisterRequest => {
                debug!("Received a REGISTER");

                handle_register(msg.data_as_register_request().unwrap(), storage, api)
            }
            Payload::Get => {
                debug!("Received a GET");
                let get = msg.data_as_get().unwrap();

                match get.get_type_type() {
                    GetType::NONE => Self::empty_msg(),
                    GetType(3_u8..=u8::MAX) => todo!(),
                    GetType::GetPlans => {
                        todo!()
                    }
                    GetType::GetPlan => {
                        todo!()
                    }
                }
            }
            Payload::Train => {
                debug!("Received a Train");
                let _values = msg.data_as_train().unwrap();
                todo!()
            }
            Payload::BindRequest => match msg.data_as_bind_request() {
                None => todo!(),
                Some(b) => {
                    let mut storage = storage.lock().unwrap();
                    let (data_port, watermark_port) =
                        storage.attach(usize::MAX, b.plan_id() as usize, b.stop_id() as usize)?;
                    drop(storage);
                    Self::build_bind_response(data_port as usize, watermark_port as usize)
                }
            },
            Payload::UnbindRequest => match msg.data_as_unbind_request() {
                None => todo!(),
                Some(u) => {
                    let mut storage = storage.lock().unwrap();
                    storage.detach(0, u.plan_id() as usize, u.stop_id() as usize);
                    drop(storage);
                    Self::empty_msg()
                }
            },
            Payload(4_u8..=u8::MAX) => todo!(),
        }
    }

    fn empty_msg<'a>() -> Result<Vec<u8>, Vec<u8>> {
        Self::build_status_response("Empty message".to_string())
    }

    fn build_status_response(status: String) -> Result<Vec<u8>, Vec<u8>> {
        let mut builder = FlatBufferBuilder::new();
        let status = builder.create_string(&status);

        let status = Status::create(&mut builder, &StatusArgs { msg: Some(status) });
        let msg = Message::create(
            &mut builder,
            &MessageArgs {
                data_type: Default::default(),
                data: None,
                status: Some(status),
            },
        );
        builder.finish(msg, None);
        Ok(builder.finished_data().to_vec())
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
        let msg = Message::create(
            &mut builder,
            &MessageArgs {
                data_type: Payload::BindRequest,
                data: Some(bind.as_union_value()),
                status: None,
            },
        );
        builder.finish(msg, None);
        Ok(builder.finished_data().to_vec())
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

    let register = RegisterRequest::create(
        &mut builder,
        &RegisterRequestArgs {
            id: Some(id as u64),
            catalog: Some(catalog),
        },
    )
    .as_union_value();

    let msg = Message::create(
        &mut builder,
        &MessageArgs {
            data_type: Payload::RegisterRequest,
            data: Some(register),
            status: None,
        },
    );

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}
