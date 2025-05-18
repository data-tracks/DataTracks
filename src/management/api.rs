use std::sync::{Arc, Mutex};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset};
use schemas::message_generated;
use schemas::message_generated::protocol::{Catalog, CatalogArgs, CreateType, GetType, Message, MessageArgs, Payload, Plans, PlansArgs, Register, RegisterArgs, Status, StatusArgs, Plan as FlatPlan, PlanArgs, Bind, Unbind};
use tracing::{debug, info};
use crate::management::{Storage};
use crate::processing::Plan;

#[derive(Debug, Default)]
pub struct API{
    clients: Vec<isize>,
    count: isize
}


impl API {
    pub fn handle_message( storage: Arc<Mutex<Storage>>, api: Arc<Mutex<API>>, msg: Message) -> Result<Vec<u8>, Vec<u8>> {
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
                    CreateType::CreatePlan => {
                        info!("Received a CREATE PLAN");
                        match storage.lock().unwrap().create_plan(create) {
                            Ok(_) => Self::build_status_response("Created plan".to_string()),
                            Err(err) => Self::build_status_response(err)
                        }
                    }
                }
            }
            Payload::Register => {
                debug!("Received a REGISTER");

                handle_register(msg.data_as_register().unwrap(), storage, api)
            }
            Payload::Get => {
                debug!("Received a GET");
                let get = msg.data_as_get().unwrap();

                match get.get_type_type() {
                    GetType::NONE => {
                        Self::empty_msg()
                    },
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
                let values = msg.data_as_train().unwrap();
                todo!()
            }
            Payload::Bind => {
                match msg.data_as_bind() {
                    None => todo!(),
                    Some(b) => {
                        let mut storage = storage.lock().unwrap();
                        storage.attach(0, b.planId() as usize, b.stopId() as usize);
                        drop(storage);
                        Self::empty_msg()
                    }
                }
            },
            Payload::Unbind => {
                match msg.data_as_unbind() {
                    None => todo!(),
                    Some(u) => {
                        let mut storage = storage.lock().unwrap();
                        storage.detach(0);
                        drop(storage);
                        Self::empty_msg()
                    }
                }
            }
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
        let msg = Message::create(&mut builder, &MessageArgs {
            data_type: Default::default(),
            data: None,
            status: Some(status),
        });
        builder.finish(msg, None);
        Ok(builder.finished_data().to_vec())

    }
}

fn handle_register(_request: Register, storage: Arc<Mutex<Storage>>, api: Arc<Mutex<API>>) -> Result<Vec<u8>, Vec<u8>> {
    let mut api = api.lock().unwrap();
    let id = api.count;
    api.count += 1;
    api.clients.push(id);
    let mut builder = FlatBufferBuilder::new();

    let storage = storage.lock().unwrap();
    let plans = storage.plans.lock().unwrap().values().map(|plan| {
        let name = builder.create_string(&plan.name);
        let template = builder.create_string(&plan.dump(false));

        FlatPlan::create(&mut builder, &PlanArgs { name: Some(name), template: Some(template) })
    }).collect::<Vec<_>>();

    let plans = builder.create_vector(&plans);

    let plans = Plans::create(&mut builder, &PlansArgs { plans: Some(plans) });
    let catalog = Catalog::create(&mut builder, &CatalogArgs { plans: Some(plans), ..Default::default() });

    let register = Register::create(&mut builder, &RegisterArgs { id: Some(id as i64), catalog: Some(catalog), ..Default::default() }).as_union_value();

    let msg = Message::create(&mut builder, &MessageArgs{data_type: Payload::Register, data: Some(register), status: None });

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}

fn serialize_plans<'a>(builder: &'a mut FlatBufferBuilder<'a>, plan: &'a Plan) -> ForwardsUOffset<message_generated::protocol::Plan<'a>> {
    todo!()
}


