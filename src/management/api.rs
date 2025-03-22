use std::sync::{Arc, Mutex};
use axum::http::StatusCode;
use flatbuffers::{FlatBufferBuilder};
use schemas::message_generated::protocol::{CreateType, GetType, Message, MessageArgs, MessageBuilder, Payload, Status, StatusArgs, StringArgs};
use tracing::{debug, info};
use url::quirks::protocol;
use crate::management::{Manager, Storage};
use crate::processing::Plan;

pub struct API{
}


impl API {
    pub fn handle_message<'a>( storage: Arc<Mutex<Storage>>, msg: Message) -> Result<Vec<u8>, Vec<u8>> {
        match msg.data_type() {
            Payload::NONE => {
                info!("Received a NONE");
                Self::empty_msg()
            }
            Payload::Create => {
                info!("Received a CREATE");
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
                info!("Received a REGISTER");

                Self::build_status_response("Sent catalog".to_string())
            }
            Payload::Get => {
                info!("Received a GET");
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
            Payload::Values => {
                info!("Received a VALUES");
                let values = msg.data_as_values().unwrap();
                todo!()
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
