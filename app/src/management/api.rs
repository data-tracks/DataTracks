use crate::management::api::Status::Error;
use flatbuffers::FlatBufferBuilder;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};
use track_rails::message_generated::protocol::{
    BindRequest, BindRequestArgs, Catalog, CatalogArgs, CreatePlanRequest, CreatePlanResponse,
    CreatePlanResponseArgs, DeletePlanRequest, DeletePlanResponse, DeletePlanResponseArgs,
    ErrorStatus, ErrorStatusArgs, FilterType, GetPlansRequest, Message, MessageArgs, OkStatus,
    OkStatusArgs, Payload, Plans, PlansArgs, RegisterRequest, RegisterResponse,
    RegisterResponseArgs, StartPlanRequest, StartPlanResponse, StartPlanResponseArgs,
    Status as ProtStatus, StopPlanRequest, StopPlanResponse, StopPlanResponseArgs,
};
use error::error::TrackError;

#[derive(Debug, Default)]
pub struct Api {
    clients: Vec<isize>,
    count: isize,
}

impl Api {
    pub(crate) fn admin() -> Api {
        Api {
            ..Default::default()
        }
    }

    pub fn handle_message(
        api: Arc<Mutex<Api>>,
        msg: Message,
    ) -> Result<Vec<u8>, Vec<u8>> {
        match msg.data_type() {
            Payload::NONE => {
                debug!("Received a NONE");
                Self::empty_msg()
            }
            Payload::GetPlanRequest => {
                debug!("Received a GET plan");
                todo!()
            }
            Payload::Train => {
                debug!("Received a Train");
                let _values = msg.data_as_train().unwrap();
                todo!()
            }
            _ => build_status_response(Error(String::from("Invalid Request"))),
        }
    }

    fn empty_msg() -> Result<Vec<u8>, Vec<u8>> {
        build_status_response(Error("Empty message".to_string()))
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
        Error(err) => {
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


enum Status {
    Ok,
    Error(String),
}
