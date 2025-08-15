use flatbuffers::InvalidFlatbuffer;
use track_rails::message_generated::protocol::Message;

pub fn deserialize_message(buf: &[u8]) -> Result<Message<'_>, String> {
    flatbuffers::root::<Message>(buf).map_err(|e| match e {
        InvalidFlatbuffer::MissingRequiredField {
            required,
            error_trace,
        } => format!("missing required field {required}, trace {error_trace}"),
        InvalidFlatbuffer::InconsistentUnion {
            field,
            field_type,
            error_trace: _error_trace,
        } => format!("inconsistent field type {field_type}: {field}"),
        _ => format!("{e:?}"),
    })
}
