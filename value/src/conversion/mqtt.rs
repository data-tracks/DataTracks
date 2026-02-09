use core::str;
use std::collections::BTreeMap;
use rumqttd::Notification;
use anyhow::{anyhow, bail};
use rumqttd::protocol::Publish;
use rumqttc::{Event, Incoming};
use crate::{Dict};
use crate::value::Value;

impl TryFrom<Notification> for Dict {
    type Error = anyhow::Error;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        match value {
            Notification::Forward(f) => f.publish.try_into(),
            _ => bail!("Unexpected notification {value:?}"),
        }
    }
}

impl TryFrom<Publish> for Dict {
    type Error = anyhow::Error;

    fn try_from(publish: Publish) -> Result<Self, Self::Error> {
        let mut dict = BTreeMap::new();
        let value = str::from_utf8(&publish.payload)
            .map_err(|e| anyhow!(e))?
            .into();
        let topic = str::from_utf8(&publish.topic)
            .map_err(|e| anyhow!(e))?
            .into();
        dict.insert("$".to_string(), value);
        dict.insert("$topic".to_string(), topic);
        Ok(Value::dict(dict).into())
    }
}

impl TryFrom<Event> for Dict {
    type Error = anyhow::Error;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        match value {
            Event::Incoming(i) => match i {
                Incoming::Publish(p) => {
                    let mut map = BTreeMap::new();
                    map.insert(
                        "$".to_string(),
                        Value::text(str::from_utf8(&p.payload).map_err(|err| anyhow!(err))?),
                    );
                    map.insert("$topic".to_string(), Value::text(&p.topic));
                    Ok(Value::dict(map).as_dict()?)
                }
                _ => bail!("Unexpected Incoming publish {i:?}"),
            },
            Event::Outgoing(_) => bail!("Unexpected Outgoing publish"),
        }
    }
}