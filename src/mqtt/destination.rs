pub struct MqttDestination {
    port: u16,
}

impl MqttDestination {
    pub fn new(port: u16) -> Self {
        MqttDestination { port }
    }
}