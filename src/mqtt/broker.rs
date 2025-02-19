use rumqttd::local::{LinkRx, LinkTx};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};


pub(crate) fn create_broker(port: u16, url: String, id: usize) -> (Broker, LinkTx, LinkRx) {
    let mut config = Config::default();

    config.router = RouterConfig {
        max_connections: 100,
        max_outgoing_packet_count: 100,
        max_segment_size: 100000000,
        max_segment_count: 100000,
        ..Default::default()
    };

    config.v4 = Some(
        HashMap::from([
            (id.to_string(), ServerSettings {
                name: id.to_string(),
                listen: SocketAddr::V4(SocketAddrV4::new(url.parse().unwrap(), port)),
                tls: None,
                next_connection_delay_ms: 0,
                connections: ConnectionSettings {
                    connection_timeout_ms: 10000,
                    max_payload_size: 10000000,
                    max_inflight_count: 1000,
                    auth: None,
                    external_auth: None,
                    dynamic_filters: false,
                },
            })
        ])
    );
    // Create the broker with the configuration
    let broker = Broker::new(config);
    let (link_tx, link_rx) = broker.link("link").unwrap();
    (broker, link_tx, link_rx)
}
