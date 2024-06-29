use std::task::ready;
use log::debug;
use crate::processing::block::Block;
use crate::processing::station::Station;
use crate::processing::Train;

pub(crate) struct Platform{
    func: dyn Fn(),
}

impl Platform {
    pub(crate) fn operate(&self) {
        (self.func)()
    }
}

impl Platform {
    pub(crate) fn build(station: &mut Station) -> Box<Platform> {
        let receiver = station.incoming.1.clone();
        let sender = station.outgoing.clone();
        let transform = station.transform.transformer();
        let window = station.window.windowing();
        let stop = station.stop;
        let blocks = station.block.clone();
        let inputs = station.inputs.clone();

        let func = move || {
            let process = move |trains: &mut Vec<Train>| {
                let mut transformed = transform.process(stop, window(trains));
                transformed.last = stop;
                sender.send(transformed)
            };
            let mut block = Block::new(inputs, blocks, Box::new(process));

            while let Ok(train) = receiver.recv() {
                block.next(train) // window takes precedence to
            }
        };
        Box::Platform{func}

    }
}