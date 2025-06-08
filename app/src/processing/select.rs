use crate::processing;
use crate::processing::window::WindowStrategy;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use value::train::Train;
use value::Time;

pub type Storage = Arc<Mutex<Vec<Train>>>;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Debug)]
pub struct WindowDescriptor {
    from: Time,
    to: Time,
}

impl WindowDescriptor {
    pub fn new(from: Time, to: Time) -> WindowDescriptor {
        WindowDescriptor { from, to }
    }

    pub fn unbounded(time: Time) -> WindowDescriptor {
        WindowDescriptor {
            from: time.clone(),
            to: time.clone(),
        }
    }
}

pub struct WindowSelector {
    dirty_windows: HashMap<WindowDescriptor, bool>,
    strategy: WindowStrategy,
}

impl WindowSelector {
    pub(crate) fn new(window: processing::window::Window) -> Self {
        let strategy = window.get_strategy();
        Self {
            dirty_windows: Default::default(),
            strategy,
        }
    }

    pub(crate) fn mark(&mut self, train: &Train) {
        self.strategy.mark(train).into_iter().for_each(|window| {
            self.dirty_windows.insert(window.0, window.1);
        })
    }

    pub(crate) fn select(&mut self) -> HashMap<WindowDescriptor, bool> {
        self.dirty_windows.drain().collect()
    }
}

pub struct TriggerSelector {
    triggered_windows: HashMap<WindowDescriptor, TriggerStatus>,
    pub(crate) storage: Storage,
    fire_on: TriggerType,
    fire_early: bool,
    fire_late: bool,
    re_fire: bool,
}

impl TriggerSelector {
    pub(crate) fn new(storage: Storage) -> Self {
        TriggerSelector {
            triggered_windows: Default::default(),
            storage,
            fire_on: TriggerType::AfterWatermark,
            fire_early: false,
            fire_late: false,
            re_fire: false,
        }
    }

    pub(crate) fn select(
        &mut self,
        windows: HashMap<WindowDescriptor, bool>,
        current: &Time,
    ) -> Vec<(WindowDescriptor, Train)> {
        let mut trains = vec![];
        windows.into_iter().for_each(|(window, is_complete)| {
            let mut trigger = false;
            if let Some(status) = self.triggered_windows.get(&window) {
                // have already seen this window
                if self.re_fire && status == &TriggerStatus::Early && &window.to <= current {
                    // still early
                    trigger = true;
                } else if self.fire_late && status == &TriggerStatus::OnTime {
                    // we fired on time already and re-fire late
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::Late);
                }
            } else {
                // have not seen this window
                if &window.to <= current {
                    // on time, did not fire yet
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::OnTime);
                } else if self.fire_early {
                    // early fire
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::Early);
                }
            }
            if trigger {
                match self.get_trains(window) {
                    None => {}
                    Some(t) => trains.push((window, t)),
                }
            }
        });

        trains
    }

    fn get_trains(&self, window: WindowDescriptor) -> Option<Train> {
        let storage = self.storage.lock().unwrap();
        let is_same = window.to == window.from;
        storage
            .iter()
            .filter(|train| {
                (is_same && window.to == train.event_time)
                    || (window.from < train.event_time && window.to >= train.event_time)
            })
            .cloned()
            .reduce(|a, b| a.merge(b))
    }
}

pub enum TriggerType {
    AfterWatermark,
    OnElement,
}

#[derive(Clone, PartialEq)]
pub enum TriggerStatus {
    Early,
    OnTime,
    Late,
}

#[cfg(test)]
mod tests {
    use crate::processing::select::{Storage, TriggerSelector, WindowSelector};
    use crate::processing::window::Window::Non;
    use crate::processing::window::{BackWindow, NonWindow, Window};
    use crate::util::TimeUnit;
    use std::sync::{Arc, Mutex};
    use value::train::Train;
    use value::Time;

    #[test]
    fn test_window_no_window_current() {
        let window = NonWindow {};
        let mut selector = WindowSelector::new(Non(window));

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 3);

        selector.mark(&train);
        let windows = selector.select();
        assert_eq!(windows.len(), 1);

        let storage: Storage = Arc::new(Mutex::new(vec![]));
        let mut trigger = TriggerSelector::new(storage.clone());
        storage.lock().unwrap().push(train);

        trigger.select(windows, &Time::new(3, 3));
    }

    #[test]
    fn test_window_no_window_post() {
        let window = NonWindow {};
        let mut selector = WindowSelector::new(Non(window));

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 3);

        selector.mark(&train);
        let windows = selector.select();
        assert_eq!(windows.len(), 1);

        let storage: Storage = Arc::new(Mutex::new(vec![]));
        let mut trigger = TriggerSelector::new(storage.clone());
        storage.lock().unwrap().push(train);

        trigger.select(windows, &Time::new(5, 5));
    }

    #[test]
    fn test_window_includes_single() {
        let window = BackWindow::new(3, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 0);

        selector.mark(&train);
        let windows = selector.select();
        assert_eq!(windows.len(), 1);
    }

    #[test]
    fn test_window_includes_two_same() {
        let window = BackWindow::new(3, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);

        let windows = selector.select();
        assert_eq!(windows.len(), 1);
    }

    #[test]
    fn test_window_includes_two_different() {
        let window = BackWindow::new(3, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(4, 0);
        selector.mark(&train);

        let windows = selector.select();
        assert_eq!(windows.len(), 2);
    }

    #[test]
    fn test_trigger_includes_two_different() {
        let window = BackWindow::new(3, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let storage: Storage = Arc::new(Mutex::new(vec![]));
        let mut trigger = TriggerSelector::new(storage.clone());

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);
        storage.lock().unwrap().push(train);

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(4, 0);
        selector.mark(&train);
        storage.lock().unwrap().push(train);

        let windows = selector.select();
        assert_eq!(windows.len(), 2);

        let values = trigger.select(windows, &Time::new(4, 0));

        assert_eq!(values.len(), 2);

        assert_eq!(values.first().cloned().unwrap().1.values.unwrap().len(), 1);
        assert_eq!(values.get(1).cloned().unwrap().1.values.unwrap().len(), 2);
    }
}
