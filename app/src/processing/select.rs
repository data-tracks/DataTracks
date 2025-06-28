use crate::processing;
use crate::processing::window::WindowStrategy;
use crate::util::TriggerType;
use std::cmp::PartialEq;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;
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
            from: time,
            to: time,
        }
    }
}

pub struct WindowSelector {
    dirty_windows: BTreeMap<WindowDescriptor, bool>,
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

    pub(crate) fn select(&mut self, current: Time) -> HashMap<WindowDescriptor, bool> {
        self.dirty_windows.append(&mut self.strategy.sync(current));
        std::mem::take(&mut self.dirty_windows)
            .into_iter()
            .collect() // drain
    }
}

pub struct TriggerSelector {
    triggered_windows: HashMap<WindowDescriptor, TriggerStatus>,
    pub(crate) storage: Storage,
    trigger: TriggerType,
    fire_early: bool,
    fire_late: bool,
    re_fire: bool,
}

impl TriggerSelector {
    pub(crate) fn new(storage: Storage, trigger: TriggerType) -> Self {
        TriggerSelector {
            triggered_windows: Default::default(),
            storage,
            trigger,
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
        windows.into_iter().for_each(|(window, _is_complete)| {
            let mut trigger = false;
            if window.to == window.from {
                // we re-trigger same windows always
                trigger = true;
            } else if let Some(status) = self.triggered_windows.get(&window) {
                // have already seen this window, are in window
                if self.re_fire
                    && current < &window.to
                    && (matches!(self.trigger, TriggerType::Element))
                {
                    // still early
                    debug!("trigger early");
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::Early);

                    // are past the window, did not trigger yet or early
                } else if current >= &window.to
                    && (status == &TriggerStatus::Untriggered
                        || (status == &TriggerStatus::Early
                            && matches!(self.trigger, TriggerType::Element))
                        || (self.re_fire))
                {
                    debug!("trigger onTime or refire");
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::OnTime);
                }
            } else {
                // have not seen this window, are past it
                if &window.to <= current
                    && (matches!(self.trigger, TriggerType::Element)
                        || matches!(self.trigger, TriggerType::WindowEnd))
                {
                    // on time, did not fire yet
                    debug!("@ {} trigger onTime {:?}", current, window);
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::OnTime);
                } else if self.fire_early || matches!(self.trigger, TriggerType::Element) {
                    // early fire
                    debug!("fire early");
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

#[derive(Clone, PartialEq, Debug)]
pub enum TriggerStatus {
    Untriggered,
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
    use crate::util::TriggerType;
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
        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 1);

        let storage: Storage = Arc::new(Mutex::new(vec![]));
        let mut trigger = TriggerSelector::new(storage.clone(), TriggerType::Element);
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
        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 1);

        let storage: Storage = Arc::new(Mutex::new(vec![]));
        let mut trigger = TriggerSelector::new(storage.clone(), TriggerType::Element);
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
        let windows = selector.select(train.event_time);
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
        let time = train.event_time;
        selector.mark(&train);

        let windows = selector.select(time);
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

        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 2);
    }

    #[test]
    fn test_trigger_includes_two_different() {
        let window = BackWindow::new(4, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let storage: Storage = Arc::new(Mutex::new(vec![]));
        let mut trigger = TriggerSelector::new(storage.clone(), TriggerType::Element);

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);
        storage.lock().unwrap().push(train);

        let mut train = Train::new(vec![3.into()]);
        train.event_time = Time::new(4, 0);

        let time = train.event_time;

        selector.mark(&train);
        storage.lock().unwrap().push(train);

        let windows = selector.select(time);
        assert_eq!(windows.len(), 2);

        let values = trigger.select(windows, &Time::new(4, 0));

        assert_eq!(values.len(), 2);

        if values.first().cloned().unwrap().1.values.len() == 2 {
            // window order is not ordered
            assert_eq!(values.get(1).cloned().unwrap().1.values.len(), 1);
        } else {
            assert_eq!(values.first().cloned().unwrap().1.values.len(), 1);
        }
    }
}
