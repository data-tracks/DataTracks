use crate::processing;
use crate::processing::portal::Portal;
use crate::processing::select::WindowDescriptor::{Normal, Unbounded};
use crate::processing::window::WindowStrategy;
use crate::util::TriggerType;
use std::cmp::PartialEq;
use std::collections::{BTreeMap, HashMap};
use tracing::debug;
use value::train::{Train, TrainId};
use value::Time;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Copy, Debug)]
pub enum WindowDescriptor {
    Normal(Time, Time),
    Unbounded(TrainId),
}

impl WindowDescriptor {
    pub fn new(from: Time, to: Time) -> WindowDescriptor {
        Normal(from, to)
    }

    pub fn unbounded(train_id: TrainId) -> WindowDescriptor {
        Unbounded(train_id)
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
    pub(crate) portal: Portal,
    trigger: TriggerType,
    fire_early: bool,
    fire_late: bool,
    re_fire: bool,
}

impl TriggerSelector {
    pub(crate) fn new(portal: Portal, trigger: TriggerType) -> Self {
        TriggerSelector {
            triggered_windows: Default::default(),
            portal,
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
            match window {
                Normal(from, to) => {
                    if let Some(status) = self.triggered_windows.get(&window) {
                        // have already seen this window, are in window
                        if self.re_fire
                            && current < &to
                            && (matches!(self.trigger, TriggerType::Element))
                        {
                            // still early
                            debug!("trigger early");
                            trigger = true;
                            self.triggered_windows.insert(window, TriggerStatus::Early);

                            // are past the window, did not trigger yet or early
                        } else if current >= &to
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
                        if &to <= current
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
                        match self.get_trains(&from, &to) {
                            None => {}
                            Some(t) => trains.push((window, t)),
                        }
                    }
                }
                Unbounded(train_id) => match self.get_train(train_id) {
                    None => {}
                    Some(t) => trains.push((window, t)),
                },
            }
        });

        trains
    }

    fn get_trains(&self, from: &Time, to: &Time) -> Option<Train> {
        let mut ids = vec![];
        for (id, time) in self.portal.peek() {
            if from < &time && to >= &time {
                ids.push(id);
            }
        }
        self.portal
            .get_trains(ids)
            .into_iter()
            .reduce(|a, b| a.merge(b))
    }

    fn get_train(&self, train_id: TrainId) -> Option<Train> {
        self.portal.get_train(train_id)
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
    use crate::processing::portal::Portal;
    use crate::processing::select::{TriggerSelector, WindowSelector};
    use crate::processing::window::Window::Non;
    use crate::processing::window::{BackWindow, NonWindow, Window};
    use crate::util::TimeUnit;
    use crate::util::TriggerType;
    use value::train::Train;
    use value::Time;

    #[test]
    fn test_window_no_window_current() {
        let window = NonWindow {};
        let mut selector = WindowSelector::new(Non(window));

        let mut train = Train::new(vec![3.into()], 0);
        train.event_time = Time::new(3, 3);

        selector.mark(&train);
        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 1);

        let portal = Portal::new().unwrap();
        let mut trigger = TriggerSelector::new(portal.clone(), TriggerType::Element);
        portal.push_train(train);

        trigger.select(windows, &Time::new(3, 3));
    }

    #[test]
    fn test_window_no_window_post() {
        let window = NonWindow {};
        let mut selector = WindowSelector::new(Non(window));

        let mut train = Train::new(vec![3.into()], 0);
        train.event_time = Time::new(3, 3);

        selector.mark(&train);
        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 1);

        let portal = Portal::new().unwrap();
        let mut trigger = TriggerSelector::new(portal.clone(), TriggerType::Element);
        portal.push_train(train);

        trigger.select(windows, &Time::new(5, 5));
    }

    #[test]
    fn test_window_includes_single() {
        let window = BackWindow::new(3, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let mut train = Train::new(vec![3.into()], 0);
        train.event_time = Time::new(3, 0);

        selector.mark(&train);
        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 1);
    }

    #[test]
    fn test_window_includes_two_same() {
        let window = BackWindow::new(3, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let mut train = Train::new(vec![3.into()], 0);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);

        let mut train = Train::new(vec![3.into()], 1);
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

        let mut train = Train::new(vec![3.into()], 0);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);

        let mut train = Train::new(vec![3.into()], 1);
        train.event_time = Time::new(4, 0);
        selector.mark(&train);

        let windows = selector.select(train.event_time);
        assert_eq!(windows.len(), 2);
    }

    #[test]
    fn test_trigger_includes_two_different() {
        let window = BackWindow::new(4, TimeUnit::Millis);
        let mut selector = WindowSelector::new(Window::Back(window));

        let portal = Portal::new().unwrap();
        let mut trigger = TriggerSelector::new(portal.clone(), TriggerType::Element);

        let mut train = Train::new(vec![3.into()], 0);
        train.event_time = Time::new(3, 0);
        selector.mark(&train);
        portal.push_train(train);

        let mut train = Train::new(vec![3.into()], 1);
        train.event_time = Time::new(4, 0);

        let time = train.event_time;

        selector.mark(&train);
        portal.push_train(train);

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
