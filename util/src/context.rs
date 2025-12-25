use std::alloc::System;
use std::time::SystemTime;
use chrono::Utc;
use crate::definition::Entity;
use speedy::{Readable, Writable};

#[derive(Clone, Debug, Writable, Readable)]
pub struct InitialMeta {
    pub name: Option<String>,
}

impl InitialMeta {
    pub fn new(name: Option<String>) -> Self {
        InitialMeta { name }
    }
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct TimedMeta {
    pub id: usize,
    pub timestamp: i64,
    pub name: Option<String>,
}

impl TimedMeta {

    pub fn new(id: usize, initial_meta: InitialMeta) -> Self {
        Self {
            id,
            timestamp: Utc::now().timestamp_millis(),
            name: initial_meta.name }
    }
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct TargetedMeta {
    pub id: usize,
    pub timestamp: i64,
    pub entity: Entity,
}

impl TargetedMeta {
    pub fn new(meta: TimedMeta, entity: Entity) -> Self {
        Self {
            id: meta.id,
            timestamp: meta.timestamp,
            entity,
        }
    }
}

#[derive(Clone, Debug, Writable, Readable, PartialEq)]
pub struct Meta {
    pub name: Option<String>,
}

impl Meta {
    pub fn new(name: Option<String>) -> Self {
        Self { name }
    }
}