use std::time::Duration;
use timely::logging::TimelyEvent;

use serde::{Serialize, Deserialize};

/// event type as provided by Timely backend
pub type Event = (Duration, usize, TimelyEvent);

/// The various types of activity that can happen in a dataflow.
#[derive(PartialEq, Debug, Clone, Copy, Hash, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdgeType {
    /// Operator actually doing work
    Processing {
        oid: Option<usize>,
        send: Option<usize>,
        recv: Option<usize>,
    },
    /// Operator scheduled, but not doing any work
    Spinning(usize),
    /// remote control messages, e.g. about progress
    Progress,
    /// remote data messages, e.g. moving tuples around
    Data(usize),
    /// Waiting for unblocking.
    /// In particular, operator might wait for external input.
    Waiting,
    /// Waiting where next activity is actively prepared,
    /// e.g. in-between a ScheduleEnd and consecutive ScheduleStart.
    /// In particular, operator doesn't depend on external input.
    Busy
}

/// A node in the PAG
#[derive(Clone, PartialEq, Hash, Eq, Copy, Debug, Serialize, Deserialize)]
pub struct PagNode {
    /// Timestamp of the event (also a unique identifier!)
    pub t: Duration,
    /// Unique ID of the worker the event belongs to
    pub wid: usize,
}

/// An edge in the activity graph
#[derive(Clone, PartialEq, Hash, Eq, Debug, Serialize, Deserialize)]
pub struct PagEdge {
    /// The source node
    pub src: PagNode,
    /// The destination node
    pub dst: PagNode,
    /// The activity type
    pub edge_type: EdgeType,
}
