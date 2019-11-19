//! A minimal implementation of SnailTrail 2.
//! Constructs a fixed-window PAG and works with Timely / DD out of the box.
//! The PAG can be visualized using the dashboard cljs frontend.

#![deny(missing_docs)]

use std::time::Duration;
use timely::logging::TimelyEvent;

use serde::{Serialize, Deserialize};

use crate::EdgeType::{Processing, Spinning, Progress, Data, Waiting, Busy};

/// event type as provided by Timely backend
pub type Event = (Duration, usize, TimelyEvent);

/// The various types of activity that can happen in a dataflow.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdgeType {
    /// Operator actually doing work
    Processing {
        /// operator ID
        oid: Option<usize>,
        /// #messages sent
        send: Option<usize>,
        /// #messages received
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

impl PartialEq for EdgeType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Processing { oid: oid1, .. },
             Processing { oid: oid2, .. }) => oid1 == oid2,
            (Spinning(x), Spinning(y)) => x == y,
            (Progress, Progress) => true,
            (Data(_), Data(_)) => true,
            (Waiting, Waiting) => true,
            (Busy, Busy) => true,
            _ => false
        }
    }
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
