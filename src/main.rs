use std::time::Duration;
use std::sync::{Mutex, Arc};
use std::path::PathBuf;
use std::collections::{HashMap, BTreeSet};
use std::hash::Hash;

#[macro_use] extern crate abomonation_derive;
use abomonation::Abomonation;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::concat::Concat;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope};
use timely::logging::{TimelyEvent, WorkerIdentifier, StartStop};
use timely::logging::TimelyEvent::{Messages, Progress, Schedule, Operates};
use timely::Data;

type Event = (Duration, usize, TimelyEvent);

/// The various types of activity that can happen in a dataflow.
#[derive(Abomonation, PartialEq, Debug, Clone, Copy, Hash, Eq, PartialOrd, Ord)]
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
#[derive(Abomonation, Clone, PartialEq, Hash, Eq, Copy, Debug)]
pub struct PagNode {
    /// Timestamp of the event (also a unique identifier!)
    pub t: Duration,
    /// Unique ID of the worker the event belongs to
    pub wid: usize,
}

/// An edge in the activity graph
#[derive(Abomonation, Clone, PartialEq, Hash, Eq, Debug)]
pub struct PagEdge {
    /// The source node
    pub src: PagNode,
    /// The destination node
    pub dst: PagNode,
    /// The activity type
    pub edge_type: EdgeType,
}

fn main() {
    let source_peers: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let from_file: bool = if let Some(f) = std::env::args().nth(2) {
        f == "f".to_string()
    } else {
        false
    };
    let replay_source = make_replay_source(source_peers, from_file);

    timely::execute_from_args(std::env::args(), move |worker| {
        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");
        let peers = worker.peers();

        worker.dataflow::<Duration, _, _>(move |scope| {
            // @TODO: differential
            let stream: Stream<_, (Duration, WorkerIdentifier, TimelyEvent)> = readers.replay_into(scope);

            stream.inspect(|x| println!("{:?}", x));

            let peeled = stream.unary(Pipeline, "Peel", move |_, _| {
                let mut vector = Vec::new();
                let mut outer_operates = BTreeSet::new();
                let mut ids_to_addrs = HashMap::new();

                move |input, output| {
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);
                        for (t, wid, x) in vector.drain(..) {
                            match x {
                                Operates(e) => {
                                    let mut addr = e.addr.clone();
                                    addr.pop();
                                    outer_operates.insert(addr);

                                    ids_to_addrs.insert(e.id, e.addr);
                                }
                                Schedule(ref e) => {
                                    let addr = ids_to_addrs.get(&e.id).expect("operates went wrong");
                                    if !outer_operates.contains(addr) {
                                        output.session(&cap).give((t, wid, x));
                                    }
                                }
                                Progress(ref e) if e.source != wid || e.is_send => {
                                    output.session(&cap).give((t, wid, x));
                                }
                                Messages(ref e) => {
                                    output.session(&cap).give((t, wid, x));
                                }
                                _ => { /* filters out all events we don't need */ }
                            }
                        }
                    });
                }
            });

            let local_edges = peeled.unary(Pipeline, "Local Edges", move |_, _| {
                let mut vector = Vec::new();
                let mut buffer: HashMap<usize, Event> = HashMap::new();
                let mut oids: HashMap<usize, Option<usize>> = HashMap::new();

                move |input, output| {
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);
                        for (t, wid, x) in vector.drain(..) {
                            if let Some((prev_t, prev_wid, prev_x)) = buffer.get(&wid) {
                                // we've seen an event from this local_worker before

                                assert!(t >= *prev_t, format!("{:?} should happen before {:?}", prev_x, x));

                                let oid = oids.entry(wid).or_insert(None);
                                let edge = build_local_edge(&prev_t, &prev_wid, &prev_x, &t, &wid, &x, oid);
                                output.session(&cap).give(edge);
                            }

                            buffer.insert(wid, (t, wid, x));
                        }
                    });
                }
            });

            let sent = peeled
                .flat_map(|(t, wid, x)| match x {
                    Progress(ref e) if e.is_send => Some(((e.source, None, e.seq_no, e.channel), (t, wid, x))),
                    Messages(ref e) if e.is_send && e.source != e.target => Some(((e.source, Some(e.target), e.seq_no, e.channel), (t, wid, x))),
                    _ => None
                });

            let received = peeled
                .flat_map(|(t, wid, x)| match x {
                    Progress(ref e) if !e.is_send => Some(((e.source, None, e.seq_no, e.channel), (t, wid, x))),
                    Messages(ref e) if !e.is_send && e.source != e.target => Some(((e.source, Some(e.target), e.seq_no, e.channel), (t, wid, x))),
                    _ => None
                });

            let remote_edges = sent.join_edges(&received)
                .map(|((from_t, from_wid, from_x), (to_t, to_wid, to_x))| {
                    let edge_type = match from_x {
                        Progress(ref e) => EdgeType::Progress,
                        Messages(ref e) => EdgeType::Data(e.length),
                        _ => unreachable!()
                    };

                    PagEdge {
                        src: PagNode { t: from_t, wid: from_wid },
                        dst: PagNode { t: to_t, wid: to_wid },
                        edge_type
                    }
                });

            local_edges
                .concat(&remote_edges)
                .inspect(|x| println!("{:?}", x));
        });
    }).unwrap();
}


fn make_replay_source(source_peers: usize, from_file: bool) -> ReplaySource {
    if from_file {
        println!("Reading from {} *.dump files", source_peers);

        let files = (0 .. source_peers)
            .map(|idx| format!("{}.dump", idx))
            .map(|path| Some(PathBuf::from(path)))
            .collect::<Vec<_>>();

        ReplaySource::Files(Arc::new(Mutex::new(files)))
    } else {
        let ip_addr: std::net::IpAddr = "127.0.0.1".parse().expect("couldn't parse IP");
        let port: u16 = 1234;

        println!("Listening for {} connections on {}:{}", source_peers, ip_addr, port);

        let sockets = connect::open_sockets(ip_addr, port, source_peers).expect("couldn't open sockets");
        ReplaySource::Tcp(Arc::new(Mutex::new(sockets)))
    }
}


fn build_local_edge(prev_t: &Duration, prev_wid: &usize, prev_x: &TimelyEvent, t: &Duration, wid: &usize, x: &TimelyEvent, oid: &mut Option<usize>) -> PagEdge {
    use crate::EdgeType::{Processing, Waiting, Busy, Spinning};

    let waiting_or_busy = if t.as_nanos() - prev_t.as_nanos() > 15_000 {
        Waiting
    } else {
        Busy
    };

    let edge_type = match (prev_x, x) {
        (Schedule(p), Schedule(r)) if p.start_stop == StartStop::Start && r.start_stop == StartStop::Stop => Spinning(p.id),
        (Schedule(p), _) if p.start_stop == StartStop::Start => {
            *oid = Some(p.id);
            Processing { oid: *oid, send: None, recv: None }
        },
        (Schedule(p), _) if p.start_stop == StartStop::Stop => waiting_or_busy,
        (Progress(_), _) => waiting_or_busy,
        (Messages(p), Schedule(r)) if p.is_send && r.start_stop == StartStop::Start => {
            *oid = Some(r.id);
            Processing { oid: *oid, send: Some(p.length), recv: None }
        }
        (Messages(p), _) if p.is_send => Processing { oid: *oid, send: Some(p.length), recv: None },
        (Messages(p), _) if !p.is_send => Processing { oid: *oid, send: None, recv: Some(p.length) },
        _ => panic!("{:?}, {:?}", prev_x, x)
    };

    // Reset oid if scheduling ended.
    if let Schedule(r) = x {
        if r.start_stop == StartStop::Stop {
            *oid = None;
        }
    }

    PagEdge {
        src: PagNode { t: *prev_t, wid: *prev_wid },
        dst: PagNode { t: *t, wid: *wid },
        edge_type,
    }
}


trait JoinEdges<S: Scope, D> where D: Data + Hash + Eq + Abomonation + Send + Sync {
    fn join_edges(&self, other: &Stream<S, (D, Event)>) -> Stream<S, (Event, Event)>;
}

impl<S: Scope, D> JoinEdges<S, D>
    for Stream<S, (D, Event)>
where D: Data + Hash + Eq + Abomonation + Send + Sync + std::fmt::Debug
{
    fn join_edges(&self, other: &Stream<S, (D, Event)>) -> Stream<S, (Event, Event)> {
        use timely::dataflow::channels::pact::Exchange;

        let exchange = Exchange::new(|(_, x): &(_, Event)| match &x.2 {
            Progress(ref e) => e.source as u64,
            Messages(ref e) => e.source as u64,
            _ => unreachable!()
        });
        let exchange2 = Exchange::new(|(_, x): &(_, Event)| match &x.2 {
            Progress(ref e) => e.source as u64,
            Messages(ref e) => e.source as u64,
            _ => unreachable!()
        });

        // @TODO: in this join implementation, state continually grows.
        self.binary(&other, exchange, exchange2, "HashJoin", |_capability, _info| {
            let mut map1 = HashMap::new();
            let mut map2 = HashMap::<_, Vec<Event>>::new();

            let mut vector1 = Vec::new();
            let mut vector2 = Vec::new();

            move |input1, input2, output| {
                // Drain first input, check second map, update first map.
                input1.for_each(|cap, data| {
                    data.swap(&mut vector1);
                    let mut session = output.session(&cap);
                    for (key, val1) in vector1.drain(..) {
                        if let Some(values) = map2.get(&key) {
                            for val2 in values.iter() {
                                session.give((val1.clone(), val2.clone()));
                            }
                        }

                        map1.entry(key).or_insert(Vec::new()).push(val1);
                    }
                });

                input2.for_each(|cap, data| {
                    data.swap(&mut vector2);
                    let mut session = output.session(&cap);
                    for (key, val2) in vector2.drain(..) {
                        if let Some(values) = map1.get(&key) {
                            for val1 in values.iter() {
                                session.give((val1.clone(), val2.clone()));
                            }
                        }

                        map2.entry(key).or_insert(Vec::new()).push(val2);
                    }
                });
            }
        })
    }
}
