use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope};
use timely::logging::StartStop;
use timely::logging::TimelyEvent::{Messages, Progress, Schedule, Operates};
use timely::Data;

use st2::{PagNode, PagEdge, Event, EdgeType};

use std::collections::{HashMap, BTreeSet};
use std::hash::Hash;

use serde::{Serialize, Deserialize};


pub trait Pag<S: Scope> {
    fn peel(&self) -> Stream<S, Event>;
    fn local_edges(&self) -> Stream<S, PagEdge>;
    fn remote_edges(&self) -> Stream<S, PagEdge>;
}

impl<S: Scope> Pag<S> for Stream<S, Event> {
    fn peel(&self) -> Stream<S, Event> {
        self.unary(Pipeline, "Peel", move |_, _| {
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
                            Messages(ref _e) => {
                                output.session(&cap).give((t, wid, x));
                            }
                            _ => { /* filters out all events we don't need */ }
                        }
                    }
                });
            }
        })
    }

    fn local_edges(&self) -> Stream<S, PagEdge> {
        self.unary(Pipeline, "Local Edges", move |_, _| {
            let mut vector = Vec::new();
            let mut buffer: HashMap<usize, Event> = HashMap::new();
            let mut buffer2: HashMap<usize, Event> = HashMap::new();
            let mut oids: HashMap<usize, Option<usize>> = HashMap::new();

            move |input, output| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);

                    for curr in vector.drain(..) {
                        let (t, wid, _x) = &curr;

                        if let Some(prev) = buffer.remove(&wid) {
                            let (prev_t, _prev_wid, _prev_x) = &prev;
                            assert!(t >= prev_t);

                            if let Some(prev2) = buffer2.remove(&wid) {
                                let (prev2_t, _prev2_wid, _prev2_x) = &prev2;
                                assert!(prev_t >= prev2_t);

                                let oid = oids.entry(*wid).or_insert(None);
                                let edge = build_local_edge(&prev2, &prev, &curr, oid);
                                output.session(&cap).give(edge);
                            }

                            // move prev -> prev2
                            buffer2.insert(*wid, prev);
                        }

                        // move curr -> prev
                        buffer.insert(*wid, curr);
                    }
                });
            }
        })
    }

    fn remote_edges(&self) -> Stream<S, PagEdge> {
        let sent = self
            .flat_map(|(t, wid, x)| match x {
                Progress(ref e) if e.is_send => Some(((e.source, None, e.seq_no, e.channel), (t, wid, x))),
                Messages(ref e) if e.is_send && e.source != e.target => Some(((e.source, Some(e.target), e.seq_no, e.channel), (t, wid, x))),
                _ => None
            });

        let received = self
            .flat_map(|(t, wid, x)| match x {
                Progress(ref e) if !e.is_send => Some(((e.source, None, e.seq_no, e.channel), (t, wid, x))),
                Messages(ref e) if !e.is_send && e.source != e.target => Some(((e.source, Some(e.target), e.seq_no, e.channel), (t, wid, x))),
                _ => None
            });

        sent
            .join_edges(&received)
            .map(|((from_t, from_wid, from_x), (to_t, to_wid, _to_x))| {
                let edge_type = match from_x {
                    Progress(ref _e) => EdgeType::Progress,
                    Messages(ref e) => EdgeType::Data(e.length),
                    _ => unreachable!()
                };

                PagEdge {
                    src: PagNode { t: from_t, wid: from_wid },
                    dst: PagNode { t: to_t, wid: to_wid },
                    edge_type
                }
            })
    }
}

fn build_local_edge(prev: &Event, curr: &Event, next: &Event, oid: &mut Option<usize>) -> PagEdge {
    use EdgeType::{Processing, Waiting, Busy, Spinning};

    let (prev_t, prev_wid, prev_x) = prev;
    let (t, wid, x) = curr;
    let (_next_t, next_wid, next_x) = next;
    assert!(*prev_wid == *wid && *wid == *next_wid);

    let mut edge_type = match (prev_x, x) {
        (_, Progress(r)) if !r.is_send => {
            assert!(r.source != *wid);
            Waiting
        },
        (Schedule(p), Schedule(r)) if p.start_stop == StartStop::Start && r.start_stop == StartStop::Stop => Spinning(p.id),
        (Schedule(p), _) if p.start_stop == StartStop::Start => {
            *oid = Some(p.id);
            Processing { oid: *oid, send: None, recv: None }
        },
        // @TODO: differential message placement
        (Messages(p), Schedule(r)) if p.is_send && r.start_stop == StartStop::Start => {
            *oid = Some(r.id);
            Processing { oid: *oid, send: Some(p.length), recv: None }
        }
        (Messages(p), _) if p.is_send => Processing { oid: *oid, send: Some(p.length), recv: None },
        (Messages(p), _) if !p.is_send => Processing { oid: *oid, send: None, recv: Some(p.length) },
        _ => Busy,
    };

    // waiting on data message
    if edge_type == Busy {
        if let (Schedule(_), Messages(m)) = (x, next_x) {
            if m.source != m.target {
                edge_type = Waiting;
            }
        }
    }

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


pub trait TrimPag<S: Scope> {
    fn trim_local(&self) -> Stream<S, PagEdge>;
}

impl<S: Scope> TrimPag<S> for Stream<S, PagEdge> {
    fn trim_local(&self) -> Stream<S, PagEdge> {
        use st2::EdgeType::{Processing, Waiting, Busy, Spinning, Data, Progress};

        self.unary(Pipeline, "Trim", move |_, _| {
            let mut vector = Vec::new();
            let mut first_edge: HashMap<usize, PagEdge> = HashMap::new();

            move |input, output| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);

                    for mut edge in vector.drain(..) {
                        let wid = edge.src.wid;
                        if let Some(mut first) = first_edge.remove(&wid) {
                            if edge.edge_type == Busy && first.edge_type != Waiting {
                                first.dst = edge.dst;
                                first_edge.insert(wid, first);
                            } else if first.edge_type == Busy {
                                edge.src = first.src;
                                first_edge.insert(wid, edge);
                            } else if edge.edge_type == first.edge_type {
                                first.dst = edge.dst;

                                first.edge_type = match (first.edge_type, edge.edge_type) {
                                    (Processing { send: f_send, recv: f_recv, oid},
                                     Processing { send: e_send, recv: e_recv, ..}) => {
                                        let send = match (f_send, e_send) {
                                            (None, None) => None,
                                            (Some(x), None) => Some(x),
                                            (None, Some(x)) => Some(x),
                                            (Some(x), Some(y)) => Some(x + y)
                                        };

                                        let recv = match (f_recv, e_recv) {
                                            (None, None) => None,
                                            (Some(x), None) => Some(x),
                                            (None, Some(x)) => Some(x),
                                            (Some(x), Some(y)) => Some(x + y)
                                        };

                                        Processing { oid: oid, send, recv }
                                    }
                                    (Spinning(f), Spinning(_)) => Spinning(f),
                                    (Progress, Progress) => Progress,
                                    (Data(f), Data(e)) => Data(f + e),
                                    (Waiting, Waiting) => Waiting,
                                    (Busy, Busy) => Busy,
                                    _ => unreachable!()
                                };

                                first_edge.insert(wid, first);
                            } else {
                                output.session(&cap).give(first);
                                first_edge.insert(wid, edge);
                            }
                        } else {
                            first_edge.insert(wid, edge);
                        }
                    }
                })
            }
        })
    }
}


trait JoinEdges<S: Scope, D> where D: Data + Hash + Eq + Send + Sync + Serialize + for<'a>Deserialize<'a> {
    fn join_edges(&self, other: &Stream<S, (D, Event)>) -> Stream<S, (Event, Event)>;
}

impl<S: Scope, D> JoinEdges<S, D>
    for Stream<S, (D, Event)>
where D: Data + Hash + Eq + Send + Sync + Serialize + for<'a>Deserialize<'a>
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
