use std::time::Duration;
use std::sync::{Mutex, Arc};
use std::path::PathBuf;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::concat::Concat;

mod pag;
use crate::pag::Pag;
use crate::pag::TrimPag;

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
        let _peers = worker.peers();

        worker.dataflow::<Duration, _, _>(move |scope| {
            // @TODO: differential
            // @TODO: Most operators don't clean up for bounded computations,
            //        resulting in small errors at the very end of the dataflow.

            let stream = readers.replay_into(scope);

            let peeled = stream.peel();

            let local_edges = peeled.local_edges().trim_local();
            let remote_edges = peeled.remote_edges();

            let pag = local_edges.concat(&remote_edges);

            pag.inspect(|x| println!("{},", serde_json::to_string(x).unwrap()));
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
