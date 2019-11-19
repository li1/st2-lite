use timely::dataflow::operators::capture::EventWriter;
use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};
use timely::dataflow::InputHandle;
use timely::logging::BatchLogger;
use timely::logging::TimelyEvent;

use std::fs::File;
use std::path::Path;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // Optionally enable offline logging
        if let Err(_) = ::std::env::var("TIMELY_WORKER_LOG_ADDR") {
            println!("write to file");

            let name = format!("{}.dump", worker.index());
            let path = Path::new(&name);
            let file = match File::create(&path) {
                Err(why) => panic!("couldn't create {}: {}", path.display(), why),
                Ok(file) => file,
            };

            let writer = EventWriter::<_, _, _>::new(file);
            let mut logger = BatchLogger::new(writer);

            worker
                .log_register()
                .insert::<TimelyEvent, _>("timely", move |time, data| {
                    logger.publish_batch(time, data)
                });
        }

        // Some computation
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|x| *x as u64 + 1)
                .inspect(move |x| println!("hello {}", x))
                .probe()
        });

        for round in 0..10 {
            if round % worker.peers() == worker.index() {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}
