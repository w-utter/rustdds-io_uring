//! Interoperability test program for `RustDDS` library

#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::{io, time::Duration};
#[cfg(feature = "security")]
use std::path::Path;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, LevelFilter};
use log4rs::{
  append::console::ConsoleAppender,
  config::{Appender, Root},
  Config,
};
use rustdds::{
  with_key::Sample, DomainParticipantBuilder, Keyed, QosPolicyBuilder, StatusEvented,
  TopicDescription, TopicKind,
};
use rustdds::policy::{Deadline, Durability, History, Reliability}; /* import all QoS
                                                                     * policies directly */
use serde::{Deserialize, Serialize};
use clap::{Arg, ArgMatches, Command}; // command line argument processing
use rand::prelude::*;
use smol::Timer;
use futures::{pin_mut, stream::StreamExt, FutureExt, TryFutureExt};
#[cfg(feature = "security")]
use rustdds::DomainParticipantSecurityConfigFiles;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ShapeType {
  color: String,
  x: i32,
  y: i32,
  shape_size: i32,
}

impl Keyed for ShapeType {
  type K = String;
  fn key(&self) -> String {
    self.color.clone()
  }
}

const DA_WIDTH: i32 = 240;
const DA_HEIGHT: i32 = 270;

#[allow(clippy::too_many_lines)]
fn main() {
  configure_logging();
  let matches = get_matches();

  // Process command line arguments
  let topic_name = matches
    .get_one::<String>("topic")
    .cloned()
    .unwrap_or("Square".to_owned());
  let domain_id = matches.get_one::<u16>("domain_id").unwrap();
  let color = matches
    .get_one::<String>("color")
    .cloned()
    .unwrap_or("BLUE".to_owned());

  // Build the DomainParticipant
  let dp_builder = DomainParticipantBuilder::new(*domain_id);
  #[cfg(feature = "security")]
  let dp_builder = if let Some(sec_dir_path) = matches.get_one::<String>("security") {
    dp_builder.builtin_security(
      DomainParticipantSecurityConfigFiles::with_ros_default_names(
        Path::new(sec_dir_path),
        "no_pwd".to_string(),
      ),
    )
  } else {
    dp_builder
  };
  #[cfg(not(feature = "security"))]
  if matches.contains_id("security") {
    warn!("the security command line option was given, but the security feature is not enabled!");
  }

  let domain_participant = dp_builder
    .build()
    .unwrap_or_else(|e| panic!("DomainParticipant construction failed: {e:?}"));

  let mut qos_b = QosPolicyBuilder::new()
    .reliability(if matches.get_flag("reliable") {
      Reliability::Reliable {
        max_blocking_time: rustdds::Duration::ZERO,
      }
    } else {
      Reliability::BestEffort
    })
    .durability(
      match matches.get_one::<String>("durability").map(String::as_str) {
        Some("l") => Durability::TransientLocal,
        Some("t") => Durability::Transient,
        Some("p") => Durability::Persistent,
        _ => Durability::Volatile,
      },
    )
    .history(match matches.get_one::<i32>("history_depth") {
      None => History::KeepAll,
      Some(d) => {
        if *d < 0 {
          History::KeepAll
        } else {
          History::KeepLast { depth: *d }
        }
      }
    });
  let deadline_policy = matches
    .get_one::<f64>("deadline")
    .map(|dl| Deadline(rustdds::Duration::from_frac_seconds(*dl)));

  if let Some(dl) = deadline_policy {
    qos_b = qos_b.deadline(dl);
  }

  assert!(
    !matches.contains_id("partition"),
    "QoS policy Partition is not yet implemented."
  );

  assert!(
    !matches.contains_id("interval"),
    "QoS policy Time Based Filter is not yet implemented."
  );

  assert!(
    !matches.contains_id("ownership_strength"),
    "QoS policy Ownership Strength is not yet implemented."
  );

  let qos = qos_b.build();

  let write_interval: Duration = match deadline_policy {
    None => Duration::from_millis(200), // This is the default rate
    Some(Deadline(dd)) => Duration::from(dd).mul_f32(0.8), // slightly faster than deadline
  };

  let topic = domain_participant
    .create_topic(
      topic_name,
      "ShapeType".to_string(),
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));
  println!(
    "Topic name is {}. Type is {}.",
    topic.name(),
    topic.get_type().name()
  );

  // Set Ctrl-C handler
  let (stop_sender, stop_receiver) = smol::channel::bounded(3);
  ctrlc::set_handler(move || {
    // We will send two stop coammnds, one for reader, the other for writer.
    stop_sender.send_blocking(()).unwrap_or(());
    stop_sender.send_blocking(()).unwrap_or(());
    stop_sender.send_blocking(()).unwrap_or(());
    // ignore errors, as we are quitting anyway
  })
  .expect("Error setting Ctrl-C handler");
  println!("Press Ctrl-C to quit.");

  let is_publisher = matches.get_flag("publisher");
  let is_subscriber = matches.get_flag("subscriber");

  let writer_opt = if is_publisher {
    debug!("Publisher");
    let publisher = domain_participant.create_publisher(&qos).unwrap();
    let writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None) // None = get qos policy from publisher
      .unwrap();
    Some(writer)
  } else {
    None
  };

  let reader_opt = if is_subscriber {
    debug!("Subscriber");
    let subscriber = domain_participant.create_subscriber(&qos).unwrap();
    let reader = subscriber
      .create_datareader_cdr::<ShapeType>(&topic, Some(qos))
      .unwrap();
    debug!("Created DataReader");
    Some(reader)
  } else {
    None
  };

  let mut shape_sample = ShapeType {
    color,
    x: 0,
    y: 0,
    shape_size: 21,
  };

  let mut random_gen = rand::rng();
  // A bit complicated lottery to ensure we do not end up with zero velocity,
  // because that would make a boring demo.
  let mut x_vel = if rand::random() {
    random_gen.random_range(1..5)
  } else {
    random_gen.random_range(-5..-1)
  };
  let mut y_vel = if rand::random() {
    random_gen.random_range(1..5)
  } else {
    random_gen.random_range(-5..-1)
  };

  let dp_event_loop = async {
    let mut run = true;
    let stop = stop_receiver.recv().fuse();
    pin_mut!(stop);
    let dp_status_listener = domain_participant.status_listener();
    let mut dp_status_stream = dp_status_listener.as_async_status_stream();

    while run {
      futures::select! {
        _ = stop => run = false,
        e = dp_status_stream.select_next_some() => {
          println!("DP Status: {e:?}");
        }
      } // select!
    } // while
  };

  let read_loop = async {
    match reader_opt {
      None => (),
      Some(datareader) => {
        let mut run = true;
        let stop = stop_receiver.recv().fuse();
        pin_mut!(stop);
        let mut datareader_stream = datareader.async_sample_stream();
        let mut datareader_event_stream = datareader_stream.async_event_stream();
        while run {
          futures::select! {
            _ = stop => run = false,
            r = datareader_stream.select_next_some() => {
              match r {
                Ok(s) =>
                  match s.into_value() {
                    Sample::Value(sample) => println!(
                      "{:10.10} {:10.10} {:3.3} {:3.3} [{}]",
                      topic.name(),
                      sample.color,
                      sample.x,
                      sample.y,
                      sample.shape_size,
                    ),
                    Sample::Dispose(key) => println!("Disposed key {key:?}"),
                  }
                Err(e) => {
                  error!("{:?}", e);
                  break;
                }
              }
            }
            e = datareader_event_stream.select_next_some() => {
              println!("DataReader event: {e:?}");
            }
          } // select!
        } // while
        println!("Reader task done.");
      }
    }
  };

  let write_loop = async {
    match writer_opt {
      None => (),
      Some(datawriter) => {
        let mut run = true;
        let stop = stop_receiver.recv().fuse();
        pin_mut!(stop);
        let mut tick_stream = futures::StreamExt::fuse(Timer::interval(write_interval));

        let mut datawriter_event_stream = datawriter.as_async_status_stream();

        while run {
          futures::select! {
            _ = stop => run = false,
            _ = tick_stream.select_next_some() => {
              let r = move_shape(shape_sample, x_vel, y_vel);
              shape_sample = r.0;
              x_vel = r.1;
              y_vel = r.2;

              datawriter.async_write(shape_sample.clone(), None)
                .unwrap_or_else(|e| error!("DataWriter write failed: {e:?}"))
                .await;
            }
            e = datawriter_event_stream.select_next_some() => {
              println!("DataWriter event: {e:?}");
            }
          } // select!
        } // while
        println!("Writer task done.");
      }
    }
  };

  // Run both read and write concurrently, until both are done.
  smol::block_on(async { futures::join!(read_loop, write_loop, dp_event_loop) });
}

fn configure_logging() {
  // initialize logging, preferably from config file
  log4rs::init_file(
    "logging-config.yaml",
    log4rs::config::Deserializers::default(),
  )
  .unwrap_or_else(|e| {
    match e.downcast_ref::<io::Error>() {
      // Config file did not work. If it is a simple "No such file or directory", then
      // substitute some default config.
      Some(os_err) if os_err.kind() == io::ErrorKind::NotFound => {
        println!("No config file found in current working directory.");
        let stdout = ConsoleAppender::builder().build();
        let conf = Config::builder()
          .appender(Appender::builder().build("stdout", Box::new(stdout)))
          .build(Root::builder().appender("stdout").build(LevelFilter::Error))
          .unwrap();
        log4rs::init_config(conf).unwrap();
      }
      // Give up.
      other_error => panic!("Config problem: {other_error:?}"),
    }
  });
}

#[allow(clippy::too_many_lines)]
fn get_matches() -> ArgMatches {
  Command::new("RustDDS-interop")
    .version("0.2.2")
    .author("Juhana Helovuo <juhe@iki.fi>")
    .about("Command-line \"shapes\" interoperability test.")
    .arg(
      Arg::new("domain_id")
        .short('d')
        .value_name("id")
        .value_parser(clap::value_parser!(u16))
        .default_value("0")
        .help("Sets the DDS domain id number"),
    )
    .arg(
      Arg::new("topic")
        .short('t')
        .value_name("name")
        .help("Sets the topic name")
        .required(true),
    )
    .arg(
      Arg::new("color")
        .short('c')
        .value_name("color")
        .default_value("BLUE")
        .help("Color to publish (or filter)"),
    )
    .arg(
      Arg::new("durability")
        .short('D')
        .value_name("durability")
        .help("Set durability")
        .value_parser(["v", "l", "t", "p"]),
    )
    .arg(
      Arg::new("publisher")
        .help("Act as publisher")
        .short('P')
        .action(clap::ArgAction::SetTrue)
        .required_unless_present("subscriber"),
    )
    .arg(
      Arg::new("subscriber")
        .help("Act as subscriber")
        .short('S')
        .action(clap::ArgAction::SetTrue)
        .required_unless_present("publisher"),
    )
    .arg(
      Arg::new("best_effort")
        .help("BEST_EFFORT reliability")
        .short('b')
        .action(clap::ArgAction::SetTrue)
        .conflicts_with("reliable"),
    )
    .arg(
      Arg::new("reliable")
        .help("RELIABLE reliability")
        .short('r')
        .action(clap::ArgAction::SetTrue)
        .conflicts_with("best_effort"),
    )
    .arg(
      Arg::new("history_depth")
        .help("Keep history depth")
        .short('k')
        .value_parser(clap::value_parser!(i32))
        .default_value("1")
        .value_name("depth"),
    )
    .arg(
      Arg::new("deadline")
        .help("Set a 'deadline' with interval (seconds)")
        .short('f')
        .value_parser(clap::value_parser!(f64))
        .value_name("deadline"),
    )
    .arg(
      Arg::new("partition")
        .help("Set a 'partition' string")
        .short('p')
        .value_parser(clap::value_parser!(String))
        .value_name("partition"),
    )
    .arg(
      Arg::new("interval")
        .help("Apply 'time based filter' with interval (seconds)")
        .short('i')
        .value_parser(clap::value_parser!(f64))
        .value_name("interval"),
    )
    .arg(
      Arg::new("ownership_strength")
        .help("Set ownership strength [-1: SHARED]")
        .short('s')
        .value_parser(clap::value_parser!(i32))
        .value_name("strength"),
    )
    .arg(
      Arg::new("security")
        .help(
          "Path to directory containing security configuration files. Setting this enables \
           security.",
        )
        .long("security")
        .value_name("security"),
    )
    .get_matches()
}

#[allow(clippy::similar_names)]
fn move_shape(shape: ShapeType, xv: i32, yv: i32) -> (ShapeType, i32, i32) {
  let half_size = shape.shape_size / 2 + 1;
  let mut x = shape.x + xv;
  let mut y = shape.y + yv;

  let mut xv_new = xv;
  let mut yv_new = yv;

  if x < half_size {
    x = half_size;
    xv_new = -xv;
  }
  if x > DA_WIDTH - half_size {
    x = DA_WIDTH - half_size;
    xv_new = -xv;
  }
  if y < half_size {
    y = half_size;
    yv_new = -yv;
  }
  if y > DA_HEIGHT - half_size {
    y = DA_HEIGHT - half_size;
    yv_new = -yv;
  }
  (
    ShapeType {
      color: shape.color,
      x,
      y,
      shape_size: shape.shape_size,
    },
    xv_new,
    yv_new,
  )
}
