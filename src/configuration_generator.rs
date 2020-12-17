//! A master & slave configuration generator.
//!
//! # Usage
//! ```shell
//! configuration_generator slave_num master_configuration_template_path slave_configuration_template_path 
//! ```
//! 
//! * slave_num: the number of slaves should be no less than the group_num in master configuration
//! * master_configuration_template_path: the path of master configuration template
//! * slave_configuration_template_path: the path of slave configuration template
//! 

use fed_truth_finder::config;
use slog::Drain;
use std::io::Write;
use std::fs::File;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        eprintln!("insufficient args (args: slave_num master_configuration_template_path slave_configuration_template_path");
        std::process::exit(-1);
    }

    // init terminal logger
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog::o!());

    // generate master configuration
    let template = config::load_server_config(&args[2]);
    let master_addr = template.addr.clone();
    let master_configuration = serde_json::to_string(&template)?;
    let mut file = File::create("./config/master.json").expect("failed to create master configuration file");
    file.write_all(master_configuration.as_bytes())?;
    slog::info!(logger, #"configuration", "master configuration constructed";
        "listening" => &master_addr,
    );

    // generate slave configurations
    let slave_n: usize = args[1].parse()?;
    if slave_n < template.group_num as usize || slave_n >= u8::MAX as usize {
        eprintln!("the number of slaves should be no less than the group_num in master configuration and no bigger than 255");
        std::process::exit(-2);
    }
    let mut template = config::load_client_config(&args[3]);
    template.addrs.remote_addr = format!("http://{}", master_addr);
    let mut ports = Vec::with_capacity(slave_n);
    for i in 1..=slave_n {
        let file_path = format!("./config/slave_{}.json", i);
        let mut config = template.clone();
        let port = 50050 + i;
        config.addrs.mailbox_addr = format!("[::1]:{}", port);
        ports.push(port);

        let mut file = File::create(file_path).expect("failed to create slave configuration files");
        let config = serde_json::to_string(&config)?;
        file.write_all(config.as_bytes())?;
    }
    slog::info!(logger, #"configuration", "slave configurations constructed";
        "port list" => port_range(&ports),
    );

    // generate slaves' listener list
    let file_path = "./config/listeners.txt";
    let mut file = File::create(file_path).expect("failed to create listener list file");
    for port in ports {
        writeln!(&mut file, "http://[::1]:{}", port)?;
    }
    slog::info!(logger, #"configuration", "slave listener list constructed");

    Ok(())
}

fn port_range(ports: &[usize]) -> String {
    let mut res = String::new();
    for port in ports.iter() {
        res.push(' ');
        res.push_str(port.to_string().as_ref());
    }
    if !ports.is_empty() {
        res.remove(0);
    }
    res
}
