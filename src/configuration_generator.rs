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
use std::fs::File;
use std::io::Write;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
struct Compose {
    pub version: String,
    pub services: HashMap<String, Service>,
    pub networks: HashMap<String, Network>
}

impl Default for Compose {
    fn default() -> Self {
        Compose {
            version: String::from("3.0"),
            services: HashMap::new(),
            networks: HashMap::new()
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Service {
    pub cap_add: Vec<String>,
    pub command: String,
    pub container_name: String,
    pub image: String,
    pub networks: HashMap<String, HashMap<String, String>>,
    // pub network_mode: String,
    pub volumes: Vec<String>,
    pub working_dir: String,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Network {
    pub driver: String,
    pub ipam: HashMap<String, Vec<HashMap<String, String>>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 5 {
        eprintln!("insufficient args (args: slave_num master_configuration_template_path slave_configuration_template_path event_path");
        std::process::exit(-1);
    }

    // init terminal logger
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog::o!());

    // generate master configuration
    use std::path::Path;
    let runtime_config_dir = "./config/runtime";
    let runtime_config_dir = Path::new(runtime_config_dir);
    if !runtime_config_dir.exists() {
        std::fs::create_dir_all(runtime_config_dir)?;
    }
    let template = config::load_server_config(&args[2]);
    let master_addr = template.addr.clone();
    let master_configuration = serde_json::to_string(&template)?;
    let mut file =
        File::create("./config/runtime/master.json").expect("failed to create master configuration file");
    file.write_all(master_configuration.as_bytes())?;
    slog::info!(logger, #"configuration", "master configuration constructed";
        "listen at" => &master_addr,
    );

    // generate slave configurations
    let event_path = Path::new(&args[4]);
    let event_file_list = if event_path.is_dir() {
        let mut event_file_list = Vec::new();
        for entry in std::fs::read_dir(event_path)? {
            let path = entry?.path();
            if !path.is_dir() {
                event_file_list.push(path.as_path().to_owned());
            }
        }
        event_file_list
    } else {
        vec![event_path.to_owned()]
    };
    let slave_n: usize = args[1].parse()?;
    if slave_n < template.group_num as usize || slave_n >= u8::MAX as usize {
        eprintln!("the number of slaves should be no less than the group_num in master configuration and no bigger than 255");
        std::process::exit(-2);
    }
    if event_file_list.len() > 1 && slave_n > event_file_list.len() {
        eprintln!("the number of slaves should equals to the number of given event files");
        std::process::exit(-2);
    }
    let mut template = config::load_client_config(&args[3]);
    template.addrs.remote_addr = format!("http://{}", master_addr);
    let mailbox_port:Vec<&str> = template.addrs.mailbox_addr.split(':').collect();
    let mailbox_port = mailbox_port[1];
    for i in 1..=slave_n {
        let file_path = if event_file_list.len() == 1 {
            format!("./config/runtime/slave_{}.json", i)
        } else {
            let filename = event_file_list[i-1].file_name().unwrap();
            let filename = filename.to_owned().into_string().unwrap();
            format!("./config/runtime/{}.json", filename.split('.').next().unwrap())
        };
        let mut config = template.clone();
        let ipv4_last_byte = 1 + i;
        config.addrs.mailbox_addr = format!("192.168.0.{}:{}", ipv4_last_byte, mailbox_port);
        // let event_file_path = event_file_list[i-1].file_name().unwrap();
        // let event_file_path = event_file_path.to_owned().into_string().unwrap();
        let event_file_path = event_file_list[i-1].to_str().unwrap().to_string();
        config.event_file_path = event_file_path.replace("\\", "/");

        let mut file = File::create(file_path).expect("failed to create slave configuration files");
        let config = serde_json::to_string(&config)?;
        file.write_all(config.as_bytes())?;
    }
    slog::info!(logger, #"configuration", "slave configurations constructed");

    // generate docker-compose file
    let project_path_in_docker = "/root/FedTruthFinderImplOffline";
    let docker_image_tag = "fedtruthfinder:0.1.3-main"; // TODO: from args
    let mut subnet = HashMap::with_capacity(1);
    subnet.insert("subnet".into(), "192.168.0.0/16".into());
    subnet.insert("gateway".into(), "192.168.0.254".into());
    let ipam_config = vec![subnet];
    let mut ipam = HashMap::new();
    ipam.insert("config".into(), ipam_config);
    let test_network_config = Network {
        driver: "bridge".into(),
        ipam,
    };
    let mut networks = HashMap::with_capacity(1);
    networks.insert("test".into(), test_network_config);

    let mut services = HashMap::with_capacity(slave_n + 2);
    let mut master_service = Service::default();
    master_service.container_name = "master".into();
    master_service.image = String::from(docker_image_tag); 
    let mut test_network_config = HashMap::with_capacity(1);
    test_network_config.insert("ipv4_address".into(), "192.168.0.1".into());
    let mut network = HashMap::with_capacity(1);
    network.insert("test".into(), test_network_config);
    master_service.networks = network;
    // master_service.network_mode = String::from("host");
    master_service.volumes = vec![String::from(format!("{}:{}", std::env::current_dir()?.display(), project_path_in_docker))];
    master_service.working_dir = project_path_in_docker.into();
    master_service.command = String::from("master ./config/runtime/master.json ./log/master.log");
    services.insert("master".into(), master_service);

    for i in 1..=slave_n {
        let name = if event_file_list.len() == 1 {
            format!("slave_{}", i)
        } else {
            let filename = event_file_list[i-1].file_name().unwrap();
            let filename = filename.to_owned().into_string().unwrap();
            filename[0..(filename.len()-4)].into()
        };
        let mut slave_service = Service::default();
        slave_service.container_name = String::from(&name);
        slave_service.image = String::from(docker_image_tag);

        let mut test_network_config = HashMap::with_capacity(1);
        test_network_config.insert("ipv4_address".into(), format!("192.168.0.{}", 1 + i));
        let mut network = HashMap::with_capacity(1);
        network.insert("test".into(), test_network_config);
        slave_service.networks = network;
        slave_service.volumes = vec![format!("{}:{}", std::env::current_dir()?.display(), project_path_in_docker)];
        slave_service.working_dir = String::from(project_path_in_docker);

        // let event_file_path = if event_file_list.len() == 1 {
        //     event_file_list[0].to_str().unwrap()
        // } else {
        //     event_file_list[i-1].to_str().unwrap()
        // };

        slave_service.command = format!(
            "sh -c \"slave ./config/runtime/{}.json ./log/{}.log\"",
            &name, &name
        );
        slave_service.depends_on.push("master".into());
        services.insert(name, slave_service);
    }

    let compose = Compose {
        version: String::from("3.0"),
        services,
        networks
    };

    let log_dir_path = "./log";
    if !std::path::Path::new(log_dir_path).exists() {
        std::fs::create_dir_all(log_dir_path)?; // construct a dir for log
    }
    let docker_compose_filename = "docker-compose.yml";
    let mut file = File::create(&docker_compose_filename).expect("failed to create docker-compose file");
    writeln!(&mut file, "{}", serde_yaml::to_string(&compose)?)?;
    slog::info!(logger, #"configuration", "docker-compose file generated";
        "filename" => &docker_compose_filename,
    );

    Ok(())
}
