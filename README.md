# FedTruthFinderImpl

An implementation of FedTruthFinder.

# Configuration

## Master Configuration

A master-side configuration is a json file following such scheme:

```json
{
    "addr": "[::1]:50050",
    "keys": {
        "ca_path": "config/tls/client_ca.pem",
        "sk_path": "config/tls/server.key",
        "pk_path": "config/tls/server.pem"
    },
    "group_num": 3
}
```

* `addr`: the IP address and port that it will listen on
* `group_num`: the number of groups which should be an odd number (not less than 3).
* `keys`: reserved


## Slave Configuration

A slave-side configuration is a json file following such scheme:

```json
{
    "addrs": {
        "domain_name": "localhost",
        "remote_addr": "http://[::1]:50050",
        "mailbox_addr": "[::1]:50051"
    },
    "keys": {
        "sk_path": "config/tls/client1.key",
        "pk_path": "config/tls/client1.pem",
        "ca_path": "config/tls/ca.pem"
    },
    "error_rate": 0.25
}
```

* `addrs`
  * `remote_addr`: the url of its master
  * `mailbox_addr`: the IP address and port that it will listen on
  * `domain_name`: reserved
* `error_rate`: assuming that one event can be judged as `True` or `False` and `True` is the *'fact'*, the possibility that the slave judge a event as `False`
* `keys`: reserved

## Raw Event Broadcaster

### Listener List

Urls listed in lines.
Raw event broadcaster use these urls to dispatch event identifier to slaves.

For instance.

```
http://[::1]:50051
http://[::1]:50052
http://[::1]:50053
http://[::1]:50054
http://[::1]:50055
http://[::1]:50056
```

### Raw Event Records

Raw event records are provided through a csv file following such scheme:

```csv
identifier,delay_seconds
1,3
2,3
3,5
4,5
...,...
```

* `identifier`: a string that can universally represents an event
* `delay_seconds`: a positive number that indicates the internal after the next event

# Usage

## Preliminary

### Rust Language

1. install rust language, here's an [instruction](https://www.rust-lang.org/zh-CN/learn/get-started). It would help if you learnt a little bit about usages of `cargo`, the biuld & package management tool of rust language.
1. git clone this repository.
1. cd into this repository and install crates with `cargo install --path . --bins`.

**note**: You'd better check if `$HOME/.cargo/bin` is in your `$PATH` (system environment variable), or may not be able to run the programs installed on the previous step.

### A Network Protocol Analyzer

For instance, [wireshark](https://www.wireshark.org/).

## Obtain Data Packages

### prepare configurations

Have configuration files in the directory that you will run tests.

### start to capture

Take wireshark as an example. Since all the transmissions of these programs are based on tcp,
you can assign a packet filter like `tcp portrange 50050-50057`.

![packet filter](./img/packet_filter.png)

### master

Enter this line in your cmd/shell to run the *master*:

```shell
master config_path[, log_path]
```

* `config_path`: Path of the configuration file
* `log_path`(optional): Expected file path of output logs. If not specified, log will be output to your current terminal. However, this is not recommended because the output may be too long to avoid dropping by the terminal which means that you may loss some logs.

For instance, `master ./config/server.json ./result/server.log`.

**WARN**: Existing log files will be overwritten.

### slaves

Enter this line in your cmd/shell to run a *slave*:

```shell
slave config_path[, log_path]
```

`config_path` and 'log_path' have the same meanings as in [run master](#master). Unfortunately, there's not a shell script provided for easing the startup of slaves at this moment, although it has been on the schedule. Hence, you have to manully enter these lines to run slaves.

For instance, `slave ./config/client_1.json ./result/client_1.log`.

**WARN**: Existing log files will be overwritten.

### run raw event broadcaster

Enter this line in your cmd/shell to run a *raw event broadcaster*:

```shell
raw_event_broadcaster event_num_limitation listeners_url_path event_data_path
```

* `event_num_limitation`: 
* `listeners_url_path`: 
* `event_data_path`: 

For instance, `raw_event_broadcaster 200 config\event\listeners.txt config\event\raw_event_records_1000.csv`.

### end the capture

After having logged the last line (where the `eid` equals to min(the record maximum limit, the length of provided raw events)), the raw event broadcaster ends itself. Then, after about 3 seconds, you can:

1. kill the server;
1. kill the slaves;
1. stop capturing network packets and save them if needed.

## Example Result

If you have followed all the instructions above, you may get a result similar with 
the example one restored in `./result` where you can find:

* [a wireshark packet set](./result/200.pcapng)
* six slave-side log files named like `client_x.log`
* a master-side log file named `server.log`
