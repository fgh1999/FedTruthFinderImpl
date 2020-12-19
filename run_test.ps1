# prepare a dir for logging
try {
    New-Item ./log -type directory -ErrorAction Stop
} catch {
    # do nothing
}

# parse slave_num
[string]$slave_num_str = $args[0]
[int]$slave_num = [convert]::ToInt32($slave_num_str, 10)

# build configurations
Start-Process configuration_generator -ArgumentList $slave_num, './config/master_template.json', './config/slave_template.json' -Wait -NoNewWindow

# start master
Start-Process master -ArgumentList './config/master.json', './log/master.log' -RedirectStandardOutput './log/master.out' -RedirectStandardError './log/master.err' -NoNewWindow
Start-Sleep -s 1

# start slaves
for ([int]$i = 1; $i -le $slave_num; $i++) {
    [string]$config_path = "./config/slave_{0}.json" -f $i
    [string]$log_path = "./log/slave_{0}.log" -f $i
    [string]$out_path = "./log/slave_{0}.out" -f $i
    [string]$err_path = "./log/slave_{0}.err" -f $i
    Start-Process slave -ArgumentList $config_path, $log_path -RedirectStandardOutput $out_path -RedirectStandardError $err_path -NoNewWindow
}
Start-Sleep -s 1

# start to broadcast raw events, and wait until it ends
[string]$event_n_limitation = $args[1]
Start-Process raw_event_broadcaster -ArgumentList $event_n_limitation, "./config/listeners.txt", "./config/event/raw_event_records_1000.csv" -Wait
Start-Sleep -s 10 # wait long enough for master & slaves finishing their logging

# kill master & slaves
Get-Process -Name slave | foreach-object{$_.Kill()}
Get-Process -Name master | foreach-object{$_.Kill()}  