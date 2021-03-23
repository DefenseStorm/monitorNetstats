Monitor DVM for UDP Network Errors for DefenseStorm

NOTE - This should be installed in /usr/local and should be run as root.

NOTE - This integration requires:
	tcpdump
	iftop
	netstat


to pull this repository and submodules:

git clone --recurse-submodules https://github.com/DefenseStorm/monitorNetstats.git

1. If this is the first integration on this DVM, Do the following:

  cp ds-integration/ds_events.conf to /etc/syslog-ng/conf.d

  Edit /etc/syslog-ng/syslog-ng.conf and add local7 to the excluded list for filter f_syslog3 and filter f_messages. The lines should look like the following:

filter f_syslog3 { not facility(auth, authpriv, mail, local7) and not filter(f_debug); };

filter f_messages { level(info,notice,warn) and not facility(auth,authpriv,cron,daemon,mail,news,local7); };


  Restart syslog-ng
    service syslog-ng restart

2. Add the following entry to the root crontab so the script will run every
    minute (or what you think is appropriate)

   * * * * * cd /usr/local/monitorNetstats; ./monitorNetstats.py




To use the included parseCSV.py script:

1) use the query "app_name:monitorNetstat" to query the data. You can also filter to a single DVM (with api_key) as well as filter the timeframe.  Do not filter the data beyond that as the parse script will fail.

2) Run the parseCSV.py scirpt against the data by providing the name of the CSV file from GRID.  The parsing script will write out 4 files:
- <CSV_NAME>_iftop_connections.csv
- <CSV_NAME>_udp_buffer_info.csv
- <CSV_NAME>_iftop_totals.csv
- <CSV_NAME>_udp_network_errors.csv

into the current directory.

