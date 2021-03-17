#!/usr/bin/env python3

import sys,os,getopt
import traceback
import os
import re
import datetime
import subprocess

sys.path.insert(0, 'ds-integration')
from DefenseStorm import DefenseStorm


class integration(object):

    def get_iftopInfo(self):
        command = subprocess.Popen('iftop -N -n -t -s 1', shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.PIPE)
        output, error = command.communicate()
        lines = output.decode("utf-8").split('\n')[3:]
        connections = []
        totals = {}
        conn_info = []
        for line in lines:
            if '=>' in line or '<=' in line:
                connections.append(line)
            if 'Total send rate' in line:
                totals['send_rate'] = {}
                tmp_date = line.strip('Total send rate:').split()
                totals['send_rate']['2'] = tmp_date[0]
                totals['send_rate']['10'] = tmp_date[1]
                totals['send_rate']['40'] = tmp_date[2]
            if 'Total receive rate:' in line:
                totals['receive_rate'] = {}
                tmp_date = line.strip('Total receive rate:').split()
                totals['receive_rate']['2'] = tmp_date[0]
                totals['receive_rate']['10'] = tmp_date[1]
                totals['receive_rate']['40'] = tmp_date[2]
        First = True
        for connection in connections:
            if First == True:
                if '<=' in connection:
                    print('this shouldnt happen')
                this_conn = {}
                sentdata = connection.split()
                this_conn['local_ip'] = sentdata[1]
                this_conn['sent'] = {}
                this_conn['sent']['2'] = sentdata[3]
                this_conn['sent']['10'] = sentdata[4]
                this_conn['sent']['40'] = sentdata[5]
                this_conn['sent']['cumulative'] = sentdata[6]
                First = False
            else:
                if '=>' in connection:
                    print('this shouldnt happen')
                sentdata = connection.split()
                received_data = connection.split()
                this_conn['remote_ip'] = sentdata[0]
                this_conn['received'] = {}
                this_conn['received']['2'] = sentdata[2]
                this_conn['received']['10'] = sentdata[3]
                this_conn['received']['40'] = sentdata[4]
                this_conn['received']['cumulative'] = sentdata[5]
                conn_info.append(this_conn)
                First = True

        return conn_info, totals

    def get_udpBufferInfo(self):
        command = subprocess.Popen('cat /proc/net/udp', shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.PIPE)
        output, error = command.communicate()
        lines = output.decode("utf-8").split('\n')
        item_list=[]
        for line in lines:
            if line == "" or 'sl' in line:
                continue
            item = {}
            info = line.split()
            item['local_address'] = info[1]
            item['remote_address'] = info[2]
            item['tx_queue'], item['rx_queue'] = info[4].split(':')
            item['drops'] = info[12]
            if (item['tx_queue'] == '00000000') and (item['rx_queue'] == '00000000') and (item['drops'] == '0'):
                continue
            item_list.append(item)
        return item_list

    def get_udpConnectionCounts(self,timeout=2):

        command = subprocess.Popen('timeout %d tcpdump -n port 516 and udp' %timeout, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.PIPE)
        output, error = command.communicate()
        lines = output.decode("utf-8").split('\n')
        ip_list = {}

        for line in lines:
            try:
                info = line.split(' ')
                ip_addr = info[2]
            except:
                continue
            if ip_addr not in ip_list.keys():
                ip_list[ip_addr] = int(info[7])
            else:
                ip_list[ip_addr] += int(info[7])
        return ip_list 


    def getUDPErrors(self):
        netstat = subprocess.Popen('netstat -us', shell=True, stdout = subprocess.PIPE)
        output, error = netstat.communicate()
        lines = output.decode("utf-8").split('\n')
        err_count = None
        err_count2 = None
        for line in lines:
            if "packet receive errors" in line:
                err_count = line.split()
            if "receive buffer errors" in line:
                err_count2 = line.split()
        ret_val = {}
        ret_val['received_packets'] = int(err_count[0])
        ret_val['error'] = int(err_count2[0])
        return ret_val


    def run(self):

        event_data = self.getUDPErrors()
        if event_data['error'] == 0 and event_data['received_packets'] == 0:
            msg = 'No Network Errors Detected, received_packets = "packet receive err", error = "receive buffer errors"'
        else:
            msg = 'Network Errors Detected, received_packets = "packet receive err", error = "receive buffer errors"'
        event_data['message'] = msg
        self.ds.writeJSONEvent(event_data)

        conns, totals = self.get_iftopInfo()
        totals['message'] = 'iftop Totals'
        self.ds.writeJSONEvent(totals)
        for conn in conns:
            conn['message'] = 'iftop connection'
            self.ds.writeJSONEvent(totals)

        list = self.get_udpConnectionCounts(timeout=5)
        if len(list) == 0:
            item = {}
            item['message'] = 'No UDP Connection Counts'
            self.ds.writeJSONEvent(item)
        else:
            for item in list:
                event = {}
                event['message'] = 'UDP Connection Counts'
                event['details'] = item
                self.ds.writeJSONEvent(event)

        list = self.get_udpBufferInfo()
        if len(list) == 0:
            item = {}
            item['message'] = 'No UDP Buffer Info'
            self.ds.writeJSONEvent(item)
        else:
            for item in list:
                event = {}
                event['message'] = 'UDP Buffer Info'
                event['details'] = item
                self.ds.writeJSONEvent(item)


    def usage(self):
        print()
        print(os.path.basename(__file__))
        print()
        print('  No Options: Run a normal cycle')
        print()
        print('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print('        in the current directory')
        print()
        print('  -l    Log to stdout instead of syslog Local6')
        print()
    
    def __init__(self, argv):

        self.testing = False
        self.send_syslog = True
        self.ds = None
    
        try:
            opts, args = getopt.getopt(argv,"htnld:",["datedir="])
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-l"):
                self.send_syslog = False
    
        try:
            self.ds = DefenseStorm('monitorNetstats', testing=self.testing, send_syslog = self.send_syslog)
        except Exception as e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass



if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
