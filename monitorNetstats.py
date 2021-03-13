#!/usr/bin/env python

import sys,os,getopt
import traceback
import os
import re
import datetime
import subprocess

sys.path.insert(0, 'ds-integration')
from DefenseStorm import DefenseStorm


class integration(object):

    def getUDPErrors(self):
        netstat = subprocess.Popen(["netstat", "-us"], stdout = subprocess.PIPE)
        output = netstat.communicate()[0]
        lines = output.split('\n')
        err_count = None
        err_count2 = None
        for line in lines:
            if "packet receive errors" in line:
                err_count = line.split()
            if "receive buffer errors" in line:
                err_count2 = line.split()
        ret_val = {}
        ret_val['error'] = int(err_count2[0])
        ret_val['received_packets'] = int(err_count2[0])
        return ret_val


    def run(self):

        event_data = self.getUDPErrors()
        if event_data['error'] == 0 and event_data['received_packets'] == 0:
            msg = 'No Network Errors Detected, received_packets = "packet receive err", error = "receive buffer errors"'
        else:
            msg = 'Network Errors Detected, received_packets = "packet receive err", error = "receive buffer errors"'
        event_data['message'] = msg
        self.ds.writeJSONEvent(event_data)

    def usage(self):
        print
        print os.path.basename(__file__)
        print
        print '  No Options: Run a normal cycle'
        print
        print '  -t    Testing mode.  Do all the work but do not send events to GRID via '
        print '        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\''
        print '        in the current directory'
        print
        print '  -l    Log to stdout instead of syslog Local6'
        print
    
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
