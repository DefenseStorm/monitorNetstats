#!/usr/bin/env python3

import sys,os
import csv
from datetime import datetime


def usage():
    print('\nUsage:\n')
    print('parse_csv.py <filename>')
    print('')
    print('Where <filename> is the downloaded csv from from a search "app_name:monitorNetstat"')
    print('')

try:
    if not os.path.exists(sys.argv[1]):
        print('\nFile not found: ' + sys.argv[1])
        usage()
except:
    usage()
    sys.exit()

prefix = sys.argv[1].split('.')[0]

with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    udp_buffer_info = []
    iftop_totals = []
    iftop_connections = []
    udp_network_errors = []
    consolidated_data = {}
    for row in csv_reader:
        if line_count == 0:
            #print(f'Column names are {", ".join(row)}')
            header = {}
            counter = 0
            for item in row:
                if item == 'additional_fields':
                    header['additional_fields'] = counter
                elif item == 'timestamp':
                    header['timestamp'] = counter
                elif item == 'received_packets':
                    header['received_packets'] = counter
                elif item == 'error':
                    header['error'] = counter
                elif item == 'hostname':
                    header['hostname'] = counter
                elif item == 'message':
                    header['message'] = counter
                counter += 1
                    
            line_count += 1
        else:
            timestamp = row[header['timestamp']]
            date_str = datetime.fromtimestamp(int(timestamp) / 1000.0).strftime('%Y-%m-%d %H:%M')
            if date_str not in consolidated_data.keys():
                consolidated_data[date_str] = {}
            message = row[header['message']]
            hostname = row[header['hostname']]
            #print(message)
            line_count += 1
            entry = {}
            entry['timestamp'] = timestamp
            entry['hostname'] = hostname
            if message == 'UDP Buffer Info':
                additional_fields = row[header['additional_fields']][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                entry['tx_queue'] = int('0x' + str(entry['tx_queue']), 0)
                entry['rx_queue'] = int('0x' + str(entry['rx_queue']), 0)
                consolidated_data[date_str]['tx_queue'] = entry['tx_queue']
                consolidated_data[date_str]['rx_queue'] = entry['rx_queue']
                udp_buffer_info.append(entry)
            elif message == 'iftop Totals':
                continue
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[header['additional_fields']][+1:-1].split(',')
                for item in additional_fields:
                    #print("\"" + item + "\"")
                    if item == "":
                        continue
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                    if 'Kb' in entry[name]:
                        entry[name] = float(entry[name].strip('Kb')) * 128
                    elif 'KB' in entry[name]:
                        entry[name] = float(entry[name].strip('KB')) * 1024
                    elif 'Mb' in entry[name]:
                        entry[name] = float(entry[name].strip('Mb')) * 131072
                    elif 'MB' in entry[name]:
                        entry[name] = float(entry[name].strip('MB')) * 1048576
                    elif 'b' in entry[name]:
                        entry[name] = float(entry[name].strip('b'))
                        if entry[name] > 0:
                            entry[name] = entry[name] / 8
                    elif 'B' in entry[name]:
                        entry[name] = float(entry[name].strip('B'))
                    consolidated_data[date_str][name] = entry[name]
                iftop_totals.append(entry)
            elif message == 'UDP Connection Counts':
                #Need to finish this one
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[header['additional_fields']][+1:-1].split(',')
                for item in additional_fields:
                    name, value = item.split('=')
                    if name in ['port']:
                        entry[name] = value
                    else:
                        entry['sourceip_port'] = name
                        entry['received_packets'] = value

                #iftop_totals.append(entry)
            elif message == 'iftop connection':
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[header['additional_fields']][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                    if 'Kb' in entry[name]:
                        entry[name] = float(entry[name].strip('Kb')) * 128
                    elif 'KB' in entry[name]:
                        entry[name] = float(entry[name].strip('KB')) * 1024
                    elif 'Mb' in entry[name]:
                        entry[name] = float(entry[name].strip('Mb')) * 131072
                    elif 'MB' in entry[name]:
                        entry[name] = float(entry[name].strip('MB')) * 1048576
                    elif 'b' in entry[name]:
                        entry[name] = float(entry[name].strip('b'))
                        if entry[name] > 0:
                            entry[name] = entry[name] / 8
                    elif 'B' in entry[name]:
                        entry[name] = float(entry[name].strip('B'))
                        
                iftop_connections.append(entry)
            elif 'Network Errors Detected' in message:
                entry = {}
                entry['timestamp'] = timestamp
                entry['receive_buffer_errors'] = row[header['received_packets']]
                entry['packet_receive_err'] = row[header['error']]
                consolidated_data[date_str]['receive_buffer_errors'] = entry['receive_buffer_errors']
                consolidated_data[date_str]['packet_receive_err'] = entry['packet_receive_err']
                udp_network_errors.append(entry)
            elif message == 'Syslog Stats':
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[header['additional_fields']][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                    consolidated_data[date_str][name] = value

keys = udp_buffer_info[0].keys()
with open(prefix + '_' + 'udp_buffer_info.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(udp_buffer_info)

'''
keys = iftop_totals[0].keys()
with open(prefix + '_' + 'iftop_totals.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(iftop_totals)
keys = iftop_connections[0].keys()
with open(prefix + '_' + 'iftop_connections.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(iftop_connections)
'''

keys = udp_network_errors[0].keys()
with open(prefix + '_' + 'udp_network_errors.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(udp_network_errors)


consolidated_list = []
prev_value = 0
for key in consolidated_data.keys():
    item = {}
    item['date_time'] = key
    print(len(consolidated_data[key].keys()))
    if (len(consolidated_data[key].keys()) == 15):
        consolidated_data[key]['rx_queue'] = -1
        consolidated_data[key]['tx_queue'] = -1
    elif (len(consolidated_data[key].keys()) != 21):
        print(len(consolidated_data[key].keys()))
        print(consolidated_data[key].keys())
        print('skipping')
        continue
    for thisitem in consolidated_data[key].keys():
        item[thisitem] = consolidated_data[key][thisitem]
        if thisitem == 'packet_receive_err':
            item['packet_receive_err_increase'] = prev_value - int(consolidated_data[key][thisitem])
            prev_value = int(item[thisitem])
    consolidated_list.append(item)

keys = consolidated_list[0].keys()
with open(prefix + '_' + 'consolidated.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(consolidated_list)


