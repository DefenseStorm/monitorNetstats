
import csv

with open('DefenseStorm-search (1).csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    udp_buffer_info = []
    iftop_totals = []
    iftop_connections = []
    udp_network_errors = []
    for row in csv_reader:
        if line_count == 0:
            #print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            timestamp = row[17]
            message = row[9]
            hostname = row[6]
            #print(message)
            line_count += 1
            entry = {}
            entry['timestamp'] = timestamp
            entry['hostname'] = hostname
            if message == 'UDP Buffer Info':
                additional_fields = row[0][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                udp_buffer_info.append(entry)
            elif message == 'iftop Totals':
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[0][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                iftop_totals.append(entry)
            elif message == 'UDP Connection Counts':
                #Need to finish this one
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[0][+1:-1].split(',')
                for item in additional_fields:
                    name, value = item.split('=')
                    entry['sourceip_port'] = value
                    entry['received_packets'] = row[11]
                    entry['received_packets'] = row[11]

                #iftop_totals.append(entry)
            elif message == 'iftop connection':
                entry = {}
                entry['timestamp'] = timestamp
                additional_fields = row[0][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                iftop_connections.append(entry)
            elif 'Network Errors Detected' in message:
                entry = {}
                entry['timestamp'] = timestamp
                '''
                additional_fields = row[0][+1:-1].split(',')
                for item in additional_fields:
                    item = item.strip().split('details_')[1]
                    name, value = item.split('=')
                    entry[name] = value
                '''
                entry['receive_buffer_errors'] = row[5]
                entry['packet_receive_err'] = row[11]
                udp_network_errors.append(entry)

keys = udp_buffer_info[0].keys()
with open('udp_buffer_info.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(udp_buffer_info)

keys = iftop_totals[0].keys()
with open('iftop_totals.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(iftop_totals)

keys = iftop_connections[0].keys()
with open('iftop_connections.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(iftop_connections)

keys = udp_network_errors[0].keys()
with open('udp_network_errors.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(udp_network_errors)

#print(udp_buffer_info)
#print(iftop_totals)

