import hyperion
import asyncio
import numpy as np
import time
import sqlite3
import csv

instrument_ip = '10.0.0.55'
num_of_peaks = 8
num_of_ports = 8
streaming_time = 11

async def get_data(con):
    repeat = time.time()
    while True:
        if time.time()-repeat < 5:
            peak_num = []
            begin = time.time()

            while time.time()-begin < .08:
                peak_data = await queue.get()
                queue.task_done()
                if peak_data['data']:
                    peak_num.append(list(peak_data['data'].data))
                else:
                    return

            ts = time.time()
            sensors_num = []
            for port_list in peak_data['data'].channel_slices[:num_of_ports]:
                sensors_num.append(len(port_list))
            sensors_num.insert(0, ts)

            average_peak_num = []
            for peak in range(len(peak_num[0])):
                current_sensor = []
                for data_list in peak_num:
                    current_sensor.append(data_list[peak])
                average_peak_num.append(np.mean(current_sensor))

            add_data(con, sensors_num, average_peak_num)

        else:
            export_csv(con)
            repeat = time.time()

def add_data(con, data, peak_data):
    with con:
        cur.execute(peak_sql, peak_data)
        data.insert(0, cur.lastrowid)
        cur.execute(data_sql, data)

def export_csv(con):
    for table in database_tables:
        cur.execute("select * from "+table+";")
        with open('csv/'+table+".csv", "w", newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in cur.description])
            csv_writer.writerows(cur)

def create_table(con, create_table_sql):
    with con:
        cur.execute(create_table_sql)

peak_question = ','.join('?' * (num_of_peaks))
data_question = ','.join('?' * (num_of_ports+2))
peak_parameters = ','.join('peak'+str(i) for i in range(1,num_of_peaks+1))
data_parameters = ','.join('port'+str(i) for i in range(1,num_of_ports+1))
peak_table_variables = ','.join('peak'+str(i)+' float UNSIGNED' for i in range(1,num_of_peaks+1))
data_table_variables = ','.join('port'+str(i)+' smallint UNSIGNED' for i in range(1,num_of_ports+1))

create_peak_data_table = "create table if not exists peak_data (id integer PRIMARY KEY,{});".format(peak_table_variables)
create_data_table = "create table if not exists data (id integer PRIMARY KEY,peak_data_id integer NOT NULL,"+\
"timestamp double NOT NULL,{},FOREIGN KEY (peak_data_id) REFERENCES peak_data (id));".format(data_table_variables)

peak_sql = 'insert into peak_data({parameters}) VALUES({question})'.format(parameters = peak_parameters, question = peak_question)
data_sql = 'insert into data(peak_data_id,timestamp,{parameters}) VALUES({question})'.format(parameters = data_parameters, question = data_question)

database_tables = ('peak_data','data')

con = sqlite3.connect('database/peak_data.db')
cur = con.cursor()

if con:
    create_table(con, create_peak_data_table)
    create_table(con, create_data_table)
else:
    raise Exception('Cannot create database connection.')

loop = asyncio.get_event_loop()
queue = asyncio.Queue(maxsize=5, loop=loop)
stream_active = True

peaks_streamer = hyperion.HCommTCPPeaksStreamer(instrument_ip, loop, queue)

loop.create_task(get_data(con))
loop.call_later(streaming_time, peaks_streamer.stop_streaming)
loop.run_until_complete(peaks_streamer.stream_data())