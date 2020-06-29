import hyperion
import asyncio
import numpy as np
import time
import sqlite3
from sqlite3 import Error

instrument_ip = '10.0.0.55'    # Enter the correct IP address
num_of_peaks = 8               # Required
num_of_ports = 8               # Required for different machines
streaming_time = 1             # Time increments written to the database INCREASE LENGTH
database = "peak_data.db"      # Name the database

async def get_data(conn):
    while True:
        peak_num = []
        sensors_num = []
        for i in range(10):
            peak_data = await queue.get()
            queue.task_done()
            if peak_data['data']:
                peak_num.append(list(peak_data['data'].data))
                if i == 0:
                    ts = time.time()
                    sensors_num = []
                    for port_list in peak_data['data'].channel_slices[:num_of_ports]:
                        sensors_num.append(len(port_list))
                    sensors_num.insert(0, ts)
            else:
                break

        average_peak_num = []

        for peak in range(len(peak_num[0])):
            current_sensor = []
            for data_list in peak_num:
                current_sensor.append(data_list[peak])
            average_peak_num.append(np.mean(current_sensor))

        with conn:
            add_data(conn, sensors_num, average_peak_num)

def add_data(conn, data, peak_data):
    data_sql = 'INSERT INTO data(peak_data_id,timestamp,{parameters}) VALUES({question})'.format(parameters = data_parameters, question = data_question)
    peak_sql = 'INSERT INTO peak_data({parameters}) VALUES({question})'.format(parameters = peak_parameters, question = peak_question)

    cur = conn.cursor()
    cur.execute(peak_sql, peak_data) # Multiple rows at once?
    data.insert(0, cur.lastrowid)
    cur.execute(data_sql, data)

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

peak_question = ','.join('?' * (num_of_peaks))
data_question = ','.join('?' * (num_of_ports+2))
peak_parameters = ','.join('peak'+str(i) for i in range(1,num_of_peaks+1))
data_parameters = ','.join('port'+str(i) for i in range(1,num_of_ports+1))
peak_table_variables = ','.join('peak'+str(i)+' float UNSIGNED' for i in range(1,num_of_peaks+1))
data_table_variables = ','.join('port'+str(i)+' smallint UNSIGNED' for i in range(1,num_of_ports+1))

create_peak_data_table = "CREATE TABLE IF NOT EXISTS peak_data (id integer PRIMARY KEY,{});".format(peak_table_variables)

create_data_table = "CREATE TABLE IF NOT EXISTS data (id integer PRIMARY KEY,peak_data_id integer NOT NULL,"+\
"timestamp double NOT NULL,{},FOREIGN KEY (peak_data_id) REFERENCES peak_data (id));".format(data_table_variables)

conn = create_connection(database)

if conn:
    create_table(conn, create_peak_data_table)
    create_table(conn, create_data_table)
else:
    raise Exception("Cannot create the database connection.")

for i in range(2):
    print('start stream')
    loop = asyncio.get_event_loop() # Loop the loop
    queue = asyncio.Queue(maxsize=5, loop=loop)
    stream_active = True

    peaks_streamer = hyperion.HCommTCPPeaksStreamer(instrument_ip, loop, queue)

    loop.create_task(get_data(conn))

    loop.call_later(streaming_time, peaks_streamer.stop_streaming)

    loop.run_until_complete(peaks_streamer.stream_data())

    print('repeat stream')