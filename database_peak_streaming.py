import hyperion
import asyncio
import numpy
import time
import sqlite3
import csv
import os

instrument_ip = '10.0.0.55'
num_of_peaks = 8
num_of_ports = 8
st_length = 30 # A week
lt_increment = 100 # A minute
streaming_time = 201 # Infinite

async def get_data(con):
    repeat = time.time()
    st_data=[]; st_peak=[]; lt_data=[]; lt_peak=[]
    while True:
        if time.time()-repeat < 10: # Every day/hour
            peak_num = []
            begin = time.time()
            while time.time()-begin < .097:
                peak_data = await queue.get()
                queue.task_done()
                if peak_data['data']:
                    peak_num.append(list(peak_data['data'].data))
                else:
                    return

            sensors_num = []
            for port_list in peak_data['data'].channel_slices[:num_of_ports]:
                sensors_num.append(len(port_list))
            sensors_num.insert(0, time.time())

            st_data.append(sensors_num)

            average_peak_num = []
            for peak in range(len(peak_num[0])):
                current_sensor = []
                for data_list in peak_num:
                    current_sensor.append(data_list[peak])
                average_peak_num.append(numpy.mean(current_sensor))

            st_peak.append(average_peak_num)

        else:
            repeat = time.time()
            add_data(st_data_sql, st_peak_sql, st_data, st_peak)
            delete_st_data(repeat)

            for data in st_data[::lt_increment]:    
                lt_data.append(data)
            for peak in st_peak[::lt_increment]:
                lt_peak.append(peak)
            add_data(lt_data_sql, lt_peak_sql, lt_data, lt_peak)
            
            st_data=[]; st_peak=[]; lt_data=[]; lt_peak=[]
            export_csv()

def add_data(insert_data, insert_peak, data, peak):
    with con:
        cur.executemany(insert_data, data)
        cur.executemany(insert_peak, peak)

def delete_st_data(current_time):
    with con:
        cur.execute('delete from st_data where '+str(current_time)+'-timestamp > '+str(st_length))
        data_id = cur.execute('select id from st_data limit 1').fetchone()
        cur.execute('delete from st_peak where id < '+str(data_id[0]))

def export_csv():
    for table in database_tables:
        cur.execute('select * from '+table+';')
        with open('csv/'+table+'.csv', 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in cur.description])
            csv_writer.writerows(cur)

data_question = ','.join('?' * (num_of_ports+1))
peak_question = ','.join('?' * (num_of_peaks))
data_parameters = ','.join('port'+str(i) for i in range(1,num_of_ports+1))
peak_parameters = ','.join('peak'+str(i) for i in range(1,num_of_peaks+1))
data_table_variables = ','.join('port'+str(i)+' smallint UNSIGNED' for i in range(1,num_of_ports+1))
peak_table_variables = ','.join('peak'+str(i)+' float UNSIGNED' for i in range(1,num_of_peaks+1))

st_create_data_table = 'create table if not exists st_data (id integer PRIMARY KEY,timestamp double NOT NULL,'+data_table_variables+');'
st_create_peak_table = 'create table if not exists st_peak (id integer PRIMARY KEY,'+peak_table_variables+');'
lt_create_data_table = 'create table if not exists lt_data (id integer PRIMARY KEY,timestamp double NOT NULL,'+data_table_variables+');'
lt_create_peak_table = 'create table if not exists lt_peak (id integer PRIMARY KEY,'+peak_table_variables+');'

create_tables = (st_create_data_table, st_create_peak_table, lt_create_data_table, lt_create_peak_table)

st_data_sql = 'insert into st_data(timestamp,{parameters}) VALUES({question})'.format(parameters = data_parameters, question = data_question)
st_peak_sql = 'insert into st_peak({parameters}) VALUES({question})'.format(parameters = peak_parameters, question = peak_question)
lt_data_sql = 'insert into lt_data(timestamp,{parameters}) VALUES({question})'.format(parameters = data_parameters, question = data_question)
lt_peak_sql = 'insert into lt_peak({parameters}) VALUES({question})'.format(parameters = peak_parameters, question = peak_question)

database_tables = ('st_data','st_peak','lt_data','lt_peak')

for folder in ('database','csv'):
    os.makedirs('./'+folder, exist_ok = True)

con = sqlite3.connect('database/peak_data.db')
cur = con.cursor()

with con:
    for table in create_tables:
        cur.execute(table)

loop = asyncio.get_event_loop()
queue = asyncio.Queue(maxsize=5, loop=loop)
stream_active = True

peaks_streamer = hyperion.HCommTCPPeaksStreamer(instrument_ip, loop, queue)

loop.create_task(get_data(con))
loop.call_later(streaming_time, peaks_streamer.stop_streaming)
loop.run_until_complete(peaks_streamer.stream_data())