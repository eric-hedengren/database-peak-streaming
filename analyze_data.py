from matplotlib import pyplot as plt
import sqlite3
import csv
import os

'''
# don't export csv? instead use lists?
def export_csv(): # export interpreted data instead?
    for table in database_tables:
        cur.execute('select * from '+table+';')
        with open('csv/'+table+'.csv', 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in cur.description])
            csv_writer.writerows(cur)

os.makedirs('./csv', exist_ok = True)

con = sqlite3.connect('database/peak_data.db')
cur = con.cursor()

dt = cur.execute("select name from sqlite_master where type='table';").fetchall()
database_tables = []
for tup in dt:
    database_tables.append(tup[0])

export_csv()



itp = 'manually measured' # initial temperature | constant after measured | what temperature is measured
gf = .807 # gage factor at 22 C | constant after calculated from itp | room temperature?
metal_constant = 'unknown value' # metal constant | constant | specific to the type of metal sensors are measuring
alpha = 'unknown variable' # unsure what this variable provides
wl = None # wavelength | current wavelength data
iwl = None # initial wavelength | get data first row

total_strain = (10**6)*((wl-iwl)/iwl/gf)

temperature = (wl-iwl)/(wl*gf*(metal_constant+alpha)) + itp

strain = total_strain-temperature
'''

con = sqlite3.connect('database/peak_data.db')
cur = con.cursor()
lt_data = cur.execute('select * from lt_data;').fetchall()
lt_peak = cur.execute('select * from lt_peak;').fetchall()

timestamp = []
for data in lt_data:
    timestamp.append(data[1])

plt.figure() #figsize=(20,14)

'''
for i in range(1,len(lt_peak[0])):
    frequency = []
    for data in lt_peak:
        frequency.append(data[i])
    plt.plot(timestamp, frequency)
'''

# plot calculated strain instead of peak information
# split data by port for location information

ax1 = plt.subplot(2,1,1)
ports = lt_data[0]
number = ports[2]+1
for i in range(1,number):
    frequency = [] # make function with (i) input
    for data in lt_peak:
        frequency.append(data[i])
    plt.plot(timestamp, frequency)

previous = number
number = ports[3]
ax2 = plt.subplot(2,1,2)
for i in range(previous, number+previous):
    frequency = [] # make function with (i) input
    for data in lt_peak:
        frequency.append(data[i])
    plt.plot(timestamp, frequency)

'''
ax3 = plt.subplot(6,1,3)
ax4 = plt.subplot(6,2,1)
ax5 = plt.subplot(6,2,2)
ax6 = plt.subplot(6,2,3)
'''

plt.show()