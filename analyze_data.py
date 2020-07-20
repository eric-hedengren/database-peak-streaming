import sqlite3
import matplotlib
import csv
import os

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

'''
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

# graph some data
# subplots for each port