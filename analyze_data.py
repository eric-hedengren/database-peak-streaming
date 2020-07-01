import csv
import sqlite3

def write_csv(table):
    cur.execute("select * from "+table+";")
    with open('csv/'+table+".csv", "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cur.description])
        csv_writer.writerows(cur)

con = sqlite3.connect('peak_data.db')
cur = con.cursor()

database_tables = ('peak_data','data')

for table_name in database_tables:
    write_csv(table_name)