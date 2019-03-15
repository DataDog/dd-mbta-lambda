import csv
import sys

header = False
stops = {}
with open(sys.argv[1]) as csvfile:
     reader = csv.reader(csvfile)
     for row in reader:
         if header == False:
             header = row
         else:
             stop = dict(zip(header, row))
             stop_id = stop['stop_id']
             stops[stop_id] = stop['stop_name']

print(stops)
