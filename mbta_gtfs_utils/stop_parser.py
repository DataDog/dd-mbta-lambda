import csv

header = False
stops = {}
with open('MBTA_GTFS/stops.txt') as csvfile:
     reader = csv.reader(csvfile)
     for row in reader:
         if header == False:
             header = row
         else:
             stop = dict(zip(header, row))
             stop_id = stop['stop_id']
             stops[stop_id] = stop['stop_name']

print(stops)
