import csv

header = False
trips = {}
with open('MBTA_GTFS/trips.txt') as csvfile:
     reader = csv.reader(csvfile)
     for row in reader:
         if header == False:
             header = row
         else:
             trip = dict(zip(header, row))
             if trip['route_id'] != 'Red':
                 continue

             trip_id = trip['trip_id']
             trips[trip_id] = trip['trip_headsign']

print(trips)
