# -*- coding: utf-8 -*-
import os

import requests
from datadog import api, initialize, ThreadStats
from google.transit import gtfs_realtime_pb2
import stops

options = {
    'api_key': os.environ.get('DD_API_KEY'),
    'app_key': os.environ.get('DD_APP_KEY')
}

initialize(**options)


def handler(event, context):
    stats = ThreadStats()
    stats.start()

    tripFeed = gtfs_realtime_pb2.FeedMessage()
    tripResponse = requests.get('https://cdn.mbta.com/realtime/TripUpdates.pb')
    tripFeed.ParseFromString(tripResponse.content)
    tripFeedTs = tripFeed.header.timestamp
    for entity in tripFeed.entity:
        if entity.HasField('trip_update'):
            tripUpdate = entity.trip_update
            if tripUpdate.trip.route_id == 'Red':
                last_stop_id = tripUpdate.stop_time_update[len(tripUpdate.stop_time_update) - 1].stop_id
                destination = stops.stopNames[last_stop_id]
                trip_id = tripUpdate.trip.trip_id
                vehicle = tripUpdate.vehicle.label

                for stop in tripUpdate.stop_time_update:
                    stopName = stops.stopNames[stop.stop_id]

                    if stop.departure.time > 0:
                        if stop.arrival.time > 0:
                            # mid-route stop, use arrival time
                            time = stop.arrival.time
                        else:
                            # first stop, use departure time
                            time = stop.departure.time
                    else:
                        # last stop, ignore
                        continue

                    arrives_in = time - tripFeedTs
                    tags = [
                        'trip_id:{}'.format(trip_id),
                        'stop:{}'.format(stopName),
                        'destination:{}'.format(destination),
                        'vehicle:{}'.format(vehicle),
                        'route:Red Line',
                    ]
                    stats.gauge('mbta.trip.arrival', arrives_in, tags=tags)

    saFeed = gtfs_realtime_pb2.FeedMessage()
    saResponse = requests.get('https://cdn.mbta.com/realtime/Alerts.pb')
    saFeed.ParseFromString(saResponse.content)
    for entity in saFeed.entity:
        if entity.HasField('alert'):
            include_alert = False
            for informed in entity.alert.informed_entity:
                if informed.route_type == 1:  # Subway
                    include_alert = True
                    break
            if include_alert:
                print(entity.alert)

    stats.flush()
