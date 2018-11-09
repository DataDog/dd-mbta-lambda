# -*- coding: utf-8 -*-
import json
import os
import requests

from datadog import api, initialize, ThreadStats
from google.transit import gtfs_realtime_pb2
from stops import stop_names
from routes import route_names

options = {
    'api_key': os.environ.get('DD_API_KEY'),
    'app_key': os.environ.get('DD_APP_KEY')
}

initialize(**options)

def ingest_trip_updates():
    stats = ThreadStats()
    stats.start()
    counter = 0

    trip_feed = gtfs_realtime_pb2.FeedMessage()
    trip_response = requests.get('https://cdn.mbta.com/realtime/TripUpdates.pb')
    trip_feed.ParseFromString(trip_response.content)
    trip_feed_ts = trip_feed.header.timestamp
    for entity in trip_feed.entity:
        if entity.HasField('trip_update'):
            trip_update = entity.trip_update
            if trip_update.trip.route_id not in ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C', 'Green-D', 'Green-E'):
                continue
            route_name = trip_update.trip.route_id
            if trip_update.trip.route_id in route_names:
                route_name = route_names[trip_update.trip.route_id]
            last_stop_id = trip_update.stop_time_update[len(trip_update.stop_time_update) - 1].stop_id
            destination = stop_names[last_stop_id]
            trip_id = trip_update.trip.trip_id
            vehicle = trip_update.vehicle.label

            for stop in trip_update.stop_time_update:
                stop_name = stop_names[stop.stop_id]

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

                arrives_in = (time - trip_feed_ts)
                catchable_tag = 'catchable:false'
                if arrives_in > 120:
                    catchable_tag = 'catchable:true'

                tags = [
                    'trip_id:{}'.format(trip_id),
                    'stop:{}'.format(stop_name),
                    'destination:{}'.format(destination),
                    'vehicle:{}'.format(vehicle),
                    'route:{}'.format(route_name),
                    catchable_tag,
                ]
                if route_name.startswith('Green'):
                    tags.append('route:green')
                stats.gauge('mbta.trip.arrival_secs', arrives_in, tags=tags)
                stats.gauge('mbta.trip.arrival_min', arrives_in / 60, tags=tags)
                counter += 1
                if counter % 50 == 0:
                    print("Flushing trip updates {}...".format(counter))
                    stats.flush()
                    print("Done")

    print("Flushing trip updates {}...".format(counter))
    stats.flush()
    print("Done")


def ingest_currentmetrics():
    stats = ThreadStats()
    stats.start()
    counter = 0

    mbta_perf_api_key = os.environ.get('MBTA_PERF_API_KEY')

    routes = ['red', 'orange', 'green', 'blue']
    for route in routes:
        currentmetrics_url = 'http://realtime.mbta.com/developer/api/v2.1/currentmetrics?api_key={api_key}&format=json&route={route}'.format(
            route = route,
            api_key = mbta_perf_api_key,
        )
        currentmentrics_response = requests.get(currentmetrics_url)
        currentmetrics = json.loads(currentmentrics_response.content)

        # in the absence of data, assume good service, which means 100% of customers under all thresholds
        metrics = {
            'threshold_id_01.metric_result_last_hour': 1,
            'threshold_id_01.metric_result_current_day': 1,
            'threshold_id_02.metric_result_last_hour': 1,
            'threshold_id_02.metric_result_current_day': 1,
            'threshold_id_03.metric_result_last_hour': 1,
            'threshold_id_03.metric_result_current_day': 1,
            'threshold_id_04.metric_result_last_hour': 1,
            'threshold_id_04.metric_result_current_day': 1,
            'threshold_id_05.metric_result_last_hour': 1,
            'threshold_id_05.metric_result_current_day': 1,
            'threshold_id_06.metric_result_last_hour': 1,
            'threshold_id_06.metric_result_current_day': 1,
        }
        for threshold in currentmetrics['current_metrics']:
            metric_last_hour = '{}.metric_result_last_hour'.format(threshold['threshold_id'])
            metric_current_day = '{}.metric_result_current_day'.format(threshold['threshold_id'])
            metrics[metric_last_hour] = threshold['metric_result_last_hour']
            metrics[metric_current_day] = threshold['metric_result_current_day']

            tags = [
                'route:{}'.format(route),
                'threshold_name:{}'.format(threshold['threshold_name']),
                'threshold_type:{}'.format(threshold['threshold_type']),
            ]

            for metric, value in metrics.items():
                stats.gauge('mbta.perf.{}'.format(metric), value, tags=tags)
                counter += 1
                if counter % 50 == 0:
                    print("Flushing currentmetrics {}...".format(counter))
                    stats.flush()
                    print("Done")

    print("Flushing currentmetrics {}...".format(counter))
    stats.flush()
    print("Done")



def handler(event, context):
    ingest_trip_updates()
    ingest_currentmetrics()


    #saFeed = gtfs_realtime_pb2.FeedMessage()
    #saResponse = requests.get('https://cdn.mbta.com/realtime/Alerts.pb')
    #saFeed.ParseFromString(saResponse.content)
    #for entity in saFeed.entity:
    #    if entity.HasField('alert'):
    #        include_alert = False
    #        for informed in entity.alert.informed_entity:
    #            if informed.route_type == 1:  # Subway
    #                include_alert = True
    #                break
    #        if include_alert:
    #            print(entity.alert)
