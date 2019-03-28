# -*- coding: utf-8 -*-
import json
import os
import time

import requests

from boto.dynamodb2 import exceptions
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from datadog import api, initialize, ThreadStats
from google.transit import gtfs_realtime_pb2
from stops import stop_names
from routes import route_names

options = {
    'api_key': os.environ.get('DD_API_KEY'),
    'app_key': os.environ.get('DD_APP_KEY')
}

enabled_routes = [
    'Red',
    'Mattapan',
    'Orange',
    'Blue',
    'Green-B',
    'Green-C',
    'Green-D',
    'Green-E',
    'CR-Fairmount'
]

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
            if trip_update.trip.route_id not in enabled_routes:
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

    routes = ['red', 'orange', 'blue', 'green-B', 'green-C', 'green-D', 'green-E']
    for route in routes:
        currentmetrics_url = 'http://realtime.mbta.com/developer/api/v2.1/currentmetrics?api_key={api_key}&format=json&route={route}'.format(
            route = route,
            api_key = mbta_perf_api_key,
        )
        currentmentrics_response = requests.get(currentmetrics_url)
        currentmetrics = json.loads(currentmentrics_response.content)

        # in the absence of data, assume good service, which means 100% of customers under all thresholds
        metrics = {
            'threshold_id_01.metric_result_last_hour': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:Headway',
                    'threshold_type:wait_time_headway_based',
                ],
            },
            'threshold_id_01.metric_result_current_day': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:Headway',
                    'threshold_type:wait_time_headway_based',
                ],
            },
            'threshold_id_02.metric_result_last_hour': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:Big Gap',
                    'threshold_type:wait_time_headway_based',
                ],
            },
            'threshold_id_02.metric_result_current_day': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:Big Gap',
                    'threshold_type:wait_time_headway_based',
                ],
            },
            'threshold_id_03.metric_result_last_hour': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:2X Headway',
                    'threshold_type:wait_time_headway_based',
                ],
            },
            'threshold_id_03.metric_result_current_day': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:2X Headway',
                    'threshold_type:wait_time_headway_based',
                ],
            },
            'threshold_id_04.metric_result_last_hour': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:delayed < 3 min.',
                    'threshold_type:travel_time',
                ],
            },
            'threshold_id_04.metric_result_current_day': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:delayed < 3 min.',
                    'threshold_type:travel_time',
                ],
            },
            'threshold_id_05.metric_result_last_hour': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:delayed < 6 min.',
                    'threshold_type:travel_time',
                ],
            },
            'threshold_id_05.metric_result_current_day': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:delayed < 6 min.',
                    'threshold_type:travel_time',
                ],
            },
            'threshold_id_06.metric_result_last_hour': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:delayed 10 min.',
                    'threshold_type:travel_time ',
                ],
            },
            'threshold_id_06.metric_result_current_day': {
                'value': 1,
                'tags': [
                    'route:{}'.format(route),
                    'threshold_name:delayed 10 min.',
                    'threshold_type:travel_time ',
                ],
            },
        }
        if route.startswith('green'):
            for key in metrics:
                metrics[key]['tags'].append('route:green')

        if 'current_metrics' in currentmetrics:
            for threshold in currentmetrics['current_metrics']:
                metric_last_hour = '{}.metric_result_last_hour'.format(threshold['threshold_id'])
                metric_current_day = '{}.metric_result_current_day'.format(threshold['threshold_id'])
                metrics[metric_last_hour]['value'] = threshold['metric_result_last_hour']
                metrics[metric_current_day]['value'] = threshold['metric_result_current_day']

        for metric_name, values in metrics.items():
            stats.gauge('mbta.perf.{}'.format(metric_name), values['value'], tags=values['tags'])
            counter += 1
            if counter % 50 == 0:
                print("Flushing currentmetrics {}...".format(counter))
                stats.flush()
                print("Done")

    print("Flushing currentmetrics {}...".format(counter))
    stats.flush()
    print("Done")


def ingest_alerts():
    alerts_table = Table('mbta_alerts')
    saFeed = gtfs_realtime_pb2.FeedMessage()
    saResponse = requests.get('https://cdn.mbta.com/realtime/Alerts.pb')
    saFeed.ParseFromString(saResponse.content)
    now_ts = time.time()
    alerts = []
    for entity in saFeed.entity:
        if entity.HasField('alert'):
            include_alert = False
            for informed in entity.alert.informed_entity:
                if informed.route_type <= 1:  # Subway/Green Line
                    include_alert = True
                    break
            if include_alert:
                include_alert = False
                for period in entity.alert.active_period:
                    # Include all future and current alerts
                    if period.end == 0 or now_ts < period.end:
                        include_alert = True
                        break

            if include_alert:
                alerts.append(entity)

    for entity in alerts:
        id = int(entity.id)
        alert = entity.alert

        sorted_active_periods = sorted(entity.alert.active_period, key=lambda period: period.start)
        current_period = None
        for period in sorted_active_periods:
            if now_ts > period.start and (now_ts < period.end or period.end == 0):
                current_period = period
                break

        if current_period == None:
            continue

        alert_item = None
        try:
            alert_item = alerts_table.get_item(alert_id=id)
        except exceptions.ItemNotFound:
            pass
        if not alert_item or alert_item['start'] != current_period.start:
            alert_item = Item(alerts_table, data={
                'alert_id': id,
                'start': current_period.start,
                'end': current_period.end,
                'future': (current_period.start > now_ts),
            })

            send_and_save_event(alert_item, alert, current_period)
        elif alert_item['future'] == True and alert_item['start'] < now_ts:
            alert_item['future'] = False
            send_and_save_event(alert_item, alert, current_period)


effect_status_mapping = {
    1: 'error',
    2: 'error',
    3: 'error',
    4: 'warning',
    5: 'info',
    6: 'warning',
    7: 'info',
    8: 'info',
    9: 'warning',
}

def send_and_save_event(alert_item, alert, current_period):
        title = '[MBTA] {}'.format(alert.header_text.translation[0].text)
        text = alert.description_text.translation[0].text
        cause = gtfs_realtime_pb2.Alert().Cause.Name(alert.cause)
        effect = gtfs_realtime_pb2.Alert().Effect.Name(alert.effect)
        tags = []
        for informed in alert.informed_entity:
            if informed.route_type <= 1:
                tags.append('route:{}'.format(informed.route_id))
        tags = [
            'cause:{}'.format(cause),
            'effect:{}'.format(effect),
            'service:mbta',
        ]
        print(title)
        print(text)
        print(cause)
        print(effect)
        print(effect_status_mapping[alert.effect])
        print(api.Event.create(title=title,
                         text=text,
                         tags=tags,
                         date_happened=min(current_period.start, time.time()),
                         alert_type=effect_status_mapping[alert.effect],
                         aggregation_key=str(alert_item['alert_id']),
                         ))
        alert_item.save(overwrite=True)


def handler(event, context):
    ingest_trip_updates()
    ingest_currentmetrics()
    ingest_alerts()
