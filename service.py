# -*- coding: utf-8 -*-
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

initialize(**options)


def ingest_trip_updates():
    stats = ThreadStats()
    stats.start()

    trip_feed = gtfs_realtime_pb2.FeedMessage()
    trip_response = requests.get('https://cdn.mbta.com/realtime/TripUpdates.pb')
    trip_feed.ParseFromString(trip_response.content)
    trip_feed_ts = trip_feed.header.timestamp
    counter = 0
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
                    print("Flushing {}...".format(counter))
                    stats.flush()
                    print("Done")

    print("Flushing {}...".format(counter))
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

            if include_alert and entity.alert.effect != 7:  # Not OTHER_EFFECT
                alerts.append(entity)

    for entity in alerts:
        id = int(entity.id)
        alert = entity.alert

        current_period = None
        min_period = None
        for period in entity.alert.active_period:
            if period.start > now_ts and (period.end == 0 or now_ts < period.end):
                current_period = period
                break
            if min_period is None or min_period.start < period.start:
                min_period = period

        if current_period is None:
            current_period = min_period

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
            })

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
        title = alert.header_text.translation[0].text
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
        ]
        print(title)
        print(text)
        print(cause)
        print(effect)
        print(effect_status_mapping[alert.effect])
        api.Event.create(title=title,
                         text=text,
                         tags=tags,
                         date_happened=current_period.start,
                         alert_type=effect_status_mapping[alert.effect],
                         aggregation_key=str(alert_item['alert_id']),
                         )
        alert_item.save()


def handler(event, context):
    ingest_trip_updates()
    ingest_alerts()
