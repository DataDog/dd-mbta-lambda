# -*- coding: utf-8 -*-
import os

import requests
from datadog import api, initialize, ThreadStats
from google.transit import gtfs_realtime_pb2

options = {
    'api_key': os.environ.get('DD_API_KEY'),
    'app_key': os.environ.get('DD_APP_KEY')
}

initialize(**options)


def handler(event, context):
    tripFeed = gtfs_realtime_pb2.FeedMessage()
    tripResponse = requests.get('https://cdn.mbta.com/realtime/TripUpdates.pb')
    tripFeed.ParseFromString(tripResponse.content)
    for entity in tripFeed.entity:
        if entity.HasField('trip_update'):
            pass
#            print(entity.trip_update)

    saFeed = gtfs_realtime_pb2.FeedMessage()
    saResponse = requests.get('https://cdn.mbta.com/realtime/Alerts.pb')
    saFeed.ParseFromString(saResponse.content)
    for entity in saFeed.entity:
        if entity.HasField('alert'):
            print(entity.alert)

    stats = ThreadStats()
    stats.start()
    stats.increment('mbta.api.test.counter')
    stats.flush()

    title = "Something big happened!"
    text = 'And let me tell you all about it here!'
    tags = ['version:1', 'application:web']

    api.Event.create(title=title, text=text, tags=tags)
