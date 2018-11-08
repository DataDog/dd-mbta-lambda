# -*- coding: utf-8 -*-
import os

from datadog import api, initialize, ThreadStats

options = {
    'api_key': os.environ.get('DD_API_KEY'),
    'app_key': os.environ.get('DD_APP_KEY')
}

initialize(**options)


def handler(event, context):
    stats = ThreadStats()
    stats.start()
    stats.increment('mbta.api.test.counter')
    stats.flush()

    title = "Something big happened!"
    text = 'And let me tell you all about it here!'
    tags = ['version:1', 'application:web']

    api.Event.create(title=title, text=text, tags=tags)

