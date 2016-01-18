###  Copyright 2015, Andres F Vargas <andphe@gmail.com>
###
###  Collects metrics from an Nginx access_log with request_time
###  number of requests per response code; 50th, 90th, 99th, 99.9th percentiles
###  and max request_time are also calculated
###
### Based on SampleLogster which is Copyright 2011, Etsy, Inc.
### See LICENSE at https://github.com/andphe/logster-parsers/blob/master/LICENSE

import time
import re
import math

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

class NginxLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.http_1xx  = 0
        self.http_2xx  = 0
        self.http_3xx  = 0
        self.http_4xx  = 0
        self.http_5xx  = 0

        self.max = 0

        self.latencies = []
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line (in this case, http_status_code).
        self.reg = re.compile('.*HTTP/1.\d\" (?P<http_status_code>\d{3}) "(?P<request_time>[\d\.]+)" .*')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        try:
            # Apply regular expression to each line and extract interesting bits.
            regMatch = self.reg.match(line)

            if regMatch:
                linebits     = regMatch.groupdict()
                status       = int(linebits['http_status_code'])
                request_time = int(float(linebits['request_time']) * 1000)

                if (status < 200):
                    self.http_1xx += 1
                elif (status < 300):
                    self.http_2xx += 1
                elif (status < 400):
                    self.http_3xx += 1
                elif (status < 500):
                    self.http_4xx += 1
                else:
                    self.http_5xx += 1

                if request_time > self.max:
                    self.max = request_time

                self.latencies.append(request_time)

            else:
                raise LogsterParsingException("regmatch failed to match")

        except Exception as e:
            raise LogsterParsingException("regmatch or contents failed with %s" % e)


    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        self.duration = float(duration)

        self.latencies.sort()

        # Return a list of metrics objects
        requests = [
            MetricObject("requests.http_1xx", self.http_1xx, "Requests"),
            MetricObject("requests.http_2xx", self.http_2xx, "Requests"),
            MetricObject("requests.http_3xx", self.http_3xx, "Requests"),
            MetricObject("requests.http_4xx", self.http_4xx, "Requests"),
            MetricObject("requests.http_5xx", self.http_5xx, "Requests"),
            MetricObject("latency.p50", self.percentile(0.5), "Miliseconds"),
            MetricObject("latency.p90", self.percentile(0.9), "Miliseconds"),
            MetricObject("latency.p99", self.percentile(0.99), "Miliseconds"),
            MetricObject("latency.p999", self.percentile(0.999), "Miliseconds"),
            MetricObject("latency.max", self.max, "Miliseconds")
        ]

        return requests

    def percentile(self, percent):
        ''' Borrowed from http://code.activestate.com/recipes/511478-finding-the-percentile-of-the-values/ '''
        if not self.latencies:
            return 0
        k = (len(self.latencies) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return self.latencies[int(k)]
        d0 = self.latencies[int(f)] * (c - k)
        d1 = self.latencies[int(c)] * (k - f)
        return d0 + d1
