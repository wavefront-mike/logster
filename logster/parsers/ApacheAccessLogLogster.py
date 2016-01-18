###  A logster parser file that can be used to get metrics from an Apache
###  access log
###
###  For example:
###  sudo ./logster --dry-run --output=ganglia ApacheAccessLogLogster /var/log/httpd/access_log
###
###

import datetime
import dateutil.parser
import logging
import optparse
import re
import time

from logster.logster_helper import MetricObjectT, LogsterParser
from logster.logster_helper import LogsterParsingException

class ApacheAccessLogLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of details we find in the log we are parsing.'''

        self.logger = logging.getLogger('logster')
        self._init_counters()

        # Regular expression for matching lines we are interested in
        # Assumes format is:
        # 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326'''
        self.reg = re.compile(r'^(.*)\s([w.-]+)\s([a-z.-]+)\s\[([0-9/:a-zA-Z\- ]+)\]\s"([A-Z]+) (.*).*"\s(\d{3})\s(\d+|-)')

        self._parse_options(option_string)

    def _parse_options(self, option_string):
        """
        Parse options
        """
        if not option_string:
            self.start_time = 0
            return

        options = option_string.split(' ')
        optparser = optparse.OptionParser()
        optparser.add_option('--start', '-S', dest='start_time', default='0',
                             help='Time to start parsing')

        opts, args = optparser.parse_args(args=options)
        self.start_time = unix_time_millis(dateutil.parser.parse(opts.start_time))
        
    def _init_counters(self):
        '''Initializes the counters to 0'''
        self.request_count = {}
        self.request_count_by_status_code = {}
        self.request_count_by_method_and_path = {}

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        self.logger.debug('Processing apache access log line %s', line.strip())
        try:
            regMatch = self.reg.match(line)
            if regMatch:
                lineparts = regMatch.groups()
                dttime = parse_date_time_string(lineparts[3])
                dttime_unix_time = unix_time_millis(dttime)
                if dttime_unix_time < self.start_time:
                    return

                method = lineparts[4]
                path = re.sub(r' HTTP.*$', '', lineparts[5])
                sanitized_path = re.sub(r'[^a-zA-Z0-9._\-]', '_', path)
                method_and_path = method + '.' + sanitized_path
                status_code = lineparts[6]

                add_one_to_dictionary(self.request_count, dttime_unix_time)
                add_one_to_dictionary(self.request_count_by_status_code,
                                      dttime_unix_time, status_code)
                add_one_to_dictionary(self.request_count_by_method_and_path,
                                      dttime_unix_time, method_and_path)

            else:
                self.logger.warn("regmatch failed to match: %s", line)

        except Exception as e:
            self.logger.warn("Failed in apache access log parser %s", e)
            raise LogsterParsingException("regmatch or contents failed with %s" % e)

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        self.duration = duration

        # Return a list of metrics objects
        rtn = []

        [rtn.append(MetricObjectT(name="apache.access.requests.count", value=value, timestamp = str(ts), metric_type='g', type='float', units='')) for ts, value in self.request_count.iteritems()]

        [rtn.append(MetricObjectT(name="apache.access.requests.status." + key, value = value, timestamp = ts, metric_type='g', type='float', units='')) for ts, d in self.request_count_by_status_code.iteritems() for key, value in d.iteritems()]

        [rtn.append(MetricObjectT(name="apache.access.requests.request." + str(key), value = value, timestamp = ts, metric_type='g', type='float', units='')) for ts, d in self.request_count_by_method_and_path.iteritems() for key, value in d.iteritems()]

        self._init_counters()
        return rtn


# See https://docs.python.org/2/library/datetime.html#datetime.tzinfo.fromutc
# and http://stackoverflow.com/a/23122493
class FixedOffset(datetime.tzinfo):
    """Fixed offset in minutes: `time = utc_time + utc_offset`."""
    def __init__(self, offset):
        self.__offset = datetime.timedelta(minutes=offset)
        hours, minutes = divmod(offset, 60)
        #NOTE: the last part is to remind about deprecated POSIX GMT+h timezones
        #  that have the opposite sign in the name;
        #  the corresponding numeric value is not used e.g., no minutes
        self.__name = '<%+03d%02d>%+d' % (hours, minutes, -hours)
    def utcoffset(self, dt=None):
        return self.__offset
    def tzname(self, dt=None):
        return self.__name
    def dst(self, dt=None):
        return timedelta(0)
    def __repr__(self):
        return 'FixedOffset(%d)' % (self.utcoffset().total_seconds() / 60)

def parse_date_time_string(date_string):
    ''' 
    Parses the date/time format from the apache access log
    :param date_string in the format 10/Oct/2000:13:55:36 -0700
    :return the datetime object
    '''
    date_str = date_string.split(' ')
    datetime_str = date_str[0]
    offset_str = date_str[1]
    dttime = datetime.datetime.strptime(datetime_str, '%d/%b/%Y:%H:%M:%S')
    offset = int(offset_str[-4:-2])*60 + int(offset_str[-2:])
    if offset_str[0] == "-":
        offset = -offset

    return dttime.replace(tzinfo=FixedOffset(offset))
    
# See http://stackoverflow.com/a/11111177
epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    """
    Converts a datetime object to unix timestamp
    :param dt the datetime to be converted
    :return unix timestamp
    """
    return int((dt.replace(tzinfo=None) - epoch).total_seconds() * 1000)

def add_one_to_dictionary(d, unix_time, key=None):
    '''
    Helper function for a dictionary that has the unix_time as its top-level key.
    The value is assumed to be either:
    - another dictionary with key
    - a numeric value
if no value exists at either level of the dictionary, it creates one and sets
    the numeric value to 1
    :param d the dictionary
    :param unix_time the timestamp top-level key
    :param key (optional) the key of the second-level dictionary
    '''

    if unix_time not in d:
        if key is None:
            d[unix_time] = 1
        else:
            d[unix_time] = {}
            d[unix_time][key] = 1
    else:
        if key is None:
            d[unix_time] = d[unix_time] + 1
        elif key in d[unix_time]:
            d[unix_time][key] = d[unix_time][key] + 1
        else:
            d[unix_time][key] = 1
    
