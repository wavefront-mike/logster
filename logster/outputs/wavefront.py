from logster.logster_helper import LogsterOutput
import re
import socket


class WavefrontOutput(LogsterOutput):
    shortname = 'wavefront'

    @classmethod
    def add_options(cls, parser):
        parser.add_option('--wavefront-proxy', action='store',
                           help='Hostname and port for Wavefront proxy, e.g. example.wavefront.com:2878')
        parser.add_option('--wavefront-source', action='store',
                          help='The source/host of this log data')

    def __init__(self, parser, options, logger):
        super(WavefrontOutput, self).__init__(parser, options, logger)

        # host
        if not options.wavefront_proxy:
            parser.print_help()
            parser.error("You must supply --wavefront-proxy when using 'wavefront' as an output type.")

        if (re.match("^[\w\.\-]+\:\d+$", options.wavefront_proxy) == None):
            parser.print_help()
            parser.error("Invalid host:port found for Wavefront: '%s'" % options.wavefront_proxy)

        self.wavefront_proxy = options.wavefront_proxy

        # source
        if not options.wavefront_source:
            parser.print_help()
            parser.error("You must supply --wavefront-source when using 'wavefront' as an output type.")

        self.wavefront_source = options.wavefront_source

    def submit(self, metrics):

        if (not self.dry_run):
            host = self.wavefront_proxy.split(':')
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host[0], int(host[1])))

        try:
            for metric in metrics:
                metric_name = self.get_metric_name(metric) 
                metric_string = "\"%s\" %s %s source=%s" % (metric_name, metric.value, metric.timestamp, self.wavefront_source)
                self.logger.debug("Submitting Wavefront metric: %s" % metric_string)

                if (not self.dry_run):
                    s.sendall(("%s\n" % metric_string).encode('ascii'))
                else:
                    print("%s %s" % (self.wavefront_proxy, metric_string))
        finally:
            if (not self.dry_run):
                s.close()
