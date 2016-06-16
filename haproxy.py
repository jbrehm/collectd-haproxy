# haproxy-collectd-plugin - haproxy.py
#
# Author: Michael Leinartas
# Description: This is a collectd plugin which runs under the Python plugin to
# collect metrics from haproxy.
# Plugin structure and logging func taken from
# https://github.com/phrawzty/rabbitmq-collectd-plugin
#
# Modified by "Warren Turkal" <wt@signalfuse.com>, "Volodymyr Zhabiuk" <vzhabiuk@signalfx.com>

import cStringIO as StringIO
import socket
import csv
import glob
import pprint
import os

import collectd

PLUGIN_NAME = 'haproxy'
RECV_SIZE = 1024

METRIC_TYPES = {
    #Metrics that are collected for the whole haproxy instance.
    # The format is  haproxy_metricname : {'signalfx_corresponding_metric': 'collectd_type'}
    # Currently signalfx_corresponding_metric match haproxy_metricname
    #Correspond to 'show info' socket command
    'MaxConn': ('max_connections', 'gauge'),
    'CumConns': ('connections', 'derive'),
    'CumReq': ('requests', 'derive'),
    'MaxConnRate': ('max_connection_rate', 'gauge'),
    'MaxSessRate': ('max_session_rate', 'gauge'),
    'MaxSslConns': ('max_ssl_connections', 'gauge'),
    'CumSslConns': ('ssl_connections', 'derive'),
    'MaxPipes': ('max_pipes', 'gauge'),
    'Idle_pct': ('idle_pct', 'gauge'),
    'Tasks': ('tasks', 'gauge'),
    'Run_queue': ('run_queue', 'gauge'),
    'PipesUsed': ('pipes_used', 'gauge'),
    'PipesFree': ('pipes_free', 'gauge'),
    'Uptime_sec': ('uptime_seconds', 'derive'),
    'CurrConns': ('current_connections', 'gauge'),
    'CurrSslConns': ('current_ssl_connections', 'gauge'),
    'ConnRate': ('connection_rate', 'gauge'),
    'SessRate': ('session_rate', 'gauge'),
    'SslRate': ('ssl_rate', 'gauge'),
    'SslFrontendKeyRate': ('ssl_frontend_key_rate', 'gauge'),
    'SslBackendKeyRate': ('ssl_backend_key_rate', 'gauge'),
    'SslCacheLookups': ('ssl_cache_lookups', 'derive'),
    'SslCacheMisses': ('ssl_cache_misses', 'derive'),
    'CompressBpsIn': ('compress_bps_in', 'derive'),
    'CompressBpsOut': ('compress_bps_out', 'derive'),
    'ZlibMemUsage': ('zlib_mem_usage', 'gauge'),
    'Idle_pct': ('idle_pct', 'gauge'),

     #Metrics that are collected per each proxy separately. Proxy name would be the dimension as well as service_name
     #Correspond to 'show stats' socket command
    'bin': ('bytes_in', 'derive'),
    'bout': ('bytes_out', 'derive'),
    'chkfail': ('failed_checks', 'derive'),
    'downtime': ('downtime', 'derive'),
    'dresp': ('denied_response', 'derive'),
    'dreq': ('denied_request', 'derive'),
    'econ': ('error_connection', 'derive'),
    'ereq': ('error_request', 'derive'),
    'eresp': ('error_response', 'derive'),
    'hrsp_1xx': ('response_1xx', 'derive'),
    'hrsp_2xx': ('response_2xx', 'derive'),
    'hrsp_3xx': ('response_3xx', 'derive'),
    'hrsp_4xx': ('response_4xx', 'derive'),
    'hrsp_5xx': ('response_5xx', 'derive'),
    'hrsp_other': ('response_other', 'derive'),
    'qcur': ('queue_current', 'gauge'),
    'rate': ('session_rate', 'gauge'),
    'req_rate': ('request_rate', 'gauge'),
    'stot': ('session_total', 'derive'),
    'scur': ('session_current', 'gauge'),
    'wredis': ('redistributed', 'derive'),
    'wretr': ('retries', 'derive'),
    'throttle': ('throttle', 'gauge'),
    'req_tot': ('req_tot', 'derive'),
    'cli_abrt': ('cli_abrt', 'derive'),
    'srv_abrt': ('srv_abrt', 'derive'),
    'comp_in': ('comp_in', 'derive'),
    'comp_out': ('comp_out', 'derive'),
    'comp_byp': ('comp_byp', 'derive'),
    'comp_rsp': ('comp_rsp', 'derive'),
    'qtime': ('queue_time_avg', 'gauge'),
    'ctime': ('connect_time_avg', 'gauge'),
    'rtime': ('response_time_avg', 'gauge'),
}

#Making sure that metrics names are case insensitive.
#It helps with backward compatibility
METRIC_TYPES = dict((k.lower(), v) for k, v in METRIC_TYPES.items())
METRIC_DELIM = '.'  # for the frontend/backend stats

DEFAULT_SOCKET = '/var/lib/haproxy/stats'
DEFAULT_PROXY_MONITORS = [ 'server', 'frontend', 'backend' ]
HAPROXY_SOCKET = None


class HAProxySocket(object):
    """
            Encapsulates communication with HAProxy via the socket interface
     """

    def __init__(self, socket_file=DEFAULT_SOCKET):
        self.socket_file = socket_file

    def connect(self):
        stat_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stat_sock.connect(self.socket_file)
        return stat_sock

    def communicate(self, command):
        '''Get response from single command.

        Args:
            command: string command to send to haproxy stat socket

        Returns:
            a string of the response data
        '''
        if not command.endswith('\n'):
            command += '\n'
        stat_sock = self.connect()
        stat_sock.sendall(command)
        result_buf = StringIO.StringIO()
        buf = stat_sock.recv(RECV_SIZE)
        while buf:
            result_buf.write(buf)
            buf = stat_sock.recv(RECV_SIZE)
        stat_sock.close()
        return result_buf.getvalue()

    def get_server_info(self):
        result = {}
        output = self.communicate('show info')
        for line in output.splitlines():
            try:
                key, val = line.split(':', 1)
            except ValueError:
                continue
            result[key.strip()] = val.strip()
        return result

    def get_server_stats(self):
        output = self.communicate('show stat')
        #sanitize and make a list of lines
        output = output.lstrip('# ').strip()
        output = [l.strip(',') for l in output.splitlines()]
        csvreader = csv.DictReader(output)
        result = [d.copy() for d in csvreader]
        return result


def get_stats(socket):
    """
        Makes two calls to haproxy to fetch server info and server stats.
        Returns the dict containing metric name as the key and a tuple of metric value and the dict of dimensions if any
    """
    if socket is None:
        collectd.error("Socket configuration parameter is undefined. Couldn't get the stats")
        return
    stats = [ ]
    haproxy = HAProxySocket(socket)

    try:
        server_info = haproxy.get_server_info()
        server_stats = haproxy.get_server_stats()
    except socket.error:
        collectd.warning(
            'status err Unable to connect to HAProxy socket at %s' %
            socket)
        return stats

    for key, val in server_info.iteritems():
        try:
            stats.append((key, int(val), None))
        except (TypeError, ValueError):
            pass
    for statdict in server_stats:
        if not (statdict['svname'].lower() in PROXY_MONITORS or statdict['pxname'].lower() in PROXY_MONITORS):
              continue
        for metricname, val in statdict.items():
            try:
                stats.append((metricname, int(val), {'proxy_name': statdict['pxname'], 'service_name': statdict['svname']}))
            except (TypeError, ValueError):
                pass
    return stats


def config(config_values):
    """
    A callback method that  loads information from the HaProxy collectd plugin config file.
    Args:
    config_values (collectd.Config): Object containing config values
    """

    global PROXY_MONITORS, HAPROXY_SOCKET
    PROXY_MONITORS = [ ]
    HAPROXY_SOCKET = [ ]
    for node in config_values.children:
        if node.key == "ProxyMonitor":
            PROXY_MONITORS.append(node.values[0].lower())
        elif  node.key == "Socket":
            for socket in glob.glob(node.values[0]):
              HAPROXY_SOCKET.append(socket)
        else:
            collectd.warning('Unknown config key: %s' % node.key)
    if not PROXY_MONITORS:
        PROXY_MONITORS += DEFAULT_PROXY_MONITORS
    PROXY_MONITORS = [ p.lower() for p in PROXY_MONITORS ]


def collect_metrics():
    collectd.debug('beginning collect_metrics')
    """
        A callback method that gets metrics from HAProxy and records them to collectd.
    """
    for socket in HAPROXY_SOCKET:
        collectd.debug('starting to collect stats from socket at %s' % socket)
        info = get_stats(socket)
        process_name = os.path.basename(socket).split('.')[0]

        if not info:
            collectd.warning('%s: No data received' % PLUGIN_NAME)
            return

        for metric_name, metric_value, dimensions in info:
            if not metric_name.lower() in METRIC_TYPES:
                collectd.debug("Metric %s is not in the metric types" % metric_name)
                continue

            translated_metric_name, val_type = METRIC_TYPES[metric_name.lower()]

            collectd.debug('Collecting {0}: {1}'.format(translated_metric_name, metric_value))
            datapoint = collectd.Values()
            datapoint.type = val_type
            datapoint.type_instance = translated_metric_name
            datapoint.plugin = '-'.join([ PLUGIN_NAME, process_name])
            if dimensions:
                datapoint.plugin_instance = '-'.join([dimensions['proxy_name'], dimensions['service_name']])
            datapoint.values = (metric_value,)
            pprint_dict = {
                    'plugin': datapoint.plugin,
                    'plugin_instance': datapoint.plugin_instance,
                    'type': datapoint.type,
                    'type_instance': datapoint.type_instance,
                    'values': datapoint.values
                    }
            collectd.debug(pprint.pformat(pprint_dict))
            datapoint.dispatch()

collectd.register_config(config)
collectd.register_read(collect_metrics)
