import logging
import time
import os
from typing import List, Any
from pathlib import Path

from awslimitchecker.checker import AwsLimitChecker
from awslimitchecker.limit import AwsLimitUsage, AwsLimit
from prometheus_client import start_http_server, Summary, Gauge

CONFIG_PATH = '/var/awslimitchecker_config/'
PORT_ENV = 'ALC_PORT'
PORT_FILE = 'port'
INTERVAL_ENV = 'ALC_INTERVAL'
INTERVAL_FILE = 'refresh_interval'
REGIONS_ENV = 'ALC_REGIONS'
REGIONS_FILE = 'regions'
OVERRIDES_FILE = 'limit_overrides'
REQUEST_TIME = Summary(
    'update_processing_seconds',
    'Time spent querying aws for limits'
)
gauges = {}
trantab = str.maketrans(' -.', '___', '()')


class _Limit_Override(object):
    def __init__(self, item):
        self._region = item.split('/')[0]
        self._service = item.split('/')[1]
        self._limit_name = item.split('=')[0].split('/')[2]
        self._value = int(item.split('=')[1])

    @property
    def region(self):
        return self._region

    @property
    def service(self):
        return self._service

    @property
    def limit_name(self):
        return self._limit_name

    @property
    def value(self):
        return self._value


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)
    port, interval, regions, overrides = _get_configs()

    checkers = {}
    for region in regions:
        checkers[region] = AwsLimitChecker(region=region)
        for override in (override for override in overrides if override.region == region):
            checkers[region].set_limit_override(override.service, override.limit_name, override.value)

    start_http_server(port)
    while True:
        for region, checker in checkers.items():
            update(checker, region)
        time.sleep(interval)


@REQUEST_TIME.time()
def update(checker: AwsLimitChecker, region: str):
    try:
        checker.find_usage()

        labels = {'region': region}
        for service, svc_limits in sorted(checker.get_limits().items()):
            ec2_instances_limits = {key: svc_limits[key] for key in svc_limits.keys() if 'on-demand' in key.lower()}
            other_limits = {key: svc_limits[key] for key in svc_limits.keys() if key not in ec2_instances_limits.keys()}
            update_ec2_on_demand(region, ec2_instances_limits)
            for limit_name, limit in sorted(other_limits.items()):
                path = '.'.join([service, limit_name])
                usage = limit.get_current_usage()
                update_service(path, usage, limit.get_limit(), region)
    except Exception as e:
        logging.exception('message')


def update_ec2_on_demand(region: str, limits: List[AwsLimit]):
    if not limits:
        return
    path = 'ec2_running_on_demand_ec2_instances'
    g = gauge(path, ['instance_type'])

    for limit_name, limit in sorted(limits.items()):
        limit_path = '.'.join(['ec2', limit_name])
        limit_path = limit_path.lower().translate(trantab)
        instance_type = ''
        if limit_path == path:
            instance_type = 'total'
        else:
            # Instance names have a . in their name
            instance_type = next(word for word in limit_name.split(' ') if '.' in word)
        g.labels(region=region, type='limit', instance_type=instance_type).set(limit.get_limit())
        for resource in limit.get_current_usage():
            g.labels(region=region, type='current', instance_type=instance_type).set(resource.get_value())


def update_service(path: str, usage: List[AwsLimitUsage], limit: int, region: str):
    g = gauge(path)

    g.labels(region=region, type='limit').set(limit)
    for resource in usage:
        metric_type = 'current'
        if resource.resource_id:
            metric_type = resource.resource_id
        g.labels(region=region, type=metric_type).set(resource.get_value())


def gauge(path: str, extra_labels: List[str] = None) -> Gauge:
    path = path.lower().translate(trantab)
    g = gauges.get(path, None)
    if g is None:
        labels = ['region', 'type'] + (extra_labels if extra_labels else [])
        g = Gauge(path, '', labels)
        gauges[path] = g
    return g


def _read_config(file_name: str, env_var: str, default: str) -> str:
    config_file = Path(CONFIG_PATH + file_name)
    if config_file.is_file():
        return config_file.read_text()
    else:
        return os.getenv(env_var, default)


def _get_configs() -> (int, int, List[str], List[_Limit_Override]):
    """Configs are either in the /var/awslimitchecker_config folder or in the environment variables"""
    port = int(_read_config(PORT_FILE, PORT_ENV, '8080'))
    interval = int(_read_config(INTERVAL_FILE, INTERVAL_ENV, '1800'))
    regions = _read_config(REGIONS_FILE, REGIONS_ENV, 'us-east-1,us-west-2').split(',')
    overrides = {_Limit_Override(item) for item in _read_config(OVERRIDES_FILE, '', '').split('\n') if '=' in item}
    return port, interval, regions, overrides


if __name__ == '__main__':
    main()
