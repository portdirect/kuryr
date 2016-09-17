# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import collections
from concurrent import futures
import functools
import ipaddress
import signal
import sys
import traceback

import netaddr
from oslo_log import log
from oslo_service import service
import requests

from kuryr._i18n import _LE
from kuryr._i18n import _LI
from kuryr.common import config
from kuryr.common import constants
from kuryr import controllers
from kuryr.raven.aio import headers
from kuryr.raven.aio import methods
from kuryr.raven import watchers
from kuryr import utils


LOG = log.getLogger(__name__)

HARDCODED_NET_NAME = 'raven-default'
DEFAULT_PREFIX_LEN = 24


def register_watchers(*watchers):
    """Registers one or more watchers to ``WATCH_ENDPOINT_AND_CALLBACKS``.

    This functions is intended to be used as a decorator for a class. The
    watchers **must** conform to the requirements of ``K8sApiWatcher`` abstract
    class, that is to implement ``WATCH_ENDPOINT`` property and ``translate``
    method.

    >>> from kuryr.raven import watchers
    >>> class SomeWatcher(watchers.K8sApiWatcher):
    ...     WATCH_ENDPONT = '/'
    ...     def translate(self, deserialized_json):
    ...         print(deserialized_json)
    ...
    >>> @register_watchers(SomeWatcher)
    ... class Foo(object)
    ...     pass
    ...
    """
    def wrapper(cls):
        if not hasattr(cls, 'WATCH_ENDPOINTS_AND_CALLBACKS'):
            # For unit tests, we preserve the order of items.
            cls.WATCH_ENDPOINTS_AND_CALLBACKS = collections.OrderedDict()
        for watcher in watchers:
            slot = cls.WATCH_ENDPOINTS_AND_CALLBACKS
            slot[watcher.WATCH_ENDPOINT] = watcher.translate
        return cls
    return wrapper


# TODO(devvesa): refactor current decorator approach to allow not intrusive
#                way to add more watchers.
if config.CONF.raven.raven_single_shot:
    @register_watchers(watchers.K8sNamespaceWatcher)
else:
    @register_watchers(watchers.K8sEndpointsWatcher,
                        watchers.K8sNamespaceWatcher,
                        watchers.K8sPodsWatcher,
                        watchers.K8sServicesWatcher)

class Raven(service.Service):
    """A K8s API watcher service watches and translates K8s resources.

    This class makes the GET requests against the K8s integration related
    resource endpoints with ``?watch=true`` query string and receive a series
    of event notifications on the watched resource endpoints. The event
    notifications are given in the JSON format. Then this class translates
    the events into the creations, deletions or updates of Neutron resources.
    """
    # TODO(tfukushima): Initialize the global neutronclient here.
    neutron = controllers.get_neutron_client()
    # For unit tests this is the ordered dictionary.
    WATCH_ENDPOINTS_AND_CALLBACKS = collections.OrderedDict()

    def __init__(self):
        super(Raven, self).__init__()
        self._executor = futures.ThreadPoolExecutor(
            max_workers=config.CONF.raven.max_workers)
        self._sequential_executor = futures.ThreadPoolExecutor(max_workers=1)
        self._event_loop = asyncio.new_event_loop()
        self._event_loop.set_default_executor(self._executor)
        self._tasks = {}
        self._reconnect = True  # For unit testing purposes
        assert not self._event_loop.is_closed()

        self._lock = asyncio.Lock(loop=self._event_loop)
        self._resource_version = 0
        self._namespace_lock = asyncio.Lock(loop=self._event_loop)
        self.namespace_added = asyncio.Condition(
            lock=self._namespace_lock, loop=self._event_loop)
        self.namespace_deleted = asyncio.Condition(
            lock=self._namespace_lock, loop=self._event_loop)
        self._service_lock = asyncio.Lock(loop=self._event_loop)
        self.service_added = asyncio.Condition(
            lock=self._service_lock, loop=self._event_loop)
        self.service_deleted = asyncio.Condition(
            lock=self._service_lock, loop=self._event_loop)

    @asyncio.coroutine
    def _update_resource_version(self, resource_version):
        # Since self._resource_version is accessed by multiple coroutines that
        # possibly run in the different threads, it needs to be protected by
        # the lock.
        with (yield from self._lock):
            if resource_version > self._resource_version:
                self._resource_version = resource_version
                LOG.debug('Updated the resource version to %s',
                          self._resource_version)

    def _get_endpoint_with_resource_version(self, endpoint):
        joint_char = '&' if '?' in endpoint else '?'
        endpoint_with_resource_version = ''.join(
            [endpoint, joint_char, 'resourceVersion=',
             str(self._resource_version)])
        return endpoint_with_resource_version

    @staticmethod
    def _get_router_ports_by_subnet_id(neutron_subnet_id, neutron_port_list):
        router_ports = [
            port for port in neutron_port_list
            if ((neutron_subnet_id in [fip['subnet_id']
                                       for fip in port.get('fixed_ips', [])])
                or (neutron_subnet_id == port.get('subnet_id', '')))]

        return router_ports

    def _get_or_create_service_router(self):
        router_name = HARDCODED_NET_NAME + '-router'
        routers = controllers._get_routers_by_attrs(
            unique=False, name=router_name)
        if routers:
            router = routers[0]
            LOG.debug('Reusing the existing router %s', router)
        else:
            created_router_response = self.neutron.create_router(
                {'router': {'name': router_name}})
            router = created_router_response['router']
            LOG.debug('Created a new router %s', router)

        return router

    def _create_default_security_group(self):
        sgs = controllers._get_security_groups_by_attrs(
            unique=False,
            name=constants.K8S_HARDCODED_SG_NAME)
        if sgs:
            sg = sgs[0]
            LOG.debug('Reusing the existing SG %s', sg)
        else:
            sg_response = self.neutron.create_security_group(
                {'security_group':
                    {'name': constants.K8S_HARDCODED_SG_NAME}})
            sg = sg_response['security_group']
            # Create ingress rules similarly to Neutron Default SG
            for ethertype in ['IPv4', 'IPv6']:
                rule = {
                    'security_group_id': sg['id'],
                    'ethertype': ethertype,
                    'direction': 'ingress',
                    'remote_group_id': sg['id'],
                }
                req = {
                    'security_group_rule': rule,
                }
                LOG.debug('Creating SG rule %s', req)
                self.neutron.create_security_group_rule(req)
            LOG.debug('Created a new default SG %s', sg)
        self._default_sg = sg['id']

    def _construct_subnetpool(self, namespace_router):
        subnetpool_name = HARDCODED_NET_NAME + '-pool'
        subnetpool_prefix = config.CONF.k8s.cluster_subnet_pool
        subnetpools = controllers._get_subnetpools_by_attrs(
            unique=False, name=subnetpool_name)
        if subnetpools:
            subnetpool = subnetpools[0]
            LOG.debug(
                'Reusing the existing subnet pool %s', subnetpool)
        else:
            subnetpool_response = self.neutron.create_subnetpool(
                {'subnetpool': {'name': subnetpool_name,
                                'prefixes': [subnetpool_prefix],
                                'default_prefixlen': DEFAULT_PREFIX_LEN}})
            subnetpool = subnetpool_response['subnetpool']
        self._subnetpool = subnetpool

    def _construct_cluster_network(self, namespace_router):
        network_name = HARDCODED_NET_NAME + '-cluster-pool'
        subnet_name = HARDCODED_NET_NAME + '-cluster-pool-subnet'
        networks = controllers._get_networks_by_attrs(name=network_name)
        if networks:
            self._cluster_network = networks[0]
        else:
            network_response = self.neutron.create_network(
                {'network': {'name': network_name}})
            self._cluster_network = network_response['network']
            LOG.debug('Created a new cluster network %s',
                self._cluster_network)

        # Ensure the subnet exists
        subnets = controllers._get_subnets_by_attrs(
            name=subnet_name,
            network_id=self._cluster_network['id'])
        if subnets:
            self._cluster_subnet = subnets[0]
        else:
            subnet_range = ipaddress.ip_network(
                config.CONF.k8s.cluster_vip_subnet)
            new_subnet = {
                'name': subnet_name,
                'network_id': self._cluster_network['id'],
                'ip_version': subnet_range.version,
                'cidr': str(subnet_range),
                'enable_dhcp': False
            }
            subnet_response = self.neutron.create_subnet(
                {'subnet': new_subnet})
            self._cluster_subnet = subnet_response['subnet']
            LOG.debug('Created a new cluster subnet %s', self._cluster_subnet)

        self._ensure_router_port(
            self._cluster_network['id'],
            self._cluster_subnet['id'])

    def _ensure_router_port(self, port_network_id, port_subnet_id):

        filtered_service_ports = controllers._get_ports_by_attrs(
            unique=False, device_owner='network:router_interface',
            device_id=self._router['id'], network_id=port_network_id)

        service_router_ports = self._get_router_ports_by_subnet_id(
            port_subnet_id, filtered_service_ports)

        if not service_router_ports:
            self.neutron.add_interface_router(
                self._router['id'], {'subnet_id': port_subnet_id})
        else:
            LOG.debug('The cluster IP subnet %s is already bound to the '
                      'router.', port_subnet_id)

    def _construct_service_network(self, namespace_router):
        service_network_name = HARDCODED_NET_NAME + '-service'
        service_networks = controllers._get_networks_by_attrs(
            unique=False, name=service_network_name)
        if service_networks:
            service_network = service_networks[0]
            LOG.debug('Reusing the existing service network %s',
                      service_network)
        else:
            service_network_response = self.neutron.create_network(
                {'network': {'name': service_network_name}})
            service_network = service_network_response['network']
            LOG.debug('Created a new service network %s', service_network)
            self._service_network = service_network

        service_subnet_cidr = config.CONF.k8s.cluster_service_subnet
        service_subnet_name = HARDCODED_NET_NAME + '-' + service_subnet_cidr
        service_subnets = controllers._get_subnets_by_attrs(
            unique=False, name=service_subnet_name,
            network_id=service_network['id'])
        if service_subnets:
            service_subnet = service_subnets[0]
            LOG.debug('Reusing the existing service subnet %s', service_subnet)
        else:
            LOG.debug('About to create service subnet')
            ip = netaddr.IPNetwork(service_subnet_cidr)
            new_service_subnet = {
                'name': service_subnet_name,
                'network_id': service_network['id'],
                'ip_version': ip.version,
                'cidr': service_subnet_cidr,
                'enable_dhcp': False,
            }
            service_subnet_response = self.neutron.create_subnet(
                {'subnet': new_service_subnet})
            service_subnet = service_subnet_response['subnet']
            LOG.debug('Created a new service subnet %s', service_subnet)

        self._service_subnet = service_subnet

        neutron_service_network_id = service_network['id']
        neutron_router_id = namespace_router['id']

        LOG.debug('Getting service ports on service network')
        filtered_service_ports = controllers._get_ports_by_attrs(
            unique=False, device_owner='network:router_interface',
            device_id=neutron_router_id,
            network_id=neutron_service_network_id)

        LOG.debug('Getting Service Subnet ID')
        service_subnet_id = service_subnet['id']
        service_router_ports = self._get_router_ports_by_subnet_id(
            service_subnet_id, filtered_service_ports)

        if not service_router_ports:
            LOG.debug('Attempting to add service subnet to router')
            self.neutron.add_interface_router(
                neutron_router_id, {'subnet_id': service_subnet_id})
        else:
            LOG.debug('The cluster IP subnet %s is already bound to the '
                      'router.', service_subnet_id)

    def _ensure_networking_base(self):
        self._create_default_security_group()

        router = self._get_or_create_service_router()
        self._router = router

        self._construct_subnetpool(router)
        self._construct_cluster_network(router)
        self._construct_service_network(router)

    def _task_done_callback(self, task):
        endpoint = self._tasks.pop(task)
        LOG.info(_LI('Finished watcher for endpoint "%s"'), endpoint)
        if config.CONF.raven.raven_single_shot:
            LOG.info(_LI('Finished Namespace Sweep. Shutting down...'))
            self.stop()
        if not self._tasks:
            LOG.info(_LI('No more tasks to handle. Shutting down...'))
            self.stop()

    def restart(self):
        """Restarts Raven instance."""
        LOG.debug('Restarted the service: %s', self.__class__.__name__)
        super(Raven, self).restart()

    def start(self):
        """Starts the event loop and watch endpoints.

        Before starting the event loop and the watch, Raven creates the Neutron
        network and subnet for the services with the router to be shared among
        the namespaces.

        Then Raven creates tasks consumed in the event loop with the registered
        callbacks for each endpoint and their cancellation callbacks. The
        signal handlers are specified for the manual termination by users as
        well.

        Finally, Raven starts running the event loop until all tasks are
        completed.
        """
        LOG.debug('Started the service: %s', self.__class__.__name__)
        super(Raven, self).start()
        LOG.debug('Watched endpoints: %s', self.WATCH_ENDPOINTS_AND_CALLBACKS)
        self._ensure_networking_base()

        for endpoint, callback in self.WATCH_ENDPOINTS_AND_CALLBACKS.items():
            task = self._event_loop.create_task(self.watch(
                endpoint, callback.__get__(self, Raven)))
            task.add_done_callback(self._task_done_callback)
            self._tasks[task] = endpoint

        self._event_loop.add_signal_handler(signal.SIGTERM, self.stop)
        self._event_loop.add_signal_handler(signal.SIGINT, self.stop)

        try:
            self._event_loop.run_forever()
        except Exception as e:
            LOG.error(_LE('Caught the exception in the event loop: %s'), e)
            LOG.error(traceback.format_exc())
            err_code = 1
        else:
            err_code = 0
        finally:
            if not self._event_loop.is_closed():
                self._event_loop.run_until_complete(
                    asyncio.gather(*asyncio.Task.all_tasks(
                        loop=self._event_loop)))
                self._event_loop.close()
            sys.exit(err_code)

    def stop(self, graceful=False):
        """Stops the event loop if it's not stopped already."""
        if hasattr(self, '_tasks') and self._tasks:
            LOG.info(_LI('Cancelling all the scheduled tasks'))
            for task, endpoint in self._tasks.items():
                LOG.info(_LI('Cancelling the watcher for "%s"'), endpoint)
                self._event_loop.call_soon_threadsafe(task.cancel)
        if self._event_loop.is_running():
            self._event_loop.stop()
        self._executor.shutdown(wait=True)
        self._sequential_executor.shutdown(wait=True)
        self._event_loop.close()

        super(Raven, self).stop(graceful)
        LOG.debug('Stopped the service: %s', self.__class__.__name__)

    def wait(self):
        """Waits for Raven to complete."""
        LOG.debug('Wait for the service: %s', self.__class__.__name__)
        super(Raven, self).wait()

    @asyncio.coroutine
    def _delegate(self, executor, func, *args, **kwargs):
        # run_in_executor of the event loop can't take the keyword args. So
        # all arguments are bound with functools.partial and create a new
        # function that takes no argument here.
        partially_applied_func = functools.partial(func, *args, **kwargs)
        result = yield from self._event_loop.run_in_executor(
            executor, partially_applied_func)
        return result

    @asyncio.coroutine
    def wait_for(self, time):
        """Waits for the specified time.

        The watchers or any other users of this class should call this method
        instead of ``asyncio.sleep``. This method uses the internal event loop
        and this is scheduled appropriately as well as other coroutines. This
        method is a coroutine.

        :param time: The waiting time in seconds.
        """
        yield from asyncio.sleep(time, loop=self._event_loop)

    @asyncio.coroutine
    def delegate(self, func, *args, **kwargs):
        """Delegates the execution of the passed function to this instance.

        The passed function or method is executed immediately in a different
        thread. This provides a generic abstraction for making the synchronized
        processes asynchronous.

        :param func:   The function or the method to be executed.
        :param args:   The arguments for the function, which can be the mixture
                       of regular and arbitrary arguments passed to the
                       function.
        :param kwargs: The keyword arguments passed to the function.
        :returns: The result of the execution of the passed function with the
                  arguments.
        """
        result = yield from self._delegate(
            self._executor, func, *args, **kwargs)
        return result

    @asyncio.coroutine
    def sequential_delegate(self, func, *args, **kwargs):
        """Delegates the sequential execution of the passed function.

        The passed function or the method is executed immediately in a
        different thread, but in a sequential way. This provides a generic
        abstraction for making the synchronized processes asynchronous.

        :param func:   The function or the method to be executed.
        :param args:   The arguments for the function, which can be the mixture
                       of regular and arbitrary arguments passed to the
                       function.
        :param kwargs: The keyword arguments passed to the function.
        :returns: The result of the execution of the passed function with the
                  arguments.
        """
        result = yield from self._delegate(
            self._sequential_executor, func, *args, **kwargs)
        return result

    @asyncio.coroutine
    def watch(self, endpoint, callback):
        """Watches the endpoint and calls the callback with its response.

        In this method the endpoint is assumed it keeps returning a JSON object
        line by line as its response. Otherwise the first line is consumed and
        the watch is finished after that.

        :param endpoint: The string of the API endpoint to be watched.
        :param callback: The function that is called with the decoded JSON
                         response returned by the HTTP call agaisnt the
                         endpoint.
        """
        canonical_endpoint = self._get_endpoint_with_resource_version(endpoint)
        response = yield from methods.get(endpoint=canonical_endpoint,
                                          loop=self._event_loop,
                                          decoder=utils.utf8_json_decoder)

        # Get headers
        status, reason, hdrs = yield from response.read_headers()
        if status != 200:
            LOG.error(_LE('GET request to endpoint %(ep)s failed with '
                          'status %(status)s and reason %(reason)s'),
                      {'ep': endpoint, 'status': status, 'reason': reason})
            raise requests.exceptions.HTTPError('{}: {}. Endpoint {}'.format(
                status, reason, endpoint))
        if hdrs.get(headers.TRANSFER_ENCODING) != 'chunked':
            LOG.error(_LE('watcher GET request to endpoint %(ep)s is not '
                          'chunked. headers: %(hdrs)s'),
                      {'ep': endpoint, 'hdrs': hdrs})
            raise IOError('Can only watch endpoints that returned chunked '
                          'encoded transfers')
        while True:
            try:
                content = yield from response.read_line()
            except asyncio.CancelledError:
                LOG.debug('Watch task of endpoint %s has been cancelled',
                          endpoint)
                break
            if content is None:
                LOG.debug('Watch task of endpoint %s has arrived at EOF',
                          endpoint)

                # Let's schedule another watch
                if self._reconnect:
                    LOG.debug('Scheduling a new watch task for endpoint %s',
                              endpoint)
                    next_watch = self._event_loop.create_task(self.watch(
                        endpoint, callback))
                    next_watch.add_done_callback(self._task_done_callback)
                    self._tasks[next_watch] = endpoint
                break
            else:
                obj = content.get('object', {})
                metadata = obj.get('metadata', {})
                if 'resourceVersion' in metadata:
                    resource_version = int(metadata['resourceVersion'])
                    yield from self._update_resource_version(resource_version)

                if asyncio.iscoroutinefunction(callback):
                    task = callback(content)
                else:
                    task = self.delegate(callback, content)

                try:
                    yield from task
                except asyncio.CancelledError:
                    LOG.debug('Watching endpoint %s was cancelled during '
                              'callback execution.', endpoint)
                    break


def run_raven():
    """Launchs a Raven service."""
    config.init(sys.argv[1:])
    log.setup(config.CONF, 'Raven')
    if config.CONF.raven.raven_single_shot:
        LOG.warning('This instance of raven will terminate after a single namespace sweep')
    raven = service.launch(config.CONF, Raven())
    raven.wait()
