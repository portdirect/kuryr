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

import abc
import asyncio
import ipaddress

from neutronclient.common import exceptions as n_exceptions
from oslo_log import log
from oslo_serialization import jsonutils
from oslo_utils import excutils
import requests
import six

from kuryr._i18n import _LE
from kuryr._i18n import _LI
from kuryr._i18n import _LW
from kuryr.common import config
from kuryr.common import constants
from kuryr.raven import aio
from kuryr.raven import models
from kuryr import utils


ADDED_EVENT = 'ADDED'
DELETED_EVENT = 'DELETED'
MODIFIED_EVENT = 'MODIFIED'

LOG = log.getLogger(__name__)

PATCH_HEADERS = {
    'Content-Type': 'application/merge-patch+json',
    'Accept': 'application/json',
}


@asyncio.coroutine
def _update_annotation(delegator, path, kind, annotations):
    data = {
        "kind": kind,
        "apiVersion": "v1",
    }
    metadata = {}
    metadata.update({'annotations': annotations})
    data.update({'metadata': metadata})

    response = yield from delegator(
        requests.patch, constants.K8S_API_ENDPOINT_BASE + path,
        data=jsonutils.dumps(data), headers=PATCH_HEADERS)
    assert response.status_code == requests.codes.ok
    LOG.debug("Successfully updated the annotations.")


def _get_endpoint_members(subsets):
    """Returns a set of tuples (address, port) of the endpoint members.

    :param subsets: The dictionary represents the subsets property.
    """
    members = set()
    for subset in subsets:
        ports = subset['ports']
        if 'addresses' not in subset:
            LOG.debug('Subset %s does not yet have addresses to process',
                      subset)
            continue
        addresses = subset['addresses']
        for port in ports:
            protocol_port = port['port']
            for address in addresses:
                members.add(models.PoolMember(address['ip'], protocol_port))
    return members


def _get_pool_members(pool_members):
    """Returns a set of tuples (address, port) of the pool members.

    :param pool_members: The response dictionary of listing pool members with
                         neutronclient
    """
    members = set()
    for member in pool_members:
        members.add(models.PoolMember(member['address'],
                                      member['protocol_port'],
                                      member_id=member['id']))
    return members


@asyncio.coroutine
def _add_pool_member(delegator, client, service_name, pool_id, subnet_id, address, protocol_port):
    """Creates an LBaaS member

    :param delegator: Object to delegate the creation of the member
    :param client: neutron client instance
    :param pool_id: uuid of the pool the member will be part of
    :param address: IPv4 address object of the LBaaS pool member
    :param port: Protocol port for the LBaaS pool to access the member
    """
    # Get LBs for pool
    try:
        neutron_pool_response = yield from delegator(
            client.show_lbaas_pool, lbaas_pool=pool_id)
        neutron_loadbalancers = neutron_pool_response['pool']['loadbalancers']
        LOG.debug('neutron_loadbalancers %s', neutron_loadbalancers)
    except n_exceptions.NeutronClientException as ex:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE("Error happened retriving Neutron"
                          "loadbalancers: %s"), ex)

    neutron_loadbalancer_id = neutron_loadbalancers[0]['id']
    LOG.debug('neutron loadbalancers id: %s', neutron_loadbalancer_id)

    # Wait for lb to be provisioned
    lb_provsisoned = False
    LOG.debug('Waiting for lb to be active before attempting to add new member')
    while not lb_provsisoned:
        try:
            neutron_loadbalancer_response = yield from delegator(
                client.show_loadbalancer, lbaas_loadbalancer=neutron_loadbalancer_id)
            neutron_loadbalancer = neutron_loadbalancer_response['loadbalancer']['provisioning_status']
        except n_exceptions.NeutronClientException as ex:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("Error happened during monitoring a"
                              " Neutron loadbalancer: %s"), ex)
        if neutron_loadbalancer == 'ACTIVE':
            lb_provsisoned = True

    response = yield from delegator(
        client.create_lbaas_member, pool_id,
        {
            'member': {
                'subnet_id': subnet_id,
                'address': str(address),
                'protocol_port': protocol_port,
                'weight': 1,
            },
        })

    LOG.debug('Successfully created a new member %(member)s for the pool '
              '%(pool_id)s',
              {'member': response['member'], 'pool_id': pool_id})


@asyncio.coroutine
def _del_pool_member(delegator, client, pool_id, member_id):
    """Creates an LBaaS member

    :param delegator: Object to delegate the creation of the member
    :param client: neutron client instance
    :param member_id: uuid object of the pool member to delete
    """
    yield from delegator(
        client.delete_lbaas_member, lbaas_member=str(member_id), lbaas_pool=pool_id)
    LOG.debug('Successfully deleted LBaaS pool member %s.', member_id)


@six.add_metaclass(abc.ABCMeta)
class K8sAPIWatcher(object):
    """A K8s API watcher interface for watching and translating K8s resources.

    This is an abstract class and intended to be interited and conformed its
    abstract property and method by its subclasses. ``WATCH_ENDPOINT``
    represents the API endpoint to watch and ``translate`` is called every time
    when the event notifications are propagated.
    """
    @abc.abstractproperty
    def WATCH_ENDPOINT(self):
        """Gives the K8s API endpoint to be watched and translated.

        This property represents the K8s API endpoint which response is
        consumed by ``translate`` method. Although this is defined as a
        property, the subclasses can just have it as the class level attribute,
        which hides this abstract property.
        """

    @abc.abstractmethod
    def translate(self, deserialized_json):
        """Translates an event notification from the apiserver.

        This method tranlates the piece of JSON responses into requests against
        the Neutron API. Subclasses of ``K8sAPIWatcher`` **must** implement
        this method to have the concrete translation logic for the specific
        one or more resources.

        This method may be a coroutine function, a decorated generator function
        or an ``async def`` function.

        :param deserialized_json: the deserialized JSON resoponse from the
                                  apiserver
        """


class K8sPodsWatcher(K8sAPIWatcher):
    """A Pod watcher.

    ``K8sPodsWatcher`` makes a GET request against ``/api/v1/pods?watch=true``
    and receives the event notifications. Then it translates them, when
    applicable, into requests against the Neutron API.

    An example of a JSON response from the apiserver follows. It is
    pretty-printed but the actual response is provided as a single line of
    JSON.
    ::

      {
        "type": "ADDED",
        "object": {
          "kind": "Pod",
          "apiVersion": "v1",
          "metadata": {
            "name": "frontend-qr8d6",
            "generateName": "frontend-",
            "namespace": "default",
            "selfLink": "/api/v1/namespaces/default/pods/frontend-qr8d6",
            "uid": "8e174673-e03f-11e5-8c79-42010af00003",
            "resourceVersion": "107227",
            "creationTimestamp": "2016-03-02T06:25:27Z",
            "labels": {
              "app": "guestbook",
              "tier": "frontend"
            },
            "annotations": {
              "kubernetes.io/created-by": {
                "kind": "SerializedReference",
                "apiVersion": "v1",
                "reference": {
                  "kind": "ReplicationController",
                  "namespace": "default",
                  "name": "frontend",
                  "uid": "8e1657d9-e03f-11e5-8c79-42010af00003",
                  "apiVersion": "v1",
                  "resourceVersion": "107226"
                }
              }
            }
          },
          "spec": {
            "volumes": [
              {
                "name": "default-token-wpfjn",
                "secret": {
                  "secretName": "default-token-wpfjn"
                }
              }
            ],
            "containers": [
              {
                "name": "php-redis",
                "image": "gcr.io/google_samples/gb-frontend:v3",
                "ports": [
                  {
                    "containerPort": 80,
                    "protocol": "TCP"
                  }
                ],
                "env": [
                  {
                    "name": "GET_HOSTS_FROM",
                    "value": "dns"
                  }
                ],
                "resources": {
                  "requests": {
                    "cpu": "100m",
                    "memory": "100Mi"
                  }
                },
                "volumeMounts": [
                  {
                    "name": "default-token-wpfjn",
                    "readOnly": true,
                    "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount"  # noqa
                  }
                ],
                "terminationMessagePath": "/dev/termination-log",
                "imagePullPolicy": "IfNotPresent"
              }
            ],
            "restartPolicy": "Always",
            "terminationGracePeriodSeconds": 30,
            "dnsPolicy": "ClusterFirst",
            "serviceAccountName": "default",
            "serviceAccount": "default",
            "securityContext": {}
          },
          "status": {
            "phase": "Pending"
          }
        }
      }
    """
    PODS_ENDPOINT = constants.K8S_API_ENDPOINT_V1 + '/pods'
    WATCH_ENDPOINT = PODS_ENDPOINT + '?watch=true'

    @asyncio.coroutine
    def translate(self, decoded_json):
        """Translates a K8s pod into a Neutron port.

        The service translation can be assumed to be done before replication
        controllers and pods are created based on the "best practice" of K8s
        resource definition. So in this method pods are translated into ports
        based on the service information.

        When the port is created, the pod information is updated with the port
        information to provide the necessary information for the bindings.

        If the pod belongs to the service and the pod is deleted, the
        associated pool member is deleted as well in the cascaded way.

        :param decoded_json: A pod event to be translated.
        """
        @asyncio.coroutine
        def get_networks(network_name):
            networks_response = yield from self.delegate(
                self.neutron.list_networks,
                name=network_name)
            networks = networks_response['networks']
            return networks

        @asyncio.coroutine
        def get_subnets(subnet_name):
            subnets_response = yield from self.delegate(
                self.neutron.list_subnets, name=subnet_name)
            subnets = subnets_response['subnets']
            return subnets

        LOG.debug("Pod notification %s", decoded_json)
        event_type = decoded_json.get('type', '')
        content = decoded_json.get('object', {})
        metadata = content.get('metadata', {})
        annotations = metadata.get('annotations', {})
        labels = metadata.get('labels', {})
        namespace = metadata.get('namespace')

        spec = content.get('spec', {})
        hostnetwork = spec.get('hostNetwork')

        if hostnetwork is None:
            LOG.debug('Unable to obtain pods hostnetwork status, assuming that we can continue')
        elif hostnetwork:
            LOG.debug('Ignoring POD as its running in the hosts network namespace')
            return

        if event_type == ADDED_EVENT:
            with (yield from self.namespace_added):
                namespace_network_name = namespace
                namespace_subnet_name = utils.get_subnet_name(namespace)

                namespace_networks = yield from get_networks(
                    namespace_network_name)
                namespace_subnets = yield from get_subnets(
                    namespace_subnet_name)
                # Wait until the namespace translation is done.
                while not (namespace_networks and namespace_subnets):
                    yield from self.namespace_added.wait()
                    namespace_networks = yield from get_networks(
                        namespace_network_name)
                    namespace_subnets = yield from get_subnets(
                        namespace_subnet_name)
                namespace_network = namespace_networks[0]
                namespace_subnet = namespace_subnets[0]

                if constants.K8S_ANNOTATION_PORT_KEY in annotations:
                    LOG.debug('Ignore an ADDED event as the pod already has a '
                              'neutron port')
                    return
                sg = labels.get(constants.K8S_LABEL_SECURITY_GROUP_KEY,
                                self._default_sg)
                new_port = {
                    'name': metadata.get('name', ''),
                    'network_id': namespace_network['id'],
                    'admin_state_up': True,
                    'device_owner': constants.DEVICE_OWNER,
                    'fixed_ips': [{'subnet_id': namespace_subnet['id']}],
                    'security_groups': [sg]
                }
                try:
                    created_port = yield from self.delegate(
                        self.neutron.create_port, {'port': new_port})
                    port = created_port['port']
                    LOG.debug("Successfully create a port %s.", port)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        # REVISIT(yamamoto): We ought to report to a user.
                        # eg. marking the pod error.
                        LOG.error(_LE("Error happened during creating a"
                                      " Neutron port: %s"), ex)

                # Update the port device id to point to itself
                # this makes horizon integration much simpler.
                new_port_update = {
                    'device_id': port['id'],
                }
                try:
                    created_port_update = yield from self.delegate(
                        self.neutron.update_port,  port['id'], {'port': new_port_update})
                    port = created_port_update['port']
                    LOG.debug("Successfully updated port %s.", port)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        # REVISIT(yamamoto): We ought to report to a user.
                        # eg. marking the pod error.
                        LOG.error(_LE("Error happened during creating a"
                                      " Neutron port: %s"), ex)

                path = metadata.get('selfLink', '')
                annotations.update(
                    {constants.K8S_ANNOTATION_PORT_KEY: jsonutils.dumps(port)})
                annotations.update(
                    {constants.K8S_ANNOTATION_SUBNET_KEY: jsonutils.dumps(
                        namespace_subnet)})
                if path:
                    yield from _update_annotation(self.delegate, path, 'Pod',
                                                  annotations)

        elif event_type == DELETED_EVENT:
            with (yield from self.namespace_deleted):
                neutron_port = jsonutils.loads(
                    annotations.get(constants.K8S_ANNOTATION_PORT_KEY, '{}'))
                if neutron_port:
                    port_id = neutron_port['id']
                    try:
                        yield from self.delegate(
                            self.neutron.delete_port, port_id)
                    except n_exceptions.PortNotFoundClient:
                        LOG.info(_LI('Neutron port %s had already been '
                                     'deleted. Nothing remaining to do'),
                                 port_id)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error happend during deleting a"
                                          " Neutron port: %s"), ex)
                    LOG.debug("Successfully deleted the neutron port.")
                    # Notify the namespace deletion is ready to be resumed.
                    self.namespace_deleted.notify_all()
                else:
                    LOG.debug('Deletion event without neutron port '
                              'information. Ignoring it...')
        elif event_type == MODIFIED_EVENT:
            old_port = annotations.get(constants.K8S_ANNOTATION_PORT_KEY)
            if old_port:
                sg = labels.get(constants.K8S_LABEL_SECURITY_GROUP_KEY,
                                self._default_sg)
                port_id = jsonutils.loads(old_port)['id']
                update_req = {
                    'security_groups': [sg],
                }
                try:
                    updated_port = yield from self.delegate(
                        self.neutron.update_port,
                        port=port_id, body={'port': update_req})
                    port = updated_port['port']
                    LOG.debug("Successfully update a port %s.", port)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        # REVISIT(yamamoto): We ought to report to a user.
                        # eg. marking the pod error.
                        LOG.error(_LE("Error happened during updating a"
                                      " Neutron port: %s"), ex)
                # REVISIT(yamamoto): Do we want to update the annotation
                # with the new SG?  Probably.  Note that updating
                # annotation here would yield another MODIFIED_EVENT.


class K8sNamespaceWatcher(K8sAPIWatcher):
    """A namespace watcher.

    ``K8sNamespacesWatcher`` makes a GET request against
    ``/api/v1/namespaces?watch=true`` and receives the event notifications.
    Then it translates them into requrests against the Neutron API.

    An example of a JSON response follows. It is pretty-printed but the
    actual response is provided as a single line of JSON.
    ::

      {
        "type": "ADDED",
        "object": {
          "kind": "Namespace",
          "apiVersion": "v1",
          "metadata": {
            "name": "test",
            "selfLink": "/api/v1/namespaces/test",
            "uid": "f094ea6b-06c2-11e6-8128-42010af00003",
            "resourceVersion": "497821",
            "creationTimestamp": "2016-04-20T06:41:41Z"
          },
          "spec": {
            "finalizers": [
              "kubernetes"
            ]
          },
          "status": {
            "phase": "Active"
          }
        }
      }
    """
    NAMESPACES_ENDPOINT = constants.K8S_API_ENDPOINT_V1 + '/namespaces'
    WATCH_ENDPOINT = NAMESPACES_ENDPOINT + '?watch=true'

    @asyncio.coroutine
    def translate(self, decoded_json):
        """Translates a K8s namespace into two Neutron networks and subnets.

        The two pairs of the network and the subnet are created for the cluster
        network. Each subnet is associated with its dedicated network. They're
        named in the way the administrator can recognise what they're easily
        based on the names of the namespaces.

        :param decoded_json: A namespace event to be translated.
        """
        @asyncio.coroutine
        def get_ports(network_id):
            neutron_ports_response = yield from self.delegate(
                self.neutron.list_ports, network_id=neutron_network_id)
            neutron_ports = neutron_ports_response['ports']
            return neutron_ports

        LOG.debug("Namespace notification %s", decoded_json)
        event_type = decoded_json.get('type', '')
        content = decoded_json.get('object', {})
        metadata = content.get('metadata', {})
        annotations = metadata.get('annotations', {})
        if event_type == ADDED_EVENT:
            with (yield from self.namespace_added):
                namespace_network_name = metadata['name']
                namespace_subnet_name = utils.get_subnet_name(
                    namespace_network_name)
                namespace_networks_response = yield from self.delegate(
                    self.neutron.list_networks,
                    name=namespace_network_name)
                namespace_networks = namespace_networks_response['networks']

                # Ensure the network exists
                if namespace_networks:
                    namespace_network = namespace_networks[0]
                else:
                    # NOTE(devvesa): To avoid name collision, we should add the
                    #                uid of the namespace in the neutron tags
                    #                info
                    network_response = yield from self.delegate(
                        self.neutron.create_network,
                        {'network': {'name': namespace_network_name}})
                    namespace_network = network_response['network']
                    LOG.debug('Created a new network %s', namespace_network)
                    annotations.update(
                        {constants.K8S_ANNOTATION_NETWORK_KEY: jsonutils.dumps(
                            namespace_network)})

                # Ensure the subnet exists
                namespace_subnets_response = yield from self.delegate(
                    self.neutron.list_subnets,
                    name=namespace_subnet_name)
                namespace_subnets = namespace_subnets_response['subnets']
                if namespace_subnets and (
                        constants.K8S_ANNOTATION_SUBNET_KEY in annotations):
                    namespace_subnet = namespace_subnets[0]
                else:
                    new_subnet = {
                        'name': namespace_subnet_name,
                        'network_id': namespace_network['id'],
                        'enable_dhcp': 'false',
                        'ip_version': 4,  # TODO(devvesa): parametrize this
                        'subnetpool_id': self._subnetpool['id'],
                    }
                    subnet_response = yield from self.delegate(
                        self.neutron.create_subnet, {'subnet': new_subnet})
                    namespace_subnet = subnet_response['subnet']
                    LOG.debug('Created a new subnet %s', namespace_subnet)

                annotations.update(
                    {constants.K8S_ANNOTATION_SUBNET_KEY: jsonutils.dumps(
                        namespace_subnet)})

                neutron_network_id = namespace_network['id']
                # Router is created in the subnet pool at raven start time.
                neutron_router_id = self._router['id']
                neutron_subnet_id = namespace_subnet['id']
                filtered_ports_response = yield from self.delegate(
                    self.neutron.list_ports,
                    device_owner='network:router_interface',
                    device_id=neutron_router_id,
                    network_id=neutron_network_id)
                filtered_ports = filtered_ports_response['ports']

                router_ports = self._get_router_ports_by_subnet_id(
                    neutron_subnet_id, filtered_ports)

                if not router_ports:
                    yield from self.delegate(
                        self.neutron.add_interface_router,
                        neutron_router_id, {'subnet_id': neutron_subnet_id})
                else:
                    LOG.debug('The subnet %s is already bound to the router',
                              neutron_subnet_id)

                path = metadata.get('selfLink', '')
                metadata.update({'annotations': annotations})
                content.update({'metadata': metadata})
                headers = {
                    'Content-Type': 'application/merge-patch+json',
                    'Accept': 'application/json',
                }
                response = yield from self.delegate(
                    requests.patch, constants.K8S_API_ENDPOINT_BASE + path,
                    data=jsonutils.dumps(content), headers=headers)
                assert response.status_code == requests.codes.ok

                # Notify the namespace translation is done.
                self.namespace_added.notify_all()
                LOG.debug("Successfully updated the annotations.")
        elif event_type == DELETED_EVENT:
            namespace_network = jsonutils.loads(
                annotations.get(constants.K8S_ANNOTATION_NETWORK_KEY, '{}'))
            namespace_subnet = jsonutils.loads(
                annotations.get(constants.K8S_ANNOTATION_SUBNET_KEY, '{}'))

            neutron_network_id = namespace_network.get('id', None)
            neutron_router_id = self._router.get('id', None)
            neutron_subnet_id = namespace_subnet.get('id', None)

            if namespace_network:
                try:
                    yield from self.delegate(
                        self.neutron.remove_interface_router,
                        neutron_router_id, {'subnet_id': neutron_subnet_id})

                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happend during deleting a "
                                      "router port: %s"), ex)

                # Wait until all the ports are deleted.
                ports = yield from get_ports(neutron_network_id)
                while ports:
                    yield from self.namespace_deleted.wait()
                    ports = yield from get_ports(neutron_network_id)

                try:
                    yield from self.delegate(
                        self.neutron.delete_network, neutron_network_id)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happend during deleting a"
                                      " Neutron Network: %s"), ex)
                LOG.debug("Successfully deleted the neutron network.")
            else:
                LOG.debug('Deletion event without neutron network information.'
                          'Ignoring it...')

        LOG.debug('Successfully translated the namespace')


class K8sServicesWatcher(K8sAPIWatcher):
    """A service watcher.

    ``K8sServicesWatcher`` makes a GET request against
    ``/api/v1/services?watch=true`` and receives the event notifications. Then
    it translates them into requrests against the Neutron API.

    An example of a JSON response follows. It is pretty-printed but the
    actual response is provided as a single line of JSON.
    ::

      {
        "type": "ADDED",
        "object": {
          "kind": "Service",
          "apiVersion": "v1",
          "metadata": {
            "name": "kubernetes",
            "namespace": "default",
            "selfLink": "/api/v1/namespaces/default/services/kubernetes",
            "uid": "7c8c674f-d6ed-11e5-8c79-42010af00003",
            "resourceVersion": "7",
            "creationTimestamp": "2016-02-19T09:45:18Z",
            "labels": {
              "component": "apiserver",
              "provider": "kubernetes"
            }
          },
          "spec": {
            "ports": [
              {
                "name": "https",
                "protocol": "TCP",
                "port": 443,
                "targetPort": 443
              }
            ],
            "clusterIP": "192.168.3.1",
            "type": "ClusterIP",
            "sessionAffinity": "None"
          },
          "status": {
            "loadBalancer": {}
          }
        }
      }
    """
    SERVICES_ENDPOINT = constants.K8S_API_ENDPOINT_V1 + '/services'
    WATCH_ENDPOINT = SERVICES_ENDPOINT + '?watch=true'

    @asyncio.coroutine
    def translate(self, decoded_json):
        """Translates a K8s service into a Neutron Pool and a Neutron VIP.

        The service translation can be assumed to be done before replication
        controllers and pods are created based on the "best practice" of K8s
        resource definition. So in this mothod only the Neutorn Pool and the
        Neutorn VIP are created. The Neutron Pool Members are added in the
        namespace translations.

        When the Neutron Pool is created, the service is updated with the Pool
        information in order that the namespace event translator can associate
        the Neutron Pool Members with the Pool. The namspace event traslator
        inspects the service information in the apiserver and retrieve the
        necessary Pool information.

        :param decoded_json: A service event to be translated.
        """
        def get_subnets(subnet_name):
            cluster_subnet_response = yield from self.delegate(
                self.neutron.list_subnets, name=cluster_subnet_name)
            cluster_subnets = cluster_subnet_response['subnets']
            return cluster_subnets

        LOG.debug("Service notification %s", decoded_json)
        event_type = decoded_json.get('type', '')
        content = decoded_json.get('object', {})
        metadata = content.get('metadata', {})
        annotations = metadata.get('annotations', {})
        service_name = metadata.get('name', '')
        if service_name == 'kubernetes':
            LOG.debug('Ignore "kubernetes" infra service')
            return

        service_spec = content.get('spec', {})
        service_type = service_spec.get('type', 'ClusterIP')
        if service_type == 'NodePort':
            LOG.warning(
                _LW('NodePort type service is not supported. '
                    'Ignoring the event.'))
            return

        if event_type == ADDED_EVENT:
            # Ensure the namespace translation is done.
            with (yield from self.namespace_added):
                if constants.K8S_ANNOTATION_POOL_KEY in annotations:
                    LOG.debug('Ignore an ADDED event as the pool already has '
                              'a neutron port')
                    return
                namespace = metadata.get(
                    'namespace', constants.K8S_DEFAULT_NAMESPACE)
                cluster_subnet_name = utils.get_subnet_name(namespace)
                cluster_subnets = yield from get_subnets(cluster_subnet_name)
                # Wait until the namespace translation is done.
                while not cluster_subnets:
                    self.namespace_added.wait()
                    cluster_subnets = yield from get_subnets(
                        cluster_subnet_name)
                cluster_subnet = cluster_subnets[0]

            # Service translation starts here.
            with (yield from self.service_added):

                service_ports = service_spec.get('ports', [])
                # Assume there's the only single port spec.
                port = service_ports[0]
                protocol = port['protocol']
                protocol_port = port['port']

                # Get the cluserIP from kube.
                cluster_ip = service_spec['clusterIP']

                # Create a loadbalancer.
                lb_request = {
                    'loadbalancer': {
                        'name': namespace,
                        'provider': 'haproxy',
                        'vip_subnet_id': self._service_subnet['id'],
                        'vip_address': cluster_ip,
                        'description': service_name,
                    },
                }
                try:
                    created_lb = yield from self.delegate(
                        self.neutron.create_loadbalancer, lb_request)
                    lb = created_lb['loadbalancer']
                    LOG.debug('Succeeded to created a lb %s', lb)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happened during creating a"
                                      " Neutron lb: %s"), ex)
                annotations.update(
                    {constants.K8S_ANNOTATION_VIP_KEY: jsonutils.dumps(lb)})

                # Create a listener.
                listener_request = {
                    'listener': {
                        'name': service_name,
                        'protocol_port': protocol_port,
                        'protocol': protocol,
                        'loadbalancer_id': lb['id'],
                        'description': 'k8s-' + service_name,
                    },
                }
                try:
                    created_listener = yield from self.delegate(
                        self.neutron.create_listener, listener_request)
                    listener = created_listener['listener']
                    LOG.debug('Succeeded to created a listener %s', listener)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happened during creating a"
                                      " Neutron listener: %s"), ex)
                annotations.update(
                    {constants.K8S_ANNOTATION_LISTENER_KEY: jsonutils.dumps(listener)})

                # Wait for lb to be provisioned
                LOG.debug('Waiting for lb to provision before adding pool')
                lb_provsisoned = False
                while not lb_provsisoned:
                    try:
                        neutron_loadbalancer_response = yield from self.delegate(
                            self.neutron.show_loadbalancer, lbaas_loadbalancer=lb['id'])
                        neutron_loadbalancer = neutron_loadbalancer_response['loadbalancer']
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error happened during monitoring a"
                                          " Neutron loadbalancer: %s"), ex)
                    if neutron_loadbalancer['provisioning_status'] == 'ACTIVE':
                        lb_provsisoned = True

                annotations.update(
                    {constants.K8S_ANNOTATION_VIP_KEY: jsonutils.dumps(neutron_loadbalancer)})

                # Create a pool.
                pool_request = {
                    'pool': {
                        'name': service_name,
                        'protocol': protocol,
                        'listener_id': listener['id'],
                        'lb_algorithm': config.CONF.raven.lb_method,
                    },
                }
                try:
                    created_pool = yield from self.delegate(
                        self.neutron.create_lbaas_pool, pool_request)
                    pool = created_pool['pool']
                    LOG.debug('Succeeded to created a pool %s', pool)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happened during creating a"
                                      " Neutron pool: %s"), ex)


                annotations.update(
                    {constants.K8S_ANNOTATION_POOL_KEY: jsonutils.dumps(pool)})


                # add security group rule in the default security group to
                # allow access from the VIP to all containers
                sgs = self.neutron.list_security_groups(
                    name=constants.K8S_HARDCODED_SG_NAME)
                if sgs:
                    sg = sgs['security_groups'][0]
                else:
                    raise Exception('Security group should be already created'
                                    ' at this point')

                if ipaddress.ip_address(cluster_ip).version == 4:
                    ip_version = 'IPv4'
                else:
                    ip_version = 'IPv6'

                rule = {
                    'security_group_id': sg['id'],
                    'ethertype': ip_version,
                    'direction': 'ingress',
                    'remote_ip_prefix': '%s/32' % cluster_ip,
                }
                req = {
                    'security_group_rule': rule,
                }
                LOG.debug('Creating SG rule %s', req)
                self.neutron.create_security_group_rule(req)

                if service_type == 'LoadBalancer':
                    LOG.debug('Creating floating IP for service')
                    # Create a fip.
                    fip_request = {
                        'floatingip': {
                            'floating_network_id': config.CONF.raven.public_net_id,
                            'port_id': neutron_loadbalancer['vip_port_id'],
                        },
                    }
                    try:
                        created_fip = yield from self.delegate(
                            self.neutron.create_floatingip, fip_request)
                        fip = created_fip['floatingip']
                        LOG.debug('Succeeded to created a floating ip %s', fip)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error happened during creating a"
                                          " Neutron fip: %s"), ex)


                    LOG.debug('Creating security group for service floating IP')
                    # Create a sg.
                    fipsg_request = {
                        'security_group': {
                            'name': 'raven-' + service_name + '-sg',
                        },
                    }
                    try:
                        created_fipsg = yield from self.delegate(
                            self.neutron.create_security_group, fipsg_request)
                        fipsg = created_fipsg['security_group']
                        LOG.debug('Succeeded to created a security group for floating ip %s', fipsg)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error during creation of security"
                                          "group for floating IP: %s"), ex)

                    LOG.debug('Creating security group rules for service floating IP')
                    # Create a sg.
                    fipsgrules_request = {
                        'security_group_rule': {
                            'direction': 'ingress',
                            'protocol': protocol,
                            'ethertype': 'IPv4',
                            'port_range_max': protocol_port,
                            'port_range_min': protocol_port,
                            'remote_ip_prefix': '0.0.0.0/0',
                            'security_group_id': fipsg['id'],
                        },
                    }
                    try:
                        created_fipsgrules = yield from self.delegate(
                            self.neutron.create_security_group_rule, fipsgrules_request)
                        fipsgrules = created_fipsgrules['security_group_rule']
                        LOG.debug('Succeeded to created security group rules for  floating ip %s', fipsgrules)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error during creation of security"
                                          "group rules for floating IP: %s"), ex)


                    LOG.debug('Attaching security group to service floating IP')
                    # Create a port update.
                    fipsgupdate_request = {
                        'port': {
                            'security_groups': [ fipsg['id'] ],
                        },
                    }
                    try:
                        created_fipsgupdate = yield from self.delegate(
                            self.neutron.update_port, fip['port_id'], fipsgupdate_request)
                        fipsgupdate = created_fipsgupdate['port']
                        LOG.debug('Attached security group to service floating IP %s', fipsgupdate)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error during attachment of security"
                                          "group to floating IP: %s"), ex)

                    annotations.update(
                        {constants.K8S_ANNOTATION_FIP_KEY: jsonutils.dumps(fip)})
                    annotations.update(
                        {constants.K8S_ANNOTATION_FIPSG_KEY: jsonutils.dumps(fipsg)})


                path = metadata.get('selfLink', '')
                if path:
                    yield from _update_annotation(
                        self.delegate, path, 'Service', annotations)
                # Notify the service translation is done to
                # K8sEndpointsWatcher.
                self.service_added.notify()

        elif event_type == DELETED_EVENT:
            with (yield from self.service_deleted):
                neutron_pool = jsonutils.loads(
                    annotations.get(constants.K8S_ANNOTATION_POOL_KEY, '{}'))
                if not neutron_pool:
                    LOG.debug('Deletion event without neutron pool '
                              'information. Ignoring it.')
                    return
                neutron_listener = jsonutils.loads(
                    annotations.get(constants.K8S_ANNOTATION_LISTENER_KEY, '{}'))
                if not neutron_listener:
                    LOG.debug('Deletion event without neutron listener '
                              'information. Ignoring it.')
                    return
                neutron_vip = jsonutils.loads(
                    annotations.get(constants.K8S_ANNOTATION_VIP_KEY, '{}'))
                if not neutron_vip:
                    LOG.debug('Deletion event without neutron VIP information.'
                              ' Ignoring it.')
                    return

                if service_type == 'LoadBalancer':
                    neutron_fip = jsonutils.loads(
                        annotations.get(constants.K8S_ANNOTATION_FIP_KEY, '{}'))
                    if not neutron_fip:
                        LOG.debug('Deletion event without neutron FIP information.'
                                  ' Ignoring it.')
                        return

                    # Delete the fip for out service.
                    try:
                        fip_id = neutron_fip['id']
                        LOG.debug('fip_id %s', fip_id)
                        neutron_fip_response = yield from self.delegate(
                            self.neutron.show_floatingip, floatingip=fip_id)
                        LOG.debug('neutron_fip_response %s', neutron_fip_response)

                        neutron_fip = neutron_fip_response['floatingip']['id']
                        LOG.debug('neutron_fip %s', neutron_fip)
                        if neutron_fip:
                            yield from self.delegate(
                                self.neutron.delete_floatingip, fip_id)
                        else:
                            LOG.warning(_LW("The fip %s doesn't exist. Ignoring "
                                            "the deletion of the fip."), fip_id)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error happened during deleting a"
                                          " Neutron fip: %s"), ex)
                    LOG.debug('Successfully deleted the Neutron fip %s',  neutron_fip)

                # Delete the listener for out service.
                try:
                    listener_id = neutron_listener['id']
                    LOG.debug('listener_id %s', listener_id)

                    neutron_listeners_response = yield from self.delegate(
                        self.neutron.show_listener, lbaas_listener=listener_id)
                    LOG.debug('neutron_listeners_response %s', neutron_listeners_response)

                    neutron_listeners = neutron_listeners_response['listener']['id']
                    LOG.debug('neutron_listeners %s', neutron_listeners)
                    if neutron_listeners:
                        yield from self.delegate(
                            self.neutron.delete_listener, listener_id)
                    else:
                        LOG.warning(_LW("The Listener %s doesn't exist. Ignoring "
                                        "the deletion of the VIP."), listener_id)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happened during deleting a"
                                      " Neutron VIP: %s"), ex)
                LOG.debug('Successfully deleted the Neutron Listener %s',  neutron_listeners)

                # delete the pool that contains our service endpoints
                try:
                    pool_id = neutron_pool['id']
                    neutron_pools_response = yield from self.delegate(
                        self.neutron.show_lbaas_pool, lbaas_pool=pool_id)
                    neutron_pools = neutron_pools_response['pool']['id']
                    if neutron_pools:
                        yield from self.delegate(
                            self.neutron.delete_lbaas_pool, pool_id)
                    else:
                        LOG.warning(_LW("The pool %s doesn't exist. Ignoring "
                                        "the  deletion of the pool."), vip_id)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happened during deleting a"
                                      " Neutron pool: %s"), ex)
                LOG.debug('Successfully deleted the Neutron pool %s',
                          neutron_pool)


                # delete security group rule in the default security group for
                # the LB VIP we are about to deleted
                sgs = self.neutron.list_security_groups(
                    name=constants.K8S_HARDCODED_SG_NAME)
                if sgs:
                    sg = sgs['security_groups'][0]
                else:
                    raise Exception('Security group should be already created'
                                    ' at this point')

                vip_address = '%s/32' % neutron_vip['vip_address']
                LOG.debug('vip_address: %s', vip_address)

                sgrs = self.neutron.list_security_group_rules(
                    security_group_id=sg['id'],
                    remote_ip_prefix=vip_address)
                if sgrs:
                    sgr = sgrs['security_group_rules'][0]
                    self.neutron.delete_security_group_rule(sgr['id'])
                    LOG.debug('Successfully deleted the security group rule %s', sgr['id'])


                # Delete the actual LB.
                try:
                    vip_id = neutron_vip['id']
                    neutron_vip_response = yield from self.delegate(
                        self.neutron.show_loadbalancer, lbaas_loadbalancer=vip_id)
                    neutron_vips = neutron_vip_response['loadbalancer']['id']
                    if neutron_vips:
                        yield from self.delegate(
                            self.neutron.delete_loadbalancer, vip_id)
                    else:
                        LOG.warning(_LW("The lb %s doesn't exist. Ignoring "
                                        "the  deletion of the lb."), vip_id)
                except n_exceptions.NeutronClientException as ex:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE("Error happened during deleting a"
                                      " Neutron lb: %s"), ex)
                LOG.debug('Successfully deleted the Neutron lb %s',
                          vip_id)


                # Delete the lb security group.
                if service_type == 'LoadBalancer':
                    neutron_fipsg = jsonutils.loads(
                        annotations.get(constants.K8S_ANNOTATION_FIPSG_KEY, '{}'))
                    if not neutron_fip:
                        LOG.debug('Deletion event without neutron FIPSG information.'
                                  ' Ignoring it.')
                        return

                    # Delete the fipsg for out service.
                    try:
                        fipsg_id = neutron_fipsg['id']
                        LOG.debug('fipsg_id %s', fipsg_id)
                        neutron_fipsg_response = yield from self.delegate(
                            self.neutron.show_security_group, security_group=fipsg_id)
                        LOG.debug('neutron_fipsg_response %s', neutron_fipsg_response)

                        neutron_fipsg = neutron_fipsg_response['security_group']['id']
                        LOG.debug('neutron_fipsg %s', neutron_fipsg)
                        if neutron_fipsg:
                            yield from self.delegate(
                                self.neutron.delete_security_group, fipsg_id)
                        else:
                            LOG.warning(_LW("The fipsg %s doesn't exist. Ignoring "
                                            "the deletion of the fipsg."), fip_id)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE("Error happened during deleting a"
                                          " Neutron fipsg: %s"), ex)
                    LOG.debug('Successfully deleted the Neutron fipsg %s',  neutron_fipsg)

                self.service_deleted.notify()


class K8sEndpointsWatcher(K8sAPIWatcher):
    """An endpoints watcher.

    ``K8sEndpointsWatcher`` makes a GET request against
    ``/api/v1/endpoints?watch=true`` and receives the event notifications. Then
    it translates them into requrests against the Neutron API.

    An example of a JSON response follows. It is pretty-printed but the
    actual response is provided as a single line of JSON.
    ::

      {
        "type": "ADDED",
        "object": {
          "kind": "Endpoints",
          "apiVersion": "v1",
          "metadata": {
            "name": "frontend",
            "namespace": "default",
            "selfLink": "/api/v1/namespaces/default/endpoints/frontend",
            "uid": "436bf3f9-1e53-11e6-8128-42010af00003",
            "resourceVersion": "1034915",
            "creationTimestamp": "2016-05-20T06:22:44Z",
            "labels": {
              "app": "guestbook",
              "tier": "frontend"
            }
          },
          "subsets": [
            {
              "addresses": [
                {
                  "ip": "172.16.0.77",
                  "targetRef": {
                    "kind": "Pod",
                    "namespace": "default",
                    "name": "frontend-g607i",
                    "uid": "43748958-1e53-11e6-8128-42010af00003",
                    "resourceVersion": "1034914"
                  }
                },
                {
                  "ip": "172.16.0.78",
                  "targetRef": {
                    "kind": "Pod",
                    "namespace": "default",
                    "name": "frontend-hl8ic",
                    "uid": "4374c8f0-1e53-11e6-8128-42010af00003",
                    "resourceVersion": "1034899"
                  }
                },
                {
                  "ip": "172.16.0.79",
                  "targetRef": {
                    "kind": "Pod",
                    "namespace": "default",
                    "name": "frontend-blc48",
                    "uid": "4374dd81-1e53-11e6-8128-42010af00003",
                    "resourceVersion": "1034912"
                  }
                }
              ],
              "ports": [
                {
                  "port": 80,
                  "protocol": "TCP"
                }
              ]
            }
          ]
        }
      }
    """
    SERVICES_ENDPOINT = constants.K8S_API_ENDPOINT_V1 + '/endpoints'
    WATCH_ENDPOINT = SERVICES_ENDPOINT + '?watch=true'

    @asyncio.coroutine
    def translate(self, decoded_json):
        """Translates a K8s endpoints into a Neutron Pool Member.

        The endpoints translation can be assumed to be done after the service
        translation, which creates the Neutron Pool and the VIP.

        :param decoded_json: An endpoint event to be translated.
        """
        @asyncio.coroutine
        def get_subnets(subnet_name):
            cluster_subnet_response = yield from self.delegate(
                self.neutron.list_subnets, name=cluster_subnet_name)
            cluster_subnets = cluster_subnet_response['subnets']
            return cluster_subnets

        @asyncio.coroutine
        def get_pool(service_endpoint):
            """Gets the serialized pool information associated with a service.

            :param service_endpoint: The URI of the service associated with the
                                     pool to be retrieved.
            :returns: The deserialized JSON object of the pool associated with
                      the service which URI is given as ``service_endpoint``.
                      If it doesn't exist, the empty dictionary will be
                      returned.
            """
            service_response = yield from aio.methods.get(
                endpoint=service_endpoint, loop=self._event_loop)
            status, _, _ = yield from service_response.read_headers()
            assert status == 200
            service_response_body = yield from service_response.read_all()
            service = utils.utf8_json_decoder(service_response_body)
            service_metadata = service.get('metadata', {})
            service_annotations = service_metadata.get('annotations', {})
            serialized_pool = service_annotations.get(
                constants.K8S_ANNOTATION_POOL_KEY, '{}')
            pool = jsonutils.loads(serialized_pool)

            return pool

        LOG.debug('Endpoints notification %s', decoded_json)
        event_type = decoded_json.get('type', '')
        content = decoded_json.get('object', {})
        metadata = content.get('metadata', {})

        # FIXME(tfukushima): Ignore DELETED events for now.
        if event_type == DELETED_EVENT:
            LOG.info(_LI('Ignoring DELETED events. Pool members are deleted '
                         'when the service and corresponding pool are '
                         'deleted.'))
            return

        namespace = metadata.get('namespace',
                                 constants.K8S_DEFAULT_NAMESPACE)
        service_name = metadata['name']
        # FIXME(tfukushima): Ignore kubernetes service for now.
        if service_name == 'kubernetes':
            LOG.info(_LI('Ignoring "kubernetes" service since it is not '
                         'supported yet'))
            return
        service_endpoint = utils.get_service_endpoint(namespace, service_name)


        cluster_subnet_name = utils.get_subnet_name(namespace)
        cluster_subnets = yield from get_subnets(cluster_subnet_name)
        # Wait until the namespace translation is done.
        while not cluster_subnets:
            self.namespace_added.wait()
            cluster_subnets = yield from get_subnets(
                cluster_subnet_name)
        cluster_subnet = cluster_subnets[0]
        subnet_id = cluster_subnet['id']


        LOG.debug('About to update pool')
        with (yield from self.service_added):
            LOG.debug('Attempting to get pool')
            pool = yield from get_pool(service_endpoint)
            LOG.debug('Pool %s', pool)
            # Wait until the service translation is done.
            while not pool:
                # Wait until the service translation is finished.
                yield from self.service_added.wait()
                pool = yield from get_pool(service_endpoint)
            pool_id = pool['id']
            LOG.debug('Pool id %s', pool_id)

            if event_type in (ADDED_EVENT, MODIFIED_EVENT):
                endpoint_members = _get_endpoint_members(content.get('subsets',
                                                                     ()))
                LOG.debug('Endpoints members %s', endpoint_members)

                members_response = yield from self.sequential_delegate(
                    self.neutron.list_lbaas_members, lbaas_pool=pool_id)
                LOG.debug('Members response %s', members_response)
                pool_members = _get_pool_members(
                    members_response.get('members', ()))
                LOG.debug('Pool members %s', pool_members)

                for member in (endpoint_members - pool_members):
                    try:
                        yield from _add_pool_member(self.sequential_delegate,
                                                    self.neutron,
                                                    service_name,
                                                    pool_id,
                                                    subnet_id,
                                                    member.address,
                                                    member.protocol_port)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE('Error happened creating a Neutron '
                                          'loadbalancer pool member: %s'),
                                      ex)
                for member in (pool_members - endpoint_members):
                    try:
                        yield from _del_pool_member(self.sequential_delegate,
                                                    self.neutron,
                                                    pool_id,
                                                    member.uuid)
                    except n_exceptions.NeutronClientException as ex:
                        with excutils.save_and_reraise_exception():
                            LOG.error(_LE('Error happened deleting a Neutron '
                                          'loadbalancer pool member: %s'),
                                      ex)
