# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import netaddr
from oslo_concurrency import processutils
import pyroute2

from kuryr.common import config

KIND_VETH = 'veth'
DOWN = 'DOWN'
CONTAINER_VETH_POSTFIX = '_c'
FIXED_IP_KEY = 'fixed_ips'
IP_ADDRESS_KEY = 'ip_address'
MAC_ADDRESS_KEY = 'mac_address'
SUBNET_ID_KEY = 'subnet_id'
VETH_POSTFIX = '-veth'
IFF_UP = 0x1  # The last bit represents if the interface is up


def _is_up(interface):
    flags = interface['flags']
    if not flags:
        return False
    return (flags & IFF_UP) == 1


def port_bind(endpoint_id, neutron_port, neutron_subnets):
    """Binds the Neutorn port to the network interface on the host.

    :param endpoint_id: the ID of the Docker container as string
    :param neutron_port: a port dictionary returned from python-neutronclient
    :param neutron_subnets: a list of all subnets potentially related with the
                            neutron_port under the same network
    :returns: the tuple of the names of the veth pair and the tuple of stdout
              and stderr returned by processutils.execute invoked with the
              executable script for binding
    :raises: pyroute2.ipdb.common.CreateException,
             pyroute2.ipdb.common.CommitException,
             processutils.ProcessExecutionError
    """
    ifname = endpoint_id[:8] + VETH_POSTFIX
    peer_name = ifname + CONTAINER_VETH_POSTFIX
    subnets_dict = {subnet['id']: subnet for subnet in neutron_subnets}

    ip = pyroute2.IPDB()
    with ip.create(ifname=ifname, kind=KIND_VETH,
                   reuse=True, peer=peer_name) as host_veth:
        if not _is_up(host_veth):
            host_veth.up()
    with ip.interfaces[peer_name] as peer_veth:
        fixed_ips = neutron_port.get(FIXED_IP_KEY, [])
        if not fixed_ips and (IP_ADDRESS_KEY in neutron_port):
            peer_veth.add_ip(neutron_port[IP_ADDRESS_KEY])
        for fixed_ip in fixed_ips:
            if IP_ADDRESS_KEY in fixed_ip and (SUBNET_ID_KEY in fixed_ip):
                subnet_id = fixed_ip[SUBNET_ID_KEY]
                subnet = subnets_dict[subnet_id]
                cidr = netaddr.IPNetwork(subnet['cidr'])
                peer_veth.add_ip(fixed_ip[IP_ADDRESS_KEY], cidr.prefixlen)
        peer_veth.address = neutron_port[MAC_ADDRESS_KEY].lower()
        if not _is_up(peer_veth):
            peer_veth.up()
    ip.release()

    midonet_exec_path = config.CONF.binding.binding_executable_path
    port_id = neutron_port['id']
    stdout, stderr = processutils.execute('sudo', 'bash', midonet_exec_path,
                         port_id, ifname)

    return (ifname, peer_name, (stdout, stderr))
