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

from kuryr.common import config


SCHEMA = {
    "PLUGIN_ACTIVATE": {"Implements": ["NetworkDriver", "IpamDriver"]},
    # TODO(tfukushima): This is mocked and should be replaced with real data.
    "ENDPOINT_OPER_INFO": {"Value": {}},
    "SUCCESS": {}
}

DEVICE_OWNER = 'kuryr:container'

NEUTRON_ID_LH_OPTION = 'kuryr.net.uuid.lh'
NEUTRON_ID_UH_OPTION = 'kuryr.net.uuid.uh'
NET_NAME_PREFIX = 'kuryr-net-'

NETWORK_GENERIC_OPTIONS = 'com.docker.network.generic'
NEUTRON_UUID_OPTION = 'neutron.net.uuid'
NEUTRON_NAME_OPTION = 'neutron.net.name'
KURYR_EXISTING_NEUTRON_NET = 'kuryr.net.existing'

K8S_ANNOTATION_POOL_KEY = 'kuryr.org/neutron-pool'
K8S_ANNOTATION_PORT_KEY = 'kuryr.org/neutron-port'
K8S_ANNOTATION_SUBNET_KEY = 'kuryr.org/neutron-subnet'
K8S_ANNOTATION_NETWORK_KEY = 'kuryr.org/neutron-network'
K8S_ANNOTATION_VIP_KEY = 'kuryr.org/neutron-vip'
K8S_ANNOTATION_LISTENER_KEY = 'kuryr.org/neutron-listener'
K8S_ANNOTATION_FIP_KEY = 'kuryr.org/neutron-fip'
K8S_ANNOTATION_FIPSG_KEY = 'kuryr.org/neutron-fipsg'

K8S_API_ENDPOINT_BASE = config.CONF.k8s.api_root
K8S_API_ENDPOINT_V1 = K8S_API_ENDPOINT_BASE + '/api/v1'

K8S_DEFAULT_NAMESPACE = 'default'

# REVISIT(yamamoto): Which of label or annotation is more suitable for
# this purpose?  Do we want to allow users to specify multiple SGs?
K8S_LABEL_SECURITY_GROUP_KEY = 'kuryr.org/neutron-security-group'

K8S_HARDCODED_SG_NAME = 'raven-default-sg'

MAX_RETRIES = config.CONF.raven.max_retries
MAX_WAIT_INTERVAL = config.CONF.raven.max_wait_interval
BACKOFF_UNIT = 0.2
