#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Routines for configuring Kuryr
"""

import multiprocessing
import os

from oslo_config import cfg
from oslo_log import log

from kuryr._i18n import _
from kuryr import version


core_opts = [
    cfg.StrOpt('pybasedir',
               default=os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                    '../../')),
               help=_('Directory where Kuryr python module is installed.')),
    cfg.StrOpt('bindir',
               default='$pybasedir/usr/libexec/kuryr',
               help=_('Directory for Kuryr vif binding executables.')),
    cfg.StrOpt('kuryr_uri',
               default='http://127.0.0.1:2377',
               help=_('Kuryr URL for accessing Kuryr through json rpc.')),
    cfg.StrOpt('log_level',
               default=os.environ.get('LOG_LEVEL', 'INFO'),
               choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
               help=_('The level of the logging.')),
    cfg.StrOpt('capability_scope',
               default=os.environ.get('CAPABILITY_SCOPE', 'local'),
               choices=['local', 'global'],
               help=_('Kuryr plugin scope reported to libnetwork.')),
    cfg.StrOpt('subnetpool_name_prefix',
               default='kuryrPool',
               help=_('Neutron subnetpool name will be prefixed by this.')),
    cfg.StrOpt('local_default_address_space',
               default='local_scope',
               help=_('The default neutron local address-scope name')),
    cfg.StrOpt('global_default_address_space',
               default='global_scope',
               help=_('The default neutron global address-scope name.')),
]
neutron_opts = [
    cfg.StrOpt('neutron_uri',
               default=os.environ.get('OS_URL', 'http://127.0.0.1:9696'),
               help=_('Neutron URL for accessing the network service.')),
    cfg.StrOpt('enable_dhcp',
               default='True',
               help=_('Enable or Disable dhcp for neutron subnets.')),
]
keystone_opts = [
    cfg.StrOpt('auth_uri',
               default=os.environ.get('IDENTITY_URL',
                                      'http://127.0.0.1:35357/v2.0'),
               help=_('The URL for accessing the identity service.')),
    cfg.StrOpt('admin_user',
               default=os.environ.get('SERVICE_USER'),
               help=_('The username to auth with the identity service.')),
    cfg.StrOpt('admin_tenant_name',
               default=os.environ.get('SERVICE_TENANT_NAME'),
               help=_('The tenant name to auth with the identity service.')),
    cfg.StrOpt('admin_password',
               default=os.environ.get('SERVICE_PASSWORD'),
               help=_('The password to auth with the identity service.')),
    cfg.StrOpt('admin_token',
               default=os.environ.get('SERVICE_TOKEN'),
               help=_('The admin token.')),
    cfg.StrOpt('auth_ca_cert',
               default=os.environ.get('SERVICE_CA_CERT'),
               help=_('The CA certification file.')),
    cfg.BoolOpt('auth_insecure',
                default=False,
                help=_("Turn off verification of the certificate for ssl")),
]
binding_opts = [
    cfg.StrOpt('veth_dst_prefix',
               default='eth',
               help=('The name prefix of the veth endpoint put inside the '
                     'container.'))
]

raven_opts = [
    cfg.StrOpt('logfile_path',
               default='/var/log/kuryr/raven.log'),
    cfg.IntOpt('max_workers',
               help=_('The maximum number of the workers used by the executor '
                      'of the event loop. The executor is thread-based and it '
                      'multiplexes the I/O operations and makes them '
                      'asynchronous. This option determs the concurrency '
                      'level and usually it is set the number of the '
                      'processors multiplied by five. See the documentation '
                      'of ``concurrent.futures.ThreadPoolExecutor`` for more '
                      'details.'),
               default=multiprocessing.cpu_count() * 5),
    cfg.IntOpt('max_retries', default=10,
               help=_('The maximum number of the retries for translations. '
                      'Translations can fail because they may depend on other'
                      'translations or similar timing issues. This value sets '
                      'how many times Raven retries.')),
    cfg.IntOpt('max_wait_interval', default=30,
               help=_('The maximum time in seconds for the wait interval of '
                      'the translations')),
    cfg.StrOpt('public_net_id', default='39029e1e-46f8-4330-8b0f-8d88db6a8a0e',
               help=_('The default network id to provide external access to cluster')),
    cfg.StrOpt('lb_method', default='ROUND_ROBIN',
               choices=['ROUND_ROBIN', 'LEAST_CONNECTIONS', 'SOURCE_IP'],
               help=_('The Neutron Load Balancer algorithm type. One of '
                      '"ROUND_ROBIN", "LEAST_CONNECTIONS", "SOURCE_IP".')),
]

k8s_opts = [
    cfg.StrOpt('api_root',
               default=os.environ.get('K8S_API', 'http://localhost:8080')),
    cfg.StrOpt('cluster_subnet_pool',
               default=os.environ.get(
                   'SUBNET_POOL', '192.168.0.0/16')),
    # NOTE(tfukushima): FLANNEL_NET is used in the deployment scripts.
    #   https://github.com/kubernetes/kubernetes/search?utf8=%E2%9C%93&q=flannel_net  # noqa
    cfg.StrOpt('cluster_vip_subnet',
               default=os.environ.get(
                   'FLANNEL_NET', '172.16.0.0/16')),
    # NOTE(tfukushima): SERVICE_CLUSTER_IP_RANGE is used in the deployment
    #   scripts.
    # https://github.com/kubernetes/kubernetes/search?utf8=%E2%9C%93&q=SERVICE_CLUSTER_IP_RANGE&type=Code  # noqa
    cfg.StrOpt('cluster_service_subnet',
               default=os.environ.get(
                   'SERVICE_CLUSTER_IP_RANGE', '10.3.0.0/24')),
]


CONF = cfg.CONF
CONF.register_opts(core_opts)
CONF.register_opts(neutron_opts, group='neutron_client')
CONF.register_opts(keystone_opts, group='keystone_client')
CONF.register_opts(binding_opts, 'binding')
CONF.register_opts(raven_opts, 'raven')
CONF.register_opts(k8s_opts, 'k8s')

# Setting oslo.log options for logging.
log.register_options(CONF)


def init(args, **kwargs):
    cfg.CONF(args=args, project='kuryr',
             version=version.version_info.release_string(), **kwargs)
