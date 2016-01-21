
from config_obj import *
import argparse

class ConfigShell():

    def __init__(self):
        self.parser_init()

    def env(self, *args, **kwargs):
        for arg in args:
            value = os.environ.get(arg, None)
            if value:
                return value
        return kwargs.get('default', '')

    def do_help(self, args):
        if args.obj_parser:
                args.obj_parser.print_help()
        else:
            self.parser.print_help()

    def parser_init(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--username', help = 'User name')
        parser.add_argument('--password', help = 'Password')
        parser.add_argument('--tenant', help = 'Tenant name')
        parser.add_argument('--region', help = 'Region name')
        parser.add_argument('--api-server', help = 'API server address')

        parser.add_argument('cmd', choices = ['add', 'show', 'delete', 'help'],
                metavar = '<command>', help = '[ add | show | delete | help ]')

        subparsers = parser.add_subparsers(metavar = '<object>')
        self.sub_cmd_dict = {}

        sub_parser = subparsers.add_parser('vdns', help = 'Virtual DNS')
        sub_parser.set_defaults(obj_class = ConfigVirtualDns,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of virtual DNS')
        sub_parser.add_argument('--domain-name', metavar = '<name>',
                help = 'The name of DNS domain')
        sub_parser.add_argument('--record-order',
                choices = ['fixed', 'random', 'round-robin'],
                default = 'random', metavar = '<order>',
                help = 'The order of DNS records ' \
                       '[ random | fixed | round-robin ]')
        sub_parser.add_argument('--next-dns', metavar = '<name>',
                help = 'The name of next virtual DNS service or ' \
                       'the IP address of DNS server reachable by fabric.')

        sub_parser = subparsers.add_parser('ipam', help = 'Network IPAM')
        sub_parser.set_defaults(obj_class = ConfigIpam,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of IPAM')
        sub_parser.add_argument('--dns-type',
                choices = ['none', 'default', 'tenant', 'virtual'],
                metavar = '<type>',
                help = 'The type of DNS service ' \
                       '[ none | default | virtual | tenant ]')
        sub_parser.add_argument('--virtual-dns', metavar = '<name>',
                help = 'The name of virtual DNS service')
        sub_parser.add_argument('--tenant-dns', metavar = '<address>',
                action = 'append',
                help = 'The address of tenant DNS')
        sub_parser.add_argument('--domain-name', metavar = '<name>',
                help = 'The name of DNS domain')
        sub_parser.add_argument('--ntp-server', metavar = '<address>',
                help = 'The address of NTP server')

        sub_parser = subparsers.add_parser('policy', help = 'Network Policy')
        sub_parser.set_defaults(obj_class = ConfigPolicy,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of policy')
        sub_parser.add_argument('--rule', action = 'append',
                metavar = '<arguments>',
                help = 'Policy rule ' \
                       'direction=[ "<>" | ">" ],' \
                       'protocol=[ any | tcp | udp | icmp ],' \
                       'src-net=[ <name> | any ],' \
                       'dst-net=[ <name> | any ],' \
                       'src-port=[ <start>:<end> | any ],' \
                       'dst-port=[ <start>:<end> | any ],' \
                       'action=[ pass | deny | drop | reject | alert | ' \
                               'log | service ],' \
                       'service=<name>,' \
                       'index=<index>')

        sub_parser = subparsers.add_parser('security-group',
                help = 'Security Group')
        sub_parser.set_defaults(obj_class = ConfigSecurityGroup,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of security group')
        sub_parser.add_argument('--rule', metavar = '<index>',
                help = 'Rule index')
        sub_parser.add_argument('--direction',
                choices = ['ingress', 'egress'],
                metavar = '<direction>',
                help = 'Direction [ ingress | egress ]')
        sub_parser.add_argument('--protocol',
                choices = ['any', 'tcp', 'udp', 'icmp'],
                metavar = '<protocol>',
                help = 'Protocol [ any | tcp | udp | icmp ]')
        sub_parser.add_argument('--address', action = 'append',
                metavar = '<prefix>/<length>', help = 'Remote IP address')
        sub_parser.add_argument('--port', action = 'append', type = str,
                metavar = '<start>:<end>', help = 'The range of remote port')

        sub_parser = subparsers.add_parser('network',
                help = 'Virtual Network')
        sub_parser.set_defaults(obj_class = ConfigNetwork,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of virtual network')
        sub_parser.add_argument('--ipam', metavar = '<name>',
                help = 'The name of IPAM')
        sub_parser.add_argument('--subnet', metavar = '<prefix>/<length>',
                help = 'Subnet prefix and length')
        sub_parser.add_argument('--gateway', metavar = '<address>',
                help = 'The gateway address of subnet')
        sub_parser.add_argument('--policy', metavar = '<name>',
                help = 'The name of network policy')
        sub_parser.add_argument('--route-target', metavar = '<AS>:<RT>',
                help = 'Route target')
        sub_parser.add_argument('--route-table', metavar = '<name>',
                help = 'The name of route table')
        sub_parser.add_argument('--l2', action = 'store_true',
                help = 'Layer 2 network, layer 2&3 by default')
        sub_parser.add_argument('--shared', action = 'store_true',
                help = 'Enable sharing with other tenants')
        sub_parser.add_argument('--external', action = 'store_true',
                help = 'Enable external access')

        sub_parser = subparsers.add_parser('floating-ip-pool',
                help = 'Floating IP Pool')
        sub_parser.set_defaults(obj_class = ConfigFloatingIpPool,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of floating IP pool')
        sub_parser.add_argument('--network', metavar = '<name>',
                help = 'The name of virtual network holding floating IP pool')
        #sub_parser.add_argument('--floating-ip', action = 'store_true',
        #        help = 'Floating IP')

        sub_parser = subparsers.add_parser('vm',
                help = 'Virtual Machine')
        sub_parser.set_defaults(obj_class = ConfigVirtualMachine,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of virtual machine')
        sub_parser.add_argument('--image', metavar = '<name>',
                help = 'The name of image')
        sub_parser.add_argument('--flavor', metavar = '<name>',
                help = 'The name of flavor')
        sub_parser.add_argument('--network', action = 'append',
                metavar = '<name>',
                help = 'The name of network')
        sub_parser.add_argument('--user-data', metavar = '<name>',
                help = 'Full file name containing user data')
        sub_parser.add_argument('--node', metavar = '<name>',
                help = 'The name of compute node')
        sub_parser.add_argument('--wait', action = 'store_true',
                help = 'Wait till VM is active')

        sub_parser = subparsers.add_parser('interface-route-table',
                help = 'Interface Route Table')
        sub_parser.set_defaults(obj_class = ConfigInterfaceRouteTable,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of interface route table')
        sub_parser.add_argument('--route', action = 'append',
                metavar = '<prefix>/<length>', help = 'Route')

        sub_parser = subparsers.add_parser('route-table',
                help = 'Network Route Table')
        sub_parser.set_defaults(obj_class = ConfigRouteTable,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of route table')
        sub_parser.add_argument('--route', action = 'append',
                metavar = '<prefix>/<length>:<next-hop>',
                help = 'The route and next-hop')

        sub_parser = subparsers.add_parser('vm-interface',
                help = 'Virtual Machine Interface')
        sub_parser.set_defaults(obj_class = ConfigVmInterface,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<VM>:<network>',
                help = 'The name of virtual machine interface')
        sub_parser.add_argument('--interface-route-table', metavar = '<name>',
                help = 'The name of interface route table')
        sub_parser.add_argument('--security-group', metavar = '<name>',
                help = 'The name of security group')
        sub_parser.add_argument('--address',
                metavar = '<address>',
                help = 'IP address')
        sub_parser.add_argument('--floating-ip',
                metavar = '<address>',
                help = 'Floating IP address [ any | <address> ]')
        sub_parser.add_argument('--floating-ip-pool',
                metavar = '<pool>',
                help = 'The floating IP pool to allocate a floating IP from ' \
                       '<tenant>:<network>:<floating IP pool>')

        sub_parser = subparsers.add_parser('image',
                help = 'Virtual Machine Image')
        self.sub_cmd_dict['image'] = sub_parser
        sub_parser.set_defaults(obj_class = ConfigImage)
        sub_parser.add_argument('name', nargs = '?', default = None)

        sub_parser = subparsers.add_parser('flavor',
                help = 'Virtual Machine Flavor')
        self.sub_cmd_dict['flavor'] = sub_parser
        sub_parser.set_defaults(obj_class = ConfigFlavor)
        sub_parser.add_argument('name', nargs = '?', default = None)

        sub_parser = subparsers.add_parser('service-template',
                help = 'Service Template')
        sub_parser.set_defaults(obj_class = ConfigServiceTemplate,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of service template')
        sub_parser.add_argument('--mode',
                choices = ['transparent', 'in-network', 'in-network-nat'],
                metavar = '<mode>',
                help = 'Service mode ' \
                       '[ transparent | in-network | in-network-nat ]')
        sub_parser.add_argument('--type',
                choices = ['firewall', 'analyzer'],
                metavar = '<type>',
                help = 'Service type [ firewall | analyzer ]')
        sub_parser.add_argument('--image', metavar = '<name>',
                help = 'The name of image')
        sub_parser.add_argument('--flavor', metavar = '<name>',
                help = 'The name of flavor')
        sub_parser.add_argument('--scale', action = 'store_true',
                help = 'Enable service scaling')
        sub_parser.add_argument('--interface',
                choices = ['management', 'left', 'right', 'other'],
                metavar = '<type>',
                action = 'append',
                help = 'Service interface ' \
                       '[ management | left | right | other ]')

        sub_parser = subparsers.add_parser('service-instance',
                help = 'Service Instance')
        sub_parser.set_defaults(obj_class = ConfigServiceInstance,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of service instance')
        sub_parser.add_argument('--template',
                metavar = '<template>',
                help = 'Service template')
        sub_parser.add_argument('--network', action = 'append',
                metavar = '<arguments>',
                help = 'network=[ <name> | auto ],tenant=<name>,' \
                       'route=<prefix>/<length> ' \
                       'The network order must be the same as interface ' \
                       'order defined in service template.')
        sub_parser.add_argument('--scale-max',
                metavar = '<number>',
                help = 'The maximum number of instances')
        sub_parser.add_argument('--auto-policy', action = 'store_true',
                help = 'Enable automatic policy')

        sub_parser = subparsers.add_parser('link-local',
                help = 'Link Local Service')
        sub_parser.set_defaults(obj_class = ConfigGlobalVrouter,
                obj_parser = sub_parser)
        sub_parser.add_argument('name', nargs = '?', default = None,
                metavar = '<name>', help = 'The name of link local service')
        sub_parser.add_argument('--link-local-address',
                metavar = '<address>',
                help = 'Link Local service address and port ' \
                       '<link local address>:<link local port>')
        sub_parser.add_argument('--fabric-address',
                metavar = '<address>',
                help = 'Fabric address and port ' \
                       '<fabric address>:<fabric port>')
        self.parser = parser

    def parse(self, argv = None):
        args = self.parser.parse_args(args = argv)
        return args

    def run(self, args, client):
        obj = args.obj_class(client = client)
        if args.cmd == 'help':
            self.do_help(args)
        elif args.cmd == 'show':
            obj.show(args.name)
        elif args.cmd == 'add':
            if (args.obj_class == ConfigVirtualDns):
                obj.add(args.name, args.record_order, args.next_dns)
            elif (args.obj_class == ConfigIpam):
                obj.add(args.name, args.dns_type, args.virtual_dns,
                        args.tenant_dns, args.domain_name, args.ntp_server)
            elif (args.obj_class == ConfigPolicy):
                obj.add(args.name, args.rule)
            elif (args.obj_class == ConfigSecurityGroup):
                obj.add(args.name, args.protocol, args.address, args.port,
                        args.direction)
            elif (args.obj_class == ConfigNetwork):
                obj.add(args.name, args.ipam, args.subnet, args.policy,
                        args.route_target, args.route_table, args.shared,
                        args.external, args.l2)
            elif (args.obj_class == ConfigFloatingIpPool):
                obj.add(args.name, args.network)
            elif (args.obj_class == ConfigServiceTemplate):
                obj.add(args.name, args.mode, args.type, args.image,
                        args.flavor, args.interface)
            elif (args.obj_class == ConfigServiceInstance):
                obj.add(args.name, args.template, args.network,
                        args.auto_policy, args.scale_max)
            elif (args.obj_class == ConfigVirtualMachine):
                obj.add(args.name, args.image, args.flavor, args.network,
                        args.node, args.user_data, args.wait)
            elif (args.obj_class == ConfigRouteTable):
                obj.add(args.name, args.route)
            elif (args.obj_class == ConfigInterfaceRouteTable):
                obj.add(args.name, args.route)
            elif (args.obj_class == ConfigVmInterface):
                obj.add(args.name, args.security_group,
                        args.interface_route_table, args.address,
                        args.floating_ip_pool, args.floating_ip)
            elif (args.obj_class == ConfigGlobalVrouter):
                obj.add(args.name, args.link_local_address,
                        args.fabric_address)
        elif args.cmd == 'delete':
            if (args.obj_class == ConfigVirtualDns):
                obj.delete(args.name)
            elif (args.obj_class == ConfigIpam):
                obj.delete(args.name, args.domain_name)
            elif (args.obj_class == ConfigPolicy):
                obj.delete(args.name, args.rule)
            elif (args.obj_class == ConfigSecurityGroup):
                obj.delete(args.name, args.rule)
            elif (args.obj_class == ConfigNetwork):
                obj.delete(args.name, args.ipam, args.policy,
                           args.route_target)
            elif (args.obj_class == ConfigFloatingIpPool):
                obj.delete(args.name, args.network)
            elif (args.obj_class == ConfigServiceTemplate):
                obj.delete(args.name)
            elif (args.obj_class == ConfigServiceInstance):
                obj.delete(args.name)
            elif (args.obj_class == ConfigVirtualMachine):
                obj.delete(args.name)
            elif (args.obj_class == ConfigRouteTable):
                obj.delete(args.name, args.route)
            elif (args.obj_class == ConfigInterfaceRouteTable):
                obj.delete(args.name, args.route)
            elif (args.obj_class == ConfigVmInterface):
                obj.delete(args.name, args.security_group,
                           args.interface_route_table, args.address,
                           args.floating_ip)
            elif (args.obj_class == ConfigGlobalVrouter):
                obj.delete(args.name)
        else:
            print 'Unknown action %s' %(args.cmd)
            return

    def main(self):
        args = self.parse()
        #print args
        #return
        client = ConfigClient(args.username, args.password, args.tenant,
                args.region, args.api_server)
        self.run(args, client)


if __name__ == '__main__':
    ConfigShell().main()

