__author__ = 'rodozov'

from Singleton import Singleton
import paramiko
import os
from threading import RLock
import json

class SSHTransportService(object):

    __metaclass__ = Singleton

    def __init__(self, connections_description = None):
        self.connections_dict = {}
        self.open_connections(connections_description)
        #paramiko.util.log_to_file("filename.log")
        #paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)

    def open_connection(self, name, description):
        ssh_client = paramiko.SSHClient()
        ssh_client.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))

        try:
            transfer_username = description['ssh_credentials']['username']
            transfer_password = description['ssh_credentials']['password']
            transfer_port = int(description['ssh_credentials']['port'])
            remote_host = description['ssh_credentials']['rhost']
            destination = description['destination_root']
            ssh_client.connect(remote_host, transfer_port, username=transfer_username, password=transfer_password)
            sftp_client = ssh_client.open_sftp()
            #ssh_client.get_transport().set_keepalive(60) # keep it alive

            self.connections_dict[name] = {'ssh_client': ssh_client,'sftp_client': sftp_client,
                                           'destination_root': destination, 'user': transfer_username,
                                           'pass': transfer_password, 'port': transfer_port, 'rhost': remote_host}

        except Exception, exc:
            print exc.message
    def open_connections(self, connections_desc):
        for c in connections_desc:
            self.open_connection(c, connections_desc[c])

    def close_connection(self, name):
        if name in self.connections_dict: self.connections_dict[name]['ssh_client'].close()

    def close_connections(self):
        for conn in self.connections_dict.keys():
            self.connections_dict[conn]['ssh_client'].close()

if __name__ == "__main__":

    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    p = 'BAKsho__4321'

    connections_dict = {}
    connections_dict.update({'webserver':optionsObject['webserver_remote']})
    connections_dict.update({'lxplus':optionsObject['lxplus_archive_remote']})
    connections_dict['webserver']['ssh_credentials']['password'] = p
    connections_dict['lxplus']['ssh_credentials']['password'] = p

    ssh_transport = SSHTransportService(connections_dict)

    print ssh_transport.connections_dict['webserver']['ssh_client']
    print ssh_transport.connections_dict['lxplus']['ssh_client']

    wserver = ssh_transport.connections_dict['webserver']['ssh_client'].open_sftp()
    #print wserver.listdir(connections_dict['webserver']['destination_root'])
    if not 'run263757' in wserver.listdir(connections_dict['webserver']['destination_root']):
        wserver.chdir(connections_dict['webserver']['destination_root'])
        wserver.listdir()
        wserver.mkdir('run263757')
