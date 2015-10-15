__author__ = 'rodozov'

from Singleton import Singleton
import paramiko
import os
import json

class SSHTransportService(object):

    __metaclass__ = Singleton

    def __init__(self, connections_description = None):
        self.connections_dict = {}
        self.open_connections(connections_description)

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
            sftp = paramiko.SFTPClient.from_transport(ssh_client.get_transport())
            self.connections_dict[name] = {'ssh_client':ssh_client,'sftp_client':sftp_client,'sftp':sftp,'destination_root':destination}
        except Exception, exc:
            print exc.message

    def close_connection(self, name):
        if name in self.connections_dict:
            for k in self.connections_dict[name].keys():
                self.connections_dict[name][k].close()

    def open_connections(self, connections_dict):
        for c in connections_dict.keys():
            self.open_connection(c, connections_dict[c])


