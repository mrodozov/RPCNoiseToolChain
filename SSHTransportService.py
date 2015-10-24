__author__ = 'rodozov'

from Singleton import Singleton
import paramiko
import os
import json
from threading import  Lock

class SSHTransportService(object):

    __metaclass__ = Singleton

    def __init__(self, connections_description = None):
        self.connections_dict = {}
        self.open_connections(connections_description)
        self.lock = Lock()

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
            ssh_client.get_transport().set_keepalive(60) # keep it alive
            #sftp = paramiko.SFTPClient.from_transport(ssh_client.get_transport())

            self.connections_dict[name] = {'ssh_client': ssh_client, 'sftp_client': sftp_client,
                                           'destination_root': destination, 'user': transfer_username,
                                           'pass': transfer_password, 'port': transfer_port, 'rhost': remote_host}

        except Exception, exc:
            print exc.message

    def close_connection(self, name):
        if name in self.connections_dict:
            for k in self.connections_dict[name].keys():
                if k == 'ssh_client' or k == 'sftp_client' or k == 'sftp':
                    self.connections_dict[name][k].close()

    def open_connections(self, connections_desc):
        for c in connections_desc.keys():
            self.open_connection(c, connections_desc[c])

    def get_clients_for_connection(self, name):
        if name in self.connections_dict:
            conn_params = self.connections_dict[name]
            ssh_client = conn_params['ssh_client']
            sftp_client = conn_params['sftp_client']
            if not ssh_client.get_transport().is_active():
                try:
                    #ssh_client.get_transport.get_channel
                    ssh_client.connect(conn_params['rhost'], conn_params['port'], conn_params['user'], conn_params['pass'])
                    sftp_client = ssh_client.open_sftp()
                except Exception, e:
                    print e.message

            return ssh_client, sftp_client

if __name__ == "__main__":

    print 'boza'

