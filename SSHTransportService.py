__author__ = 'rodozov'

from Singleton import Singleton
import paramiko
import os
import multiprocessing as mp
import json
import Queue
import getpass

class SSHTransportService(object):

    __metaclass__ = Singleton

    def __init__(self, connections_description = None):
        self.connections_dict = {}
        self.open_connections(connections_description)
        self.lockWhenUpload = mp.Lock()
        self.queue = Queue.Queue()
        paramiko.util.log_to_file("paramiko_log.log")

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

    def get_transport_for_connection(self, name):
        transport = None
        ssh_client = None
        if name in self.connections_dict:
            conn_params = self.connections_dict[name]
            ssh_client = conn_params['ssh_client']
            #sftp_client = conn_params['sftp_client']
            if not ssh_client.get_transport().is_active():
                try:
                    #ssh_client.get_transport.get_channel
                    ssh_client.connect(conn_params['rhost'], conn_params['port'], conn_params['user'], conn_params['pass'])
                    transport = ssh_client.get_transport()
                except Exception, e:
                    print e.message
        try:
            transport = ssh_client.get_transport()
        except Exception, e:
            print e.message

        return transport

    def create_dir_on_remotehost(self, base_dir, dirname):
        try:
            self.sftp_client.chdir(base_dir)
            if not dirname in self.sftp_client.listdir():
                self.sftp_client.mkdir(dirname)
        except IOError, err:
            print err.message



    def uploadFilesOnRemote(self, files, resultsdir, subdir, remotename):
        #not needed by command objects anymore but leave it just in case
        warnings = []
        logs = {}
        results = {}
        success = True
        # print 'enter command '

        #with self.lockWhenUpload:

        print resultsdir, ' ', subdir , ' ', remotename

        remote_root = self.connections_dict[remotename]['destination_root']
        #print self.connections_dict[remotename]['ssh_client']
        t = self.connections_dict[remotename]['ssh_client'].get_transport()
        ssh_transport = paramiko.SFTPClient.from_transport(t)
        print ssh_transport, ssh_transport.get_channel()

        try:
            ssh_transport.chdir(remote_root)
            if not subdir in ssh_transport.listdir(remote_root):
                ssh_transport.mkdir(subdir)
        except IOError, err:
            logs['errors'] = err.message
            print err.message
            success = False


        for f in files:
            try:
                ssh_transport.put( os.path.join(resultsdir, f), os.path.join(remote_root, subdir, f))
                results['files'][f] = True
            except IOError, err:
                errout = "I/O error({0}): {1}".format(err.errno, err.strerror)
                warnings.append('file transfer failed for ' + f + ' with ' + errout)
                results['files'][f] = 'Failed'
                print err.message

        ssh_transport.get_channel().close()

        print 'subdir is ', subdir
        return success, warnings, logs, results



if __name__ == "__main__":

    with open('resources/options_object_rodozov.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    p = getpass.getpass('lxplus pass: ')

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

    files = wserver.listdir(connections_dict['webserver']['destination_root'])
    print files
