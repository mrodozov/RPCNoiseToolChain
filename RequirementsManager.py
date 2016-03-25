__author__ = 'rodozov'

import os
import subprocess
import os.path
import json
from threading import Thread, Event
import psutil
import time
import datetime

class SSHTunnelDescriptor:
    def __init__(self,tName=None,options=None,localHost=None,localPort=None,remoteHost=None,remotePort=None,debug=False):
        self.localHost = localHost
        self.localPort = localPort
        self.remoteHost = remoteHost
        self.remotePort = remotePort
        self.tunnelName = tName
        self.options = options
        self.debug = debug

        self.isRunning = False

    def tunnelString(self):
        t =  str(self.localPort) + ':' + str(self.remoteHost) + ':' + str(self.remotePort) + ' ' + str(self.localHost)
        if self.debug:
            print t

        return t

    def __del__(self):
        if self.debug:
            print 'SSHDescriptorDestructor called for tunnel', self.tunnelName

class ProcessDescriptor:
    def __init__(self,name=None,pname=None,powner=None):
        self.name = name
        self.powner = powner
        self.pname = pname

class EnvHandler(Thread):

    #http://stackoverflow.com/questions/3764291/checking-network-connection

    def __del__(self):
        if self.debug:
            print 'dunno , print ?'

    def __init__(self, fileWithTunnels=None, fileWithProcesses=None, debug=False):
        super(EnvHandler, self).__init__()
        self.listOfTunnels = []
        self.listOfProcesses = []
        self.debug = debug
        self.suspend = None
        self.stopSignal = None
        self.initConfigs(fileWithTunnels)
        self.initProcesses(fileWithProcesses)
        #self.checkListOfTunnels()


        if self.debug:
            print 'tunnels file', fileWithTunnels

            print 'debug in  EnvHandler __init__' #not set yet

    def startTunnel(self, tunnel):
        '''
        starts new ssh tunnel
        :param tunnel: SSHDescriptor object
        :return: return success true or false
        '''
        tunnelStr = tunnel.tunnelString()
        hasStarted = False
        try:
            tunopen = subprocess.Popen("ssh -f -N -L "+tunnelStr, shell = True, close_fds=True)
            out, err = tunopen.communicate()
            if err is None:
                hasStarted = True
                tunnel.isRunning = True
        except Exception, e:
            print e.message
            print 'tunnel', tunnel.tunnelName, 'failed to start !'

        return hasStarted

    def checkTunnel(self, sshTunnel):

        tstring = sshTunnel.tunnelString()
        isRunning = False
        for p in psutil.process_iter():
            process = psutil.Process(p.pid)
            withargs =  ' '.join(process.cmdline())
            if withargs.find(tstring) is not -1:
                isRunning = True

        return isRunning

    def checkListOfTunnels(self):
        '''
        loops on the list of tunnels, described in the config, tries if the tunnels exists as processes and restart them if not
        :return: doesnt return anything #TODO - make it return dictionary of
        '''
        statusDict = {} #dictionary with name:status for each tunnel
        for tunnel in self.listOfTunnels:
            tunnelCheck = self.checkTunnel(tunnel)
            if not tunnelCheck:
                self.suspend.clear()
                if (self.startTunnel(tunnel)): self.suspend.set() # do not execute when in dev mode, it would start ssh tunnels
                if self.debug:
                    print 're/starting tunnel', tunnel.tunnelName
            else:
                if self.debug:
                    print 'tunnel ', tunnel.tunnelName , ' is running'
            statusDict[tunnel.tunnelName] = tunnelCheck

        return statusDict

    def initConfigs(self, listConfigs):
        '''
        :param listConfigs: file with ssh tunnel descriptions
        :return: return message in case of exception
        '''
        #file better be JSON with similar object attributes
        if not os.path.isfile(listConfigs):
            raise Exception("tunnels config file not found or empty")
        try:
            #check each line structure
            dictOfTunnel = []
            f = open(listConfigs,'r')
            for tunnelConfig in f:
                dic = json.loads(tunnelConfig)
                dictOfTunnel.append(dic)
                sshDescriptor = SSHTunnelDescriptor(
                dic['name'],
                dic['options'],
                dic['lhost'],
                dic['lport'],
                dic['rhost'],
                dic['rport'],
                self.debug
                )

                self.listOfTunnels.append(sshDescriptor)

        except Exception, e:
            return e.message

    def initProcesses(self, listOfProcesses):
        if not os.path.isfile(listOfProcesses):
            raise Exception("processes config file not found of empty")
        try:
            with open (listOfProcesses) as data_file:
                lines = data_file.read().splitlines()
                for line in lines:
                    jdata = json.loads(line)
                    newProcess = ProcessDescriptor (
                        jdata['name'],
                        jdata['pname'],
                        jdata['powner']
                    )
                    self.listOfProcesses.append(newProcess)
        except Exception, e:
            return e.message

    def checkProcess(self, process):
        '''
        :param process: process object
        :return: return process run status True or False
        '''
        retval = False
        processString = process.pname

        p = subprocess.Popen("ps -ef | grep '"+processString+"' | grep -v grep", shell = True, stdout = subprocess.PIPE)
        out, err = p.communicate()
        if out:
            retval = True
        if self.debug:
            print 'debug for process', process.pname
        return retval

    def checkListOfProcesses(self):
        statusDict = {}
        for process in self.listOfProcesses:
            status = self.checkProcess(process)
            statusDict[process.name] = status
            if self.debug:
                print 'process ', process.name, ' status is', status
        return statusDict

    def run(self):
        while True:
            self.checkListOfTunnels()
            print 'tunnels checked at ', datetime.datetime.now().replace(microsecond=0)
            time.sleep(100)
            if self.stopSignal.is_set():
                break



def checkSuspendEventVariable(susvar = None, stopvar = None):
    while True:

        while not susvar.is_set():
            print 'now its waiting ...'
            time.sleep(5)
        susvar.wait()
        time.sleep(30)
        print 'check suspend at ', datetime.datetime.now().replace(microsecond=0)
        if stopvar.is_set():
            break


if __name__ == "__main__":


    susp = Event()
    stop = Event()
    susp.set()
    checkSuspend = Thread(target=checkSuspendEventVariable, args=(susp, stop,))

    e_handler = EnvHandler('resources/ListOfTunnels.json','resources/process.json', True)
    e_handler.stopSignal = stop
    e_handler.suspend = susp
    e_handler.start()
    checkSuspend.start()
    e_handler.join()






    # TODO - to test the RR query suspend when closing the RR tunnel. just point port to :80 and run queries on the pointing port, kill the tunnel and see if it brings it back