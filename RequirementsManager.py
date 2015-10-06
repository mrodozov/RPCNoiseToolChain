__author__ = 'rodozov'

import os,subprocess,os.path,json

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

class ProcessDescriptor: #TODO - SSHTUnnelDescriptor could inherit from process, since
    def __init__(self,name=None,pname=None,powner=None):
        self.name = name
        self.powner = powner
        self.pname = pname

class EnvHandler:

    listOfTunnels = []
    listOfProcesses = []
    listOfEnvVars = []
    debug = False

    #TODO - make the external classes internal

    #http://stackoverflow.com/questions/3764291/checking-network-connection
    #http://stackoverflow.com/questions/4689984/implementing-a-callback-in-python-passing-a-callable-reference-to-the-current

    def __del__(self):
        if self.debug:
            print 'dunno , print ?'

    def __init__(self,fileWithTunnels,fileWithProcesses,fileWithEnvVars,debug=False):
        self.listOfTunnels = []
        self.listOfProcesses = []
        self.listOfEnvVars = []
        self.debug = debug
        self.initConfigs(fileWithTunnels)
        self.checkListOfTunnels()
        self.initProcesses(fileWithProcesses)
        self.checkListOfProcesses()
        self.initEnvVars(fileWithEnvVars)


        if self.debug:
            print 'tunnels file', fileWithTunnels
            print 'env vars file', fileWithEnvVars
            print 'debug in  EnvHandler __init__' #not set yet

    def start(self, tunnel):
        '''
        starts new ssh tunnel
        :param tunnel: SSHDescriptor object
        :return: return success true or false
        '''
        tunnelStr = tunnel.tunnelString()
        hasStarted = False
        try:
            tunopen = subprocess.Popen("ssh -f -N -L "+tunnelStr,shell = True,close_fds=True)
            out, err = tunopen.communicate()
            if err is None:
                hasStarted = True
                tunnel.isRunning = True
        except ValueError:
            print 'tunnel', tunnel.tunnelName, 'failed to start !'

        return hasStarted

    def checkTunnel(self, sshTunnel):
        '''
        checks whether a tunnel is started or not. only checks, doesnt start it
        :param sshTunnel: SSHDescriptor
        :return: return true if the process exists, false if doesn't
        '''

        tstring = sshTunnel.tunnelString()
        p = subprocess.Popen("ps -ef | grep ssh | grep '"+tstring+"' | grep -v grep", shell = True, stdout = subprocess.PIPE)
        out, err = p.communicate()
        isRunning = False
        if not out:
            isRunning = False
            if self.debug:
                print 'output for', sshTunnel.tunnelName, 'is', out
        else:
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
                self.start(tunnel) # do not execute when in dev mode, it would start ssh tunnels
                if self.debug:
                    print 'starting tunnel', tunnel.tunnelName
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
                #not valid ? why # because you are dumb f**k and check your last commit to see that there was no lHost rHost members, moron
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

    def initProcesses(self,listOfProcesses):
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

    def checkProcess(self,process):
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

    '''
    List of methods to be finished
    when there is any point of doing this with python
    '''

    def initEnvVars(self,listVars):

        if not os.path.isfile(listVars) or os.path.getsize(listVars) == 0:
            return 'vars config file not found or empty'
        with open(listVars) as data_file:
            data = json.load(data_file)
            for k,v in data.items():
                pair = {}
                pair[k] = v
                self.listOfEnvVars.append(pair)

    def setListOfEnvVars(self):
        varDict = {}
        for k,v in self.listOfEnvVars.items():
            os.environ[k] = v
            #check if the var has to be set, set it if it has to, and the assign the
            varDict[k] = os.environ[k]
        return varDict

    def executeListOfShellCommands(self):
        dictOfExitStatuses = {} # command name - exit status dictionary
        #lol
        return dictOfExitStatuses
