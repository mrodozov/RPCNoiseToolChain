# main function

from CommandClasses import *
from RunlistManager import RunlistManager
from RunProcessManager import RunProcessPool, processSingleRunChain
from RequirementsManager import EnvHandler
from ReportService import ReportHandler
import Queue
from threading import Thread
import datetime
import paramiko
import os.path
import json
from threading import Event
import socks
from RRService import RRService
import copy

if __name__ == "__main__":

    #ssh -f -N -D 1080 mrodozov@lxplus.cern.ch # to open proxy
    #os.environ['LD_LIBRARY_PATH'] = '/home/rodozov/Programs/ROOT/INSTALL/lib/root'  # the only hardcoded variable remaining, probably. let's change it in the next commit
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, 'localhost', 1080) # don't use when inside CERN's network

    passwd = ''
    #lp_ssh_cl.connect('localhost', 22, 'mrodozov', passwd)

    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    #some setup

    optionsObject['webserver_remote']['ssh_credentials']['password']= passwd
    optionsObject['lxplus_archive_remote']['ssh_credentials']['password']= passwd
    remote_destinations = {'webserver': optionsObject['webserver_remote'], 'lxplus': optionsObject['lxplus_archive_remote']}
    remote_lxplus_backup = optionsObject['lxplus_archive_remote']['destination_root']
    remote_webserver = optionsObject['webserver_remote']['destination_root']
    os.environ['LD_LIBRARY_PATH'] = optionsObject['paths']['rodozov_local']['root_path']

    #print os.environ['LD_LIBRARY_PATH']

    ssh_t_s = SSHTransportService(remote_destinations)
    db_obj = DBService('oracle://','localhost','1521','rodozov','tralala','','RPC')

    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    suspendRRcheck = Event()
    stop = Event()

    # single object, used by the Run process manager so that it's not created for each run chain
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')

    rlistMngr = RunlistManager('resources/runlist.json')
    rpMngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'results/'})
    #environ_handler = EnvHandler('resources/ListOfTunnels.json', 'resources/process.json', 'resources/variables.json')
    reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json')
    #print 'env ok ?'

    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.suspendRRcheck = suspendRRcheck
    rlistMngr.rr_connector = RRService(use_proxy=True)
    #print 'rr service again ?'
    rpMngr.runChainProcessFunction = processSingleRunChain
    rpMngr.stop_process_event = stop
    reportsMngr.stop_signal = stop
    rlistMngr.stop_event = stop

    # enough setup, run the managers

    #paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)
    Sonic = reportsMngr
    ForestGump = rpMngr
    SpeedyGonzales = rlistMngr
    # lol, run the runners, should run foreVA

    rlistMngr.ssh_service = ssh_t_s.connections_dict['webserver']['ssh_client']
    rlistMngr.runlist_remote_dir = remote_webserver

    #print 'starting'
    suspendRRcheck.set()
    print 'starting managers ...'

    SpeedyGonzales.start()
    ForestGump.runForestRun()
    Sonic.runSonicRun()

