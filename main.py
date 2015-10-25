# main function

from CommandClasses import *
from RunlistManager import RunlistManager
from RunProcessManager import RunProcessPool, processSingleRunChain
from RequirementsManager import EnvHandler
from ReportService import ReportHandler
import multiprocessing as mp
import Queue
from threading import Thread
import datetime
import paramiko
import os.path
import json

def startCommand(cmmnd):
    cmmnd.processTask()

if __name__ == "__main__":

    os.environ['LD_LIBRARY_PATH'] = '/home/rodozov/Programs/ROOT/INSTALL/lib/root'  # the only hardcoded variable remaining, probably. let's change it in the next commit
    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    stop = mp.Event()

    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt') # single object, used by the Run process manager so that it's not created for each run chain

    rlistMngr = RunlistManager('resources/runlist.json')
    rpMngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'results/'})
    environ_handler = EnvHandler('resources/ListOfTunnels.json', 'resources/process.json', 'resources/variables.json')
    reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json')

    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rpMngr.runChainProcessFunction = processSingleRunChain
    rpMngr.stop_process_event = stop
    reportsMngr.stop_signal = stop
    rlistMngr.stop_event = stop
    #rlistMngr.putRunsOnProcessQueue()

    # enough setup, run the managers

    Sonic = reportsMngr
    ForestGump = rpMngr
    SpeedyGonzales = rlistMngr

    # lol, run the runners, should run foreVA

    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    passwd = 'BAKsho__4321'
    optionsObject['webserver_remote']['ssh_credentials']['password']= passwd
    optionsObject['lxplus_archive_remote']['ssh_credentials']['password']= passwd

    remote_destinations = {'webserver_remote': optionsObject['webserver_remote'], 'lxplus_archive_remote': optionsObject['lxplus_archive_remote']}
    remote_lxplus_backup = optionsObject['lxplus_archive_remote']['destination_root']
    remote_webserver = optionsObject['webserver_remote']['destination_root']

    #db_obj = DBService('oracle://','localhost','1521','rodozov','tralala','','RPC')
    ssh_t_s = SSHTransportService(remote_destinations)
    ssh_c, sftp_c = ssh_t_s.get_clients_for_connection('webserver_remote')

    rlistMngr.runlist_sftp_client = ssh_c.open_sftp()
    try:
        rlistMngr.runlist_sftp_client.chdir(remote_webserver)
    except IOError, e:
        print e.message

    remote_r = rlistMngr.runlist_sftp_client.getcwd()
    #print remote_r
    try:
        rlistMngr.runlist_sftp_client.put('resources/runlist.json',remote_r+'/'+'runlist.json')
    except IOError,e:
        print e.message
    #print rlistMngr.runlist_sftp_client.listdir(remote_r)
    content  =  rlistMngr.runlist_sftp_client.file(remote_r+'/runlist.json').read()
    dict = json.loads(content)
    print dict
    list_to_resub = [r for r in dict.keys() if dict[r]['status'] == 'submitted']

    #print list_to_resub
    #print rlistMngr.runlist_file
    #rlistMngr.updateRunlistFile()
    
    rlistMngr.synchronizeRemoteRunlistFile()

    #content = rl.read()
    #print content



    #stop.set()
    #ForestGump.runForestRun()
    #SpeedyGonzales.start()
    #Sonic.runSonicRun()










