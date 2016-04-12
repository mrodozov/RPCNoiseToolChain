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
import os
import json
from threading import Event
import socks
from RRService import RRService
import copy
import gc

if __name__ == "__main__":

    # gc.enable()
    # gc.set_debug(gc.DEBUG_LEAK)
    print os.getpid()

    #ssh -f -N -D 1080 mrodozov@lxplus.cern.ch # to open proxy
    #os.environ['LD_LIBRARY_PATH'] = '/home/rodozov/Programs/ROOT/INSTALL/lib/root'  # the only hardcoded variable remaining, probably. let's change it in the next commit
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, 'localhost', 1080) # don't use when inside CERN's network

    passwd = ''
    mailpass = ''
    dbpass = ''
    #empty_dict = {}
    #with open('resources/ErrorLog.log', 'w') as errfile: errfile.write(json.dumps(empty_dict, indent=1, sort_keys=True))
    with open('resources/mailpaswd') as mapssf: mailpass = mapssf.readline()
    with open('resources/passwd') as pfile: passwd = pfile.readline()
    with open('resources/dbpaswd') as dbpassf: dbpass = dbpassf.readline()

    print passwd
    #lp_ssh_cl.connect('localhost', 22, 'mrodozov', passwd)

    with open('resources/options_object.txt', 'r') as optobj: optionsObject = json.loads(optobj.read())

    #some setup

    optionsObject['webserver_remote']['ssh_credentials']['password']= passwd
    optionsObject['lxplus_archive_remote']['ssh_credentials']['password']= passwd
    remote_destinations = {'webserver': optionsObject['webserver_remote'], 'lxplus': optionsObject['lxplus_archive_remote']}
    remote_lxplus_backup = optionsObject['lxplus_archive_remote']['destination_root']
    remote_webserver = optionsObject['webserver_remote']['destination_root']
    os.environ['LD_LIBRARY_PATH'] = optionsObject['paths']['rodozov_local']['root_path']

    #print os.environ['LD_LIBRARY_PATH']

    ssh_t_s = SSHTransportService(remote_destinations)
    db_obj = DBService('oracle://','localhost','1521','rodozov', dbpass,'','RPC')
    #erase data from tables to clean it up for tests
    #db_obj.deleteDataFromTable('RPC_NOISE_ROLLS')
    #db_obj.deleteDataFromTable('RPC_NOISE_STRIPS')

    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    suspendRRcheck = Event()
    stop = Event()

    # single object, used by the Run process manager so that it's not created for each run chain
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')

    rlistMngr = RunlistManager('resources/runlist.json')
    rpMngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'results/'})
    environMngr = EnvHandler('resources/ListOfTunnels.json', 'resources/process.json')
    reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json', mailpass, 'resources/ErrorLog.log')

    print 'env ok ?'

    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.suspendRRcheck = suspendRRcheck
    rlistMngr.rr_connector = RRService(use_proxy=True)
    print 'rr service ok ?'
    rpMngr.runChainProcessFunction = processSingleRunChain
    environMngr.suspend = suspendRRcheck

    rpMngr.stop_process_event = stop
    reportsMngr.stop_signal = stop
    rlistMngr.stop_event = stop
    environMngr.stopSignal = stop
    # enough setup, run the managers

    #paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)
    Sonic = reportsMngr
    ForestGump = rpMngr
    SpeedyGonzales = rlistMngr
    # lol, run the runners, should run foreVA

    rlistMngr.ssh_service = ssh_t_s.connections_dict['webserver']['ssh_client']
    rlistMngr.runlist_remote_dir = remote_webserver
    reportsMngr.remote_dir = remote_webserver
    reportsMngr.ssh_client = ssh_t_s.connections_dict['webserver']['ssh_client']

    #print 'starting'
    suspendRRcheck.set()
    print 'starting managers ...'

    environMngr.start()
    SpeedyGonzales.start()
    ForestGump.runForestRun()
    Sonic.runSonicRun()


    '''
    dbpass = ''

    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())
    with open('resources/dbpaswd') as dbpassf:
        dbpass = dbpassf.readline()

    DBService(dbType='oracle+cx_oracle://',host= 'localhost',port= '1521',user= 'rodozov',password= dbpass,schema= '',dbName= 'XE')

    db_obj = DBService()

    print db_obj
    db_obj2 = DBService()
    print db_obj2

    #db_obj.createDBRolls()
    #db_obj.createDBStrips()
    #db_obj.deleteDataFromTable('RPC_NOISE_ROLLS')
    #db_obj.deleteDataFromTable('RPC_NOISE_STRIPS') #blocks for unknown reason


    dbup = DBDataUpload(args=optionsObject['dbdataupload'])
    dbup.options['filescheck'] = ['results/run263755/database_new.txt', 'results/run263755/database_full.txt']
    dbup.options['run'] = '263755'
    dbuptwo = DBDataUpload(args=optionsObject['dbdataupload'])
    dbuptwo.options['filescheck'] = ['results/run263757/database_new.txt', 'results/run263757/database_full.txt']
    dbuptwo.options['run'] = '263757'

    # print dbup.args
    # print dbup.options
    #dbup.processTask()
    #dbuptwo.processTask()

    dbthread_one = Thread(target=dbup.processTask)
    dbthread_two = Thread(target=dbuptwo.processTask)

    dbthread_one.start()
    dbthread_two.start()
    '''