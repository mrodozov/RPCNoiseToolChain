# main function

from CommandClasses import *
from RunlistManager import RunlistManager
from RunProcessManager import RunProcessPool, processSingleRunChain
from RequirementsManager import EnvHandler
from ReportService import ReportHandler
import Queue
import os
import json
from threading import Event
import socks
from RRService import RRService
import getpass

if __name__ == "__main__":
    

    
    pid = os.getpid()
    print pid
    #socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)
    #with open('resources/process_num.txt', 'w') as proc_file: proc_file.write(pid)

    #passwd = getpass.getpass('ssh pass: ')
    #mailpass = getpass.getpass('mail pass: ')
    #dbpass = getpass.getpass('db pass: ')
    #cleanErr = getpass.getpass('clean errorLog: 0 or 1 ')
    #delRuns = getpass.getpass('delete runs ? 0/1 ')
    #runsToDelete = ['263689','263744','263729','263685','263755','263743','263745','263728','263718','263752','263757']

    cleanErr = '0'
    
    if cleanErr == '1':
        with open('resources/ErrorLog.log', 'w') as errfile: errfile.write(json.dumps({}, indent=1, sort_keys=True))
        print 'err log cleaned'
    
    with open('resources/options_object.txt', 'r') as optobj: optionsObject = json.loads(optobj.read())

    #some setup

    optionsObject['webserver_remote']['ssh_credentials']['password']= passwd
    optionsObject['lxplus_archive_remote']['ssh_credentials']['password']= passwd
    remote_destinations = {'webserver': optionsObject['webserver_remote'], 'lxplus': optionsObject['lxplus_archive_remote']}
    remote_lxplus_backup = optionsObject['lxplus_archive_remote']['destination_root']
    remote_webserver = optionsObject['webserver_remote']['destination_root']
    os.environ['LD_LIBRARY_PATH'] = optionsObject['paths']['cms_online_nt_machine']['root_path']
    
    #print os.environ['LD_LIBRARY_PATH']
    
    ssh_t_s = SSHTransportService(remote_destinations)
    db_obj = DBService(dbType='oracle://',host= '',port= '',user= 'CMS_RPC_COND_W',password= dbpass,schema= 'CMS_RPC_COND',dbName= 'cms_omds_lb')
    
    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    suspendRRcheck = Event()
    stop = Event()

    # single object, used by the Run process manager so that it's not created for each run chain
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')

    rlistMngr = RunlistManager('resources/runlist.json')

   # if delRuns == '1':
   #     for r in runsToDelete:
   #         if r in rlistMngr.runlist:
   #             del rlistMngr.runlist[r]
   #         rlistMngr.updateRunlistFile()
    
    rpMngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'/rpctdata/CAF/'})
    environMngr = EnvHandler('resources/ListOfTunnels.json', 'resources/process.json')
    reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json', mailpass, 'resources/ErrorLog.log')
    environMngr.processPool = rpMngr.pool
    
    print 'env ok ?'

    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.suspendRRcheck = suspendRRcheck
    rlistMngr.rr_connector = RRService('http://localhost:22223/runregistry/')
    
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

    
    rlistMngr.ssh_service = ssh_t_s
    rlistMngr.ssh_conn_name = 'webserver'
    rlistMngr.runlist_remote_dir = remote_webserver
    reportsMngr.remote_dir = remote_webserver
    reportsMngr.ssh_client = ssh_t_s
    reportsMngr.ssh_conn_name = 'webserver'
    
    #print 'starting'
    suspendRRcheck.set()
    print 'starting managers ...'
    
    environMngr.start()
    SpeedyGonzales.start()
    ForestGump.runForestRun()
    Sonic.runSonicRun()
    
    
    
    '''

    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())
    #with open('resources/dbpaswd') as dbpassf:                                                                                                                                                                                              
    #    dbpass = dbpassf.readline()                                                                                                                                                                                                         

    db_obj = DBService(dbType='oracle://',host= '',port= '',user= 'CMS_RPC_COND_W',password= '8B1M410RM1N0RC3SS4T',schema= 'CMS_RPC_COND',dbName= 'cms_omds_lb')

    #print db_obj                                                                                                                                                                                                                            
    #db_obj2 = DBService()                                                                                                                                                                                                                   
    #print db_obj2                                                                                                                                                                                                                           
    result = db_obj.selectFromDB(269136,'RPC_NOISE_ROLLS')
    print result
    for row in result:
        print 'raw id: ', row['raw_id'] # working, it's ok now                                                                                                                                                                               

    #db_obj.createDBRolls()                                                                                                                                                                                                                  
    #db_obj.createDBStrips()                                                                                                                                                                                                                 
    #db_obj.deleteDataFromTable('RPC_NOISE_ROLLS')                                                                                                                                                                                           
    #db_obj.deleteDataFromTable('RPC_NOISE_STRIPS') #blocks for unknown reason                                                                                                                                                               

    dbup = DBDataUpload(args=optionsObject['dbdataupload'])
    dbup.options['filescheck'] = ['/rpctdata/CAF/run269565/database_new.txt', '/rpctdata/CAF/run269565/database_full.txt']
    dbup.options['run'] = '269565'
    dbup.processTask()optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())
    #with open('resources/dbpaswd') as dbpassf:                                                                                                                                                                                              
    #    dbpass = dbpassf.readline()                                                                                                                                                                                                         
    dbup = DBDataUpload(args=optionsObject['dbdataupload'])
    dbup.options['filescheck'] = ['/rpctdata/CAF/run269565/database_new.txt', '/rpctdata/CAF/run269565/database_full.txt']
    dbup.options['run'] = '269565'
    dbup.processTask()
    
    '''
