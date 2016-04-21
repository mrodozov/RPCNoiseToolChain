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
    
<<<<<<< HEAD

    
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
    
=======
    
    # gc.enable()
    # gc.set_debug(gc.DEBUG_LEAK)
    print os.getpid()

        
    passwd = ''
    mailpass = None
    dbpass = ''
    #empty_dict = {}
    #with open('resources/ErrorLog.log', 'w') as errfile: errfile.write(json.dumps(empty_dict, indent=1, sort_keys=True))
    #with open('resources/mailpaswd') as mapssf: mailpass = mapssf.readline()
    #with open('resources/passwd') as pfile: passwd = pfile.readline()
    #with open('resources/dbpaswd') as dbpassf: dbpass = dbpassf.readline()

    #print passwd
    #lp_ssh_cl.connect('localhost', 22, 'mrodozov', passwd)

>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
    with open('resources/options_object.txt', 'r') as optobj: optionsObject = json.loads(optobj.read())

    #some setup

    optionsObject['webserver_remote']['ssh_credentials']['password']= passwd
    optionsObject['lxplus_archive_remote']['ssh_credentials']['password']= passwd
    remote_destinations = {'webserver': optionsObject['webserver_remote'], 'lxplus': optionsObject['lxplus_archive_remote']}
    remote_lxplus_backup = optionsObject['lxplus_archive_remote']['destination_root']
    remote_webserver = optionsObject['webserver_remote']['destination_root']
    os.environ['LD_LIBRARY_PATH'] = optionsObject['paths']['cms_online_nt_machine']['root_path']
<<<<<<< HEAD
    
=======

>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
    #print os.environ['LD_LIBRARY_PATH']
    
    ssh_t_s = SSHTransportService(remote_destinations)
<<<<<<< HEAD
    db_obj = DBService(dbType='oracle://',host= '',port= '',user= 'CMS_RPC_COND_W',password= dbpass,schema= 'CMS_RPC_COND',dbName= 'cms_omds_lb')
=======
    db_obj = DBService(dbType='oracle://',host= '',port= '',user= 'CMS_RPC_COND_W',password= '',schema= 'CMS_RPC_COND',dbName= 'cms_omds_lb')
>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
    
    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    suspendRRcheck = Event()
    stop = Event()

    # single object, used by the Run process manager so that it's not created for each run chain
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')

    rlistMngr = RunlistManager('resources/runlist.json')
<<<<<<< HEAD

   # if delRuns == '1':
   #     for r in runsToDelete:
   #         if r in rlistMngr.runlist:
   #             del rlistMngr.runlist[r]
   #         rlistMngr.updateRunlistFile()
    
=======
>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
    rpMngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'/rpctdata/CAF/'})
    environMngr = EnvHandler('resources/ListOfTunnels.json', 'resources/process.json')
    reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json', mailpass, 'resources/ErrorLog.log')
    environMngr.processPool = rpMngr.pool
    
    print 'env ok ?'

    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.suspendRRcheck = suspendRRcheck
<<<<<<< HEAD
    rlistMngr.rr_connector = RRService('http://localhost:22223/runregistry/')
=======
    rlistMngr.rr_connector = RRService('http://localhost:22223/runregistry/',False,False)
>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
    
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
<<<<<<< HEAD

    
    rlistMngr.ssh_service = ssh_t_s
    rlistMngr.ssh_conn_name = 'webserver'
=======
    
    rlistMngr.ssh_service = ssh_t_s.connections_dict['webserver']['ssh_client']
>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
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
<<<<<<< HEAD
    
    
    
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
=======

    
>>>>>>> 9e05d9dfb78873664fa74df3a29aa6a69b75c65d
