# main function

from CommandClasses import *
from RunlistManager import RunlistManager
from RunProcessManager import RunProcessPool, processSingleRunChain
from RequirementsManager import EnvHandler
from ReportService import ReportHandler
import multiprocessing as mp
import Queue
from threading import Thread

# TODO - finish DBservice and ssh transport singletons. Check DBService with sqlite locally

def startCommand(cmmnd):
    cmmnd.processTask()

if __name__ == "__main__":
    '''
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
    rlistMngr.putRunsOnProcessQueue()

    # enough setup, run the managers

    Sonic = reportsMngr
    ForestGump = rpMngr
    SpeedyGonzales = rlistMngr

    # lol, run the runners, should run foreVA

    stop.set()
    ForestGump.runForestRun()
    SpeedyGonzales.start()
    Sonic.runSonicRun()
    '''


    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    db_obj = DBService('oracle://','localhost','1521','rodozov','tralala','','RPC')

    #db_obj = DBService()
    #print db_obj
    #db_obj.createDBRolls()
    #db_obj.createDBStrips()
    #connone = db_obj.getConnection()
    #conntwo = db_obj.getConnection()
    #print connone
    #print conntwo

    dbup = DBDataUpload(args=optionsObject['dbdataupload'])
    dbup.options['filescheck'] = ['results/run220796/database_new.txt', 'results/run220796/database_full.txt']
    dbuptwo = DBDataUpload(args=optionsObject['dbdataupload'])
    dbuptwo.options['filescheck'] = ['results/run251638/database_new.txt', 'results/run251638/database_full.txt']
    dbuptr = DBDataUpload(args=optionsObject['dbdataupload'])
    dbuptr.options['filescheck'] = ['results/run251643/database_new.txt', 'results/run251643/database_full.txt']
    dbupf = DBDataUpload(args=optionsObject['dbdataupload'])
    dbupf.options['filescheck'] = ['results/run251718/database_new.txt', 'results/run251718/database_full.txt']

    dbup.options['run'] = '220796'
    dbuptwo.options['run'] = '251638'
    dbuptr.options['run'] = '251643'
    dbupf.options['run'] = '251718'

    t_one = Thread(target=startCommand, args=(dbup,))
    t_two = Thread(target=startCommand, args=(dbuptwo,))
    t_three = Thread(target=startCommand, args=(dbuptr,))
    t_four = Thread(target=startCommand, args=(dbupf,))

    t_one.start()
    t_two.start()
    t_three.start()
    t_four.start()

    print 'lol'

    t_one.join()
    t_two.join()
    t_three.join()
    t_four.join()


    #dbup.processTask()
    #dbuptwo.processTask()

    #result = connone.execute("select * from testme")
    #for row in result:
    #    print row

    #selection = db_obj.selectFromDB(220796, 'RPC_NOISE_ROLLS')

    #for row in selection:
    #    print row

    #selection = db_obj.selectFromDB(220796, 'RPC_NOISE_STRIPS')

    #for row in selection:
    #    print row


    '''
    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())
    '''

