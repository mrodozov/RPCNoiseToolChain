# main function

from CommandClasses import *
from RunlistManager import RunlistManager
from RunProcessManager import RunProcessPool, processSingleRunChain
from RequirementsManager import EnvHandler
from ReportService import ReportHandler
import multiprocessing as mp
import Queue

# TODO - finish DBservice and ssh transport singletons. Check DBService with sqlite locally

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

