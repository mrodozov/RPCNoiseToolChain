# main function

from CommandClasses import *
from Chain import Chain
from Event import SimpleEvent
from RunlistManager import RunlistManager
from RunProcessManager import RunProcessPool, processSingleRunChain
from RequirementsManager import SSHTunnelDescriptor, ProcessDescriptor, EnvHandler
from ReportService import ReportHandler, LogHandler, MailService
import multiprocessing as mp
import Queue
import time


if __name__ == "__main__":

    os.environ['LD_LIBRARY_PATH'] = '/home/rodozov/Programs/ROOT/INSTALL/lib/root'  # important
    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    stop = mp.Event()

    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')
    rlistMngr = RunlistManager('resources/runlist.json')
    rlistMngr.toProcessQueue = runsToProcessQueue
    rpMngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'results/'})
    rpMngr.runChainProcessFunction = processSingleRunChain
    rpMngr.stop_process_event = stop
    #environ_handler = EnvHandler('resources/ListOfTunnels.json', 'resources/process.json', 'resources/variables.json')
    reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json')
    reportsMngr.stop_signal = stop
    # enough setup, run some :)

    arun = rlistMngr.runlist['220796']
    print arun
    arun = {'220796':arun}
    runsToProcessQueue.put(arun)
    reportsQueue.put('This is crap :D ')
    stop.set()

    Sonic = reportsMngr
    ForestGump = rpMngr

    # lol

    Sonic.runSonicRun()
    ForestGump.runForestRun()
