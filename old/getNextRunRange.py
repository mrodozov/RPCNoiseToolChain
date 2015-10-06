from ConStrParser import extract

#added by mrodozov@cern.ch for possible use

import re, json ,smtplib,email,sys,getopt
from rrapi import RRApi, RRApiError
import subprocess

URL = "http://localhost:22223/runregistry/"

def getNextRange(runType, lastRun, runDuration):

    runInfo = []
    startRun = 0
    endRun = 0
    print "Accessing run registry.......\n"    
    api = RRApi(URL, debug = True)
    an_array = api.data('GLOBAL', 'runsummary', 'json', ['number'], {'datasetExists': '= true', 'number': '> '+ str(lastRun), 'duration': '> '+str(runDuration), 'rpcPresent' : 'true' , 'runClassName': runType})

    #print an_array
    
    if len(an_array) > 0:
        for record in an_array:
            num = record['number']
            runInfo.append(num)
    
    runInfo.sort()

    for run in runInfo:
        print run
    
    if len(runInfo) == 0:
        startRun = lastRun
        endRun = lastRun
    else:
        startRun = runInfo[0]
        endRun = runInfo[len(runInfo)-1]

    print 'Next range is ',startRun,' ',endRun

getNextRange(sys.argv[1],sys.argv[2],sys.argv[3])
