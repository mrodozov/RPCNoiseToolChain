import re, json ,smtplib,email,sys,getopt,subprocess
from ConStrParser import extract
from rrapi import RRApi, RRApiError

def getRangeWithStartAndDuration(runType, lastRun, runDuration, RRUrl):

        runInfo = []
        startRun = 0
        endRun = 0
        print "Accessing run registry.......\n"
        api = RRApi(RRUrl, debug = True)
        an_array = api.data('GLOBAL', 'runsummary', 'json', ['number','startTime', 'duration'], {'datasetExists': '= true', 'number': '> '+ str(lastRun),
        'duration': '> '+str(runDuration), 'rpcPresent' : 'true' , 'runClassName': runType})
        for k in an_array:
            print k['number'], k['startTime'], k['duration']


#URL = "http://runregistry.web.cern.ch/runregistry/"
URL = "http://localhost:22223/runregistry/"
#arguments - runType -> Cosmics15, Collisions15 etc or combined like "Cosmics15 OR Collisions16" lstRun -> integer, like 257000 runDuration -> integer in seconds like 600 (it will get all > 10 minutes)
getNextRange(sys.argv[1],sys.argv[2],sys.argv[3],URL)
            
