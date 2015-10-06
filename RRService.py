'''
Its a class to connect CERN runregistry api
'''

from rrapi import RRApi, RRApiError

class RRService:

    #TODO - add runregistry URL check using this  #http://stackoverflow.com/questions/3764291/checking-network-connection

    def __init__(self):
        return None

    def __del__(self):
        return None

    def getRunRange(self, runType=None, lastRun=None, runDuration=None, RRURL=None):
        runInfo = []

        #if lastRun is None or lastRun is '0':
        #    lastRun = '240000' #try not to fail with this ugly hack :D

        try:
            #print "Accessing run registry.......\n"
            api = RRApi(RRURL, debug = True)
            an_array = api.data('GLOBAL', 'runsummary', 'json', ['number', 'duration', 'runClassName'], {'datasetExists': '= true', 'number': '> '+ str(lastRun), 'duration': '> '+str(runDuration), 'rpcPresent' : 'true', 'runClassName': runType})
            #print an_array

        except RRApiError, e:
            print e.message

        return runInfo

    def getRunRangeForInspectedRuns(self,LSFraction):
        #TODO - impement this using the old python scripts, to get lumi section fraction
        runlist = []
        return runlist
