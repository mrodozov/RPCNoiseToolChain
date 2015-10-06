'''
Its a class to connect CERN runregistry api
'''

from thirdPartyAPI.rrapi import RRApi, RRApiError
import socks
import sys

class RRService:

    #TODO - add runregistry URL check using this  #http://stackoverflow.com/questions/3764291/checking-network-connection

    def __init__(self, use_proxy = False):
        self.use_proxy = use_proxy
        return None

    def __del__(self):
        return None

    def getRunRange(self, runType=None, lastRun=None, runDuration=None, RRURL=None):
        runInfo = []

        try:
            #print "Accessing run registry.......\n"
            api = RRApi(RRURL, debug = True, use_proxy=self.use_proxy)
            an_array = api.data('GLOBAL', 'runsummary', 'json', ['number', 'duration', 'runClassName'], {'datasetExists': '= true', 'number': '> '+ str(lastRun), 'duration': '> '+str(runDuration), 'rpcPresent' : 'true', 'runClassName': runType})
            print an_array

        except RRApiError, e:
            print e.message

        return runInfo

    def getRunRangeForInspectedRuns(self, LSFraction):
        #TODO - impement this using the old python scripts, to get lumi section fraction
        runlist = []
        return runlist

if __name__ == "__main__":

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)
    rr_obj = RRService(use_proxy=True)
    rr_obj.getRunRange('Collisions15', '257000', '600', 'http://runregistry.web.cern.ch/runregistry')


