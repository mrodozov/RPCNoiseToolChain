__author__ = 'rodozov'

'''
Helper class that provides conversions from
one notation to another in terms of any known identifier.
Example usage is when one needs the chamber name but knows
the roll name, the other way around or when it only knows
the RawID(detectorID). Another usable case would be to
provide with list of strip numbers + partition name for given list of channel numbers + chamber name
and vise versa. This is quite useful when entrire lists has to
be retrieved since searching one by one may be heavy (searching among 130k records)
TODO - complete the map automizing the most used cases
'''

import json

class RPCMap(object):

    def __init__(self,JsonMap=None, RawIDsFile=None):
        self.rpcmap = JsonMap
        self.rawidsfile = RawIDsFile
        self.chamberToRollMap = None
        self.rollToChamberMap = None
        self.rawToRollMap = None
        if JsonMap:
            self.chamberToRollMap = self.getChToRollMapFromFile(self.rpcmap)
            self.rollToChamberMap = self.getRollToChamberMapFromFile(self.rpcmap)
        if RawIDsFile:
            self.rawToRollMap = self.getRawIDtoRollMapFromFile(self.rawidsfile)

    def getChToRollMapFromFile(self, fileToCheck):

        data = None
        with open(fileToCheck, "r") as data_file:
            data = json.loads(data_file.read())
        return data

    def getRollToChamberMapFromFile(self, mapFile):
        revertedMap = {}
        chamberToRollMap = self.getChToRollMapFromFile(mapFile)
        for k in chamberToRollMap:
            for secondkey in chamberToRollMap[k]:
                chansStrips = chamberToRollMap[k][secondkey]
                revertedMap[secondkey] = [k, chansStrips]
        return revertedMap

    def getRawIDtoRollMapFromFile(self, mapFile):
        rawtorollmap = {}
        with open(mapFile, 'r') as data_file:
            for line in data_file.read().splitlines():
                args = line.split()
                rawid = args[1]
                roll = args[0]
                rawtorollmap[rawid] = roll
        return rawtorollmap

    def getRollNamesForChamberName(self,chamberName=None):
        result = None

        return result

    def getChamberNamesForRollName(self,rollName=None):
        result = None
        return result

    def getChamberNameForRawID(self,rawID=None):
        result = None
        return result

    def getRollNameForRawID(self,rawID):
        result = None
        return result

    def getRawIDforRollName(self):
        result = None
        return result

    def getListOfRollNameStripsNumsPairsForChamberNameListOfChannelsPair(self,chamberName=None,listOfChannelNums=[]):
        '''

        :param chamberName: name of the chamber. One to many
        :param listOfChannelNums: list of integers from 1 to 96.
        :return: return list of dictionaries formatted {RollName:stripsNumsList[]}
        '''
        result = None
        return None

if __name__ == "__main__":

    rpcnotationsmap = 'resources/rpcMap'
    rawfile = 'resources/resources/RawIDs.txt'
    rpcMap = RPCMap(rpcnotationsmap, rawfile)


    someRollID = rpcMap.rawToRollMap['637637709']
    print someRollID
    #print rpcMap.rollToChamberMap
    #chamberIDforRawID = rpcMap.rollToChamberMap[someRollID]
    #print chamberIDforRawID

    rtochmap = rpcMap.rollToChamberMap
    #chamber = rtochmap[str(someRollID)]

    for k in rtochmap:
        if str(k).find(someRollID) is not -1:
            print k, rtochmap[k][0]

    print 'blablabla'

    if someRollID in rpcMap.rollToChamberMap:
        print rpcMap.rollToChamberMap[someRollID]

    #for r in rollToChMap:
    #    print r, rollToChMap[r][0]

    #print rawToRollMap






