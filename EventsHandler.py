__author__ = 'rodozov'

from Event import SimpleEvent, Observer

'''
The event dispatcher register observers
'''

class EventsHandler(object):

    def __init__(self, observers=None):

        if observers is not None:
            # assert not isinstance(observers, list)
            self.observers = observers
            for o in observers:
                self.addObserver(o)

    def addObserver(self, observer):
        if not observer in self.observers:
            self.observers.append(observer)

    def removeObserver(self,observer):
        if observer in self.observers:
            self.observers.remove(observer)

    def removeAllObservers(self):
        for o in self.observers:
            self.observers.remove(o)

    def addObservers(self,observers):
        for o in observers:
            self.addObserver(o)

    def notify(self, event=None):
        if event:
            #assert not isinstance(event, SimpleEvent)
            for o in self.observers:
                #print 'received event with title: ', event.name
                o.update(event)










