__author__ = 'rodozov'

class SimpleEvent(object):

    def __init__(self,name=None,finish_status=None,message=None):
        self.name = name
        self.finish_status = finish_status
        self.message = message


class Observer(object):

    def update(self, event=None):
        raise NotImplementedError