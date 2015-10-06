import re, simplejson as json
from rrapi import RRApi, RRApiError

# Default User app URL
URL  = "http://runregistry.web.cern.ch/runregistry/"

def main():
    try:

        # Construct API object
        api = RRApi(URL, debug = True)

        # Print all metadata
        for w in api.workspaces():
            print "Workspace: ", w
            for t in api.tables(w):
                print "Table: ", t
                print "Columns: ", api.columns(w, t)
                print "Templates: ", api.templates(w, t)

        # Application name (there are 3 applications but please stick to USER)
        print "Application:", api.app

        #Example queries
        if api.app == "online":
            print api.count(workspace = 'GLOBAL', table = 'datasets')
            print api.count(workspace = 'GLOBAL', table = 'runsummary')
            print api.data( workspace = 'GLOBAL', table = 'datasets',   template = 'csv', filter = {'castor': {'status': '= BAD'}})
            print api.data( workspace = 'GLOBAL', table = 'datasets',   template = 'csv',  filter = {'datasetState':'= OPEN'}, query = "{castor.status} = 'GOOD'")
            print api.data( workspace = 'GLOBAL', table = 'runsummary', template = 'csv', columns = ['number', 'scalPresent', 'startTime', 'sequenceName', 'datasetExists'], filter = {'datasetExists': '= true'}, order = ['number asc'])

        if api.app == "offline":
            print api.count(workspace = 'GLOBAL', table = 'dqmgui')
            print api.count(workspace = 'GLOBAL', table = 'datasets')
            print api.data( workspace = 'GLOBAL', table = 'datasets',   template = 'csv', filter = {'castor': {'status': '= BAD'}})
            print api.data( workspace = 'GLOBAL', table = 'datasets',   template = 'csv',  filter = {'datasetState':'= OPEN'}, query = "{castor.status} = 'GOOD'")
            print api.data( workspace = 'GLOBAL', table = 'dqmgui', template = 'json', filter = {'datasetExists': '= true'})

        if api.app == "user":
            print api.count(workspace = 'GLOBAL', table = 'runlumis')
            print api.count(workspace = 'GLOBAL', table = 'datasetlumis')
            print api.count(workspace = 'GLOBAL', table = 'runsummary')
            print api.count(workspace = 'GLOBAL', table = 'datasets')
            print api.data( workspace = 'GLOBAL', table = 'datasets',   template = 'csv', filter = {'castor': {'status': '= BAD'}})
            print api.data( workspace = 'GLOBAL', table = 'datasets',   template = 'csv',  filter = {'datasetState':'= OPEN'}, query = "{castor.status} = 'GOOD'")
            print api.data('GLOBAL', 'runsummary', 'json', ['scalPresent', 'startTime', 'sequenceName'], {'datasetExists': '= true'})

            tags = api.tags()
            print json.dumps(tags, sort_keys=True, indent=4)
            for tag in tags:
                print api.count(workspace = 'GLOBAL', table = 'datasets', tag = tag['name'])

            reports = api.reports('GLOBAL')
            print json.dumps(reports, sort_keys=True, indent=4)
            for report in reports:
                print api.report('GLOBAL', report['name'])

    except RRApiError, e:
        print e

if __name__ == '__main__':
    main()
