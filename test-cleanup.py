import configparser
import synapseclient
import json
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'config.ini'))
config.sections()

syn = synapseclient.Synapse()
synProjectName= config['SYNAPSE']['ProjectName']
authToken = config['SYNAPSE']['authToken']
syn.login(authToken=authToken)
project = syn.get(synProjectName)

children = syn.getChildren(synProjectName)
for entity in children :
    print('deleting %s' % entity['id'])
    syn.delete(entity['id'])