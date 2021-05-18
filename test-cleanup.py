import configparser
import synapseclient
import json
import os
from synapse_spansert import synapse_spansert 

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'config.ini'))
config.sections()

syn = synapseclient.Synapse()
synProjectName= config['SYNAPSE']['ProjectName']
synUserName= config['SYNAPSE']['UserName']
apiKey= config['SYNAPSE']['apiKey']
syn.login(email=synUserName, apiKey=apiKey)
project = syn.get(synProjectName)

tableNames = ['span_table_definitions', 'test_1', 'test_2', 'test_3']

for tableName in tableNames :
    synId = syn.findEntityId(tableName, synProjectName)
    schema = syn.get(synId)
    syn.delete(schema)
