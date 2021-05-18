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

with open(os.path.join(os.getcwd(), 'data-1.json')) as f:
  data1 = json.load(f)
with open(os.path.join(os.getcwd(), 'data-2.json')) as f:
  data2 = json.load(f)

tableName = 'test'
columnLimit = 3

synapse_spansert(syn, synProjectName, tableName, data1, columnLimit)

# @TODO Check test data.

# @TODO Clean up test data.