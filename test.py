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

data1 = {
  "id": "1",
  "a": "1",
  "b": "1",
  "c": "1",
  "d": "1",
  "e": "1",
  "f": "1",
  "g": "1"
}

data2 = {
  "id": "2",
  "a": "2",
  "b": "2",
  "c": "2",
  "p": "2",
  "d": "2",
  "f": "2",
  "g": "2",
  "h": "2",
  "i": "2",
  "j": "2"
}

tableName = 'test'
columnLimit = 3

synapse_spansert(syn, synProjectName, tableName, data1, columnLimit)
synapse_spansert(syn, synProjectName, tableName, data2, columnLimit)

# @TODO Check test data.

# @TODO Clean up test data.
