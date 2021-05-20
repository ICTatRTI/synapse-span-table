import configparser
import synapseclient
from synapseclient import Project
import json
import os
from synapse_span_table import flexsert_span_table_record, install_span_table

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'config.ini'))
config.sections()

syn = synapseclient.Synapse()
synProjectName= config['SYNAPSE']['ProjectName']
synUserName= config['SYNAPSE']['UserName']
apiKey= config['SYNAPSE']['apiKey']
syn.login(email=synUserName, apiKey=apiKey)


def delete_all_entities_in_project(syn, synProjectName) :
  children = syn.getChildren(synProjectName)
  for entity in children:
    syn.delete(entity['id'])

def cleanup() :
  global syn
  global synProjectName
  delete_all_entities_in_project(syn, synProjectName)


install_span_table(syn, synProjectName)

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

# Test inserting records with different schemas into the same table.
flexsert_span_table_record(syn, synProjectName, tableName, data1, columnLimit)
flexsert_span_table_record(syn, synProjectName, tableName, data2, columnLimit)
#@TODO Write test conditions.
#cleanup()

# @TODO Test updating a record with the same schema.
# @TODO Test updating a record with a different schema.
# Clean up test data.
