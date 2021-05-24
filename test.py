import configparser
import synapseclient
from synapseclient import Project
import json
import os
from synapse_span_table import flexsert_span_table_record, install_span_table, read_span_table_record

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




TestName = 'Should insert records with different schemas into the same table.'
tableName = 'test'
columnLimit = 3
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
install_span_table(syn, synProjectName)
flexsert_span_table_record(syn, synProjectName, tableName, data1, columnLimit)
flexsert_span_table_record(syn, synProjectName, tableName, data2, columnLimit)
record1 = read_span_table_record(syn, synProjectName, tableName, data1['id'])
record2 = read_span_table_record(syn, synProjectName, tableName, data2['id'])
allValuesMatch = True
for key in data1.keys() :
  if record1[key] != data1[key] :
    allValuesMatch = False
for key in data2.keys():
  if record2[key] != data2[key]:
    allValuesMatch = False
if allValuesMatch == True :
  print('Passed: ' + TestName)
else :
  print('Failed: ' + TestName)
cleanup()

TestName = 'Should update a record with the same schema.'
tableName = 'test'
columnLimit = 3
data = {
  "id": "1",
  "a": "1",
  "b": "1",
  "c": "1",
  "d": "1",
  "e": "1",
  "f": "1",
  "g": "1"
}
install_span_table(syn, synProjectName)
flexsert_span_table_record(syn, synProjectName, tableName, data, columnLimit)
data['g'] = '1a'
flexsert_span_table_record(syn, synProjectName, tableName, data, columnLimit)
recordUpdated = read_span_table_record(syn, synProjectName, tableName, data['id'])
if recordUpdated['g'] == '1a' :
  print('Passed: ' + TestName)
else :
  print('Failed: ' + TestName)
cleanup()

# @TODO Test updating a record with a different schema.

# Should properly handle other types of data

# @TODO Test deleting a record.

# @TODO Clean up test data.
#cleanup()
