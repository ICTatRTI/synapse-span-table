#
# synapse_spansert:
#   Utility function for inserting data table with columns greater than Synapse limit of columnLimit and
#   support for data with fluctuating schema. When a table is first created a base table with one 
#   ID column is created, then subsequent tables limited by columnLimit columns are created. Subsequent 
#   tables share the same name affixed with `_<nth table>`. If the table exists and there are new
#   columns in the data being inserted, additional columns are added to the last table until columnLimit
#   columns are reached and then any additional tables are created to accomodate any additional new
#   columns.
#
import json.encoder
import pandas as pd
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns, build_table, table

SPAN_TABLE_DEFINITIONS = 'span_table_definitions'

def synapse_spansert(syn, projectName, tableName, data, columnLimit=152):
    requiredColumns = list(set(list(data.keys())) - set(['id']))
    # Find the Span Table's Definition.
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    row = syn.tableQuery("select * from " + spanTableDefinitionsSynId + " where tableName='" + tableName + "'", resultsAs="rowset", limit=1)
    # No Span Table Definition? Then create it, otherwise update it.
    if row.count == 0:
        create_span_table_definitions(syn, projectName, tableName, requiredColumns, columnLimit)
    else :
        spanTableDefinitions = json.loads(row.rowset.rows[0].get('values')[1])
        update_span_table_definitions(syn, projectName, tableName, spanTableDefinitions, requiredColumns, columnLimit)
    # Upsert the data.
    upsert_span_table_data(syn, projectName, tableName, data)


def install_synapse_spansert(syn, projectName) :
    spanTableSchemasSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    if spanTableSchemasSynId is None :
        schema = Schema(SPAN_TABLE_DEFINITIONS, [Column(name='tableName', columnType='LARGETEXT'), Column(name='spanTableDefinitions', columnType='LARGETEXT')], parent=projectName)
        table = syn.store(Table(schema, []))
        return table

def create_span_table_definitions(syn, projectName, tableName, requiredColumns, columnLimit) :
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    spanTableDefinitions = []
    while len(requiredColumns) > 0 :
        spanTableDefinition = {
            "tableName": tableName,
            "spanTableName": tableName + "_" + str(len(spanTableDefinitions) + 1),
            "columns": ['id']
        }
        while len(spanTableDefinition['columns']) <= columnLimit and len(requiredColumns) > 0 :
            spanTableDefinition['columns'].append(requiredColumns.pop())
        spanTableDefinitions.append(spanTableDefinition)
    spanTableDefinitionsSchema = syn.get(spanTableDefinitionsSynId)
    data = pd.DataFrame([
        {
            "tableName": tableName,
            "spanTableDefinitions": json.dumps(spanTableDefinitions)
        }
    ])
    syn.store(Table(spanTableDefinitionsSchema, data))
    for spanTableDefinition in spanTableDefinitions :
        columns = []
        for column in spanTableDefinition['columns'] :
            columns.append(Column(name=column, columnType='LARGETEXT'))
        schema = Schema(spanTableDefinition['spanTableName'],
                        columns,
                        parent=projectName)
        table = syn.store(Table(schema, []))

def update_span_table_definitions(syn, projectName, tableName, spanTableDefinitions, requiredColumns, columnLimit):
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    # @TODO
    # allColumns = ...
    # newColumns = ...
    # tablesWithRoomToGrow = ...
    # Fill up tables with room with newColumns and update corresponding entry in spanTableDefinitions.
    # If still have newColumns entries, create tables until used up. Collect new table definitions as we go.
    # Save updated spanTableDefinitions to SPAN_TABLE_DEFINITIONS table.

    #bday_column = syn.store(Column(name='birthday', columnType='DATE'))
    #schema.addColumn(bday_column)
    #schema = syn.store(schema)

def upsert_span_table_data(syn, projectName, tableName, data) :
    # @TODO
    return
