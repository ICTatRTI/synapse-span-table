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
    # Ensure the span_table_schemas table exists.
    install(syn, projectName)
    # Ensure the span tables have the necessary schema to support adding this data.
    columns = list(set(list(data.keys())) - set(['id']))
    update_span_table_schema(syn, projectName, tableName, columns, columnLimit)
    # Upsert data.
    upsert_span_table_data(syn, projectName, tableName, data)


def install(syn, projectName) :
    spanTableSchemasSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    if spanTableSchemasSynId is None :
        schema = Schema(SPAN_TABLE_DEFINITIONS, [Column(name='tableName', columnType='LARGETEXT'), Column(name='spanTableDefinitions', columnType='LARGETEXT')], parent=projectName)
        table = syn.store(Table(schema, []))
        return table

def update_span_table_schema(syn, projectName, tableName, columns, columnLimit) :
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    row = syn.tableQuery("select * from " + spanTableDefinitionsSynId + " where tableName='" + tableName + "'", resultsAs="rowset", limit=1)
    if row.count == 0 :
        spanTableDefinitions = []
        while len(columns) > 0 :
            spanTableDefinition = {
                "tableName": tableName,
                "spanTableName": tableName + "_" + str(len(spanTableDefinitions) + 1),
                "columns": ['id']
            }
            while len(spanTableDefinition['columns']) <= columnLimit and len(columns) > 0 :
                spanTableDefinition['columns'].append(columns.pop())
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
    else :
        # @TODO
        schema = syn.get("syn000000")
        bday_column = syn.store(Column(name='birthday', columnType='DATE'))
        schema.addColumn(bday_column)
        schema = syn.store(schema)

def upsert_span_table_data(syn, projectName, tableName, data) :
    # @TODO
    return







