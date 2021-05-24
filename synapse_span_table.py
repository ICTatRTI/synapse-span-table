import json.encoder
import pandas as pd
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns, build_table, table

SPAN_TABLE_DEFINITIONS = 'span_table_definitions'

def install_span_table(syn, projectName) :
    spanTableSchemasSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    if spanTableSchemasSynId is None :
        schema = Schema(SPAN_TABLE_DEFINITIONS, [Column(name='tableName', columnType='LARGETEXT'), Column(name='spanTableDefinitions', columnType='LARGETEXT')], parent=projectName)
        table = syn.store(Table(schema, []))
        return table

def get_span_table_definitions(syn, projectName, tableName) :
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    row = syn.tableQuery("select * from " + spanTableDefinitionsSynId + " where tableName='" + tableName + "'",
                         resultsAs="rowset", limit=1)
    if row.count == 0:
        return None
    else :
        spanTableDefinitions = json.loads(row.rowset.rows[0].get('values')[1])
        return spanTableDefinitions

def create_span_table(syn, projectName, tableName, requiredColumns, columnLimit) :
    # Create the base table.
    baseTableSchema = Schema(tableName,
                    [Column(name='id', columnType='LARGETEXT')],
                    parent=projectName)
    baseTable = syn.store(Table(baseTableSchema, []))
    # Calculate span table definitions for this table.
    spanTableDefinitions = []
    while len(requiredColumns) > 0 :
        spanTableDefinition = {
            "tableName": tableName,
            "spanTableName": tableName + "_" + str(len(spanTableDefinitions) + 1),
            "columns": ['id']
        }
        while len(spanTableDefinition['columns']) < columnLimit and len(requiredColumns) > 0 :
            spanTableDefinition['columns'].append(requiredColumns.pop())
        spanTableDefinitions.append(spanTableDefinition)
    # Save the span table definitions.
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    spanTableDefinitionsSchema = syn.get(spanTableDefinitionsSynId)
    data = pd.DataFrame([
        {
            "tableName": tableName,
            "spanTableDefinitions": json.dumps(spanTableDefinitions)
        }
    ])
    syn.store(Table(spanTableDefinitionsSchema, data))
    # Create the span tables from the definitions.
    for spanTableDefinition in spanTableDefinitions :
        columns = []
        for column in spanTableDefinition['columns'] :
            columns.append(Column(name=column, columnType='LARGETEXT'))
        schema = Schema(spanTableDefinition['spanTableName'],
                        columns,
                        parent=projectName)
        table = syn.store(Table(schema, []))

def update_span_table(syn, projectName, tableName, spanTableDefinitions, requiredColumns, columnLimit):
    # Find all columns currently in definitions.
    currentColumns = []
    for spanTableDefinition in spanTableDefinitions :
        currentColumns = currentColumns + spanTableDefinition['columns']
    # Of the requiredColumns, figure out which ones need to be added given currentColumns.
    columnsToAdd = []
    for requiredColumn in requiredColumns :
        if (requiredColumn not in currentColumns) :
            columnsToAdd.append(requiredColumn)
    # Fill up span tables with room with newColumns and update corresponding entry in spanTableDefinitions.
    for spanTableDefinition in spanTableDefinitions :
        while len(spanTableDefinition['columns']) < columnLimit and len(columnsToAdd) > 0 :
            columnName = columnsToAdd.pop()
            spanTableDefinition['columns'].append(columnName)
            newColumn = syn.store(Column(name=columnName, columnType='LARGETEXT'))
            synId = syn.findEntityId(spanTableDefinition['spanTableName'], projectName)
            schema = syn.get(synId)
            schema.addColumn(newColumn)
            syn.store(schema)
    # All span tables filled up and still columns to add? Lets create some more span tables.
    while len(columnsToAdd) > 0 :
        # Add the span table to the definitions list.
        spanTableDefinition = {
            "tableName": tableName,
            "spanTableName": tableName + "_" + str(len(spanTableDefinitions) + 1),
            "columns": ['id']
        }
        while len(spanTableDefinition['columns']) < columnLimit and len(columnsToAdd) > 0:
            spanTableDefinition['columns'].append(columnsToAdd.pop())
        spanTableDefinitions.append(spanTableDefinition)
        # Now actually create the span table.
        columns = []
        for column in spanTableDefinition['columns'] :
            columns.append(Column(name=column, columnType='LARGETEXT'))
        schema = Schema(spanTableDefinition['spanTableName'],
                        columns,
                        parent=projectName)
        syn.store(Table(schema, []))
    # Save updated spanTableDefinitions to SPAN_TABLE_DEFINITIONS table.
    spanTableDefinitionsSynId = syn.findEntityId(SPAN_TABLE_DEFINITIONS, projectName)
    row = syn.tableQuery("select * from " + spanTableDefinitionsSynId + " where tableName='" + tableName + "'",
                         resultsAs="rowset", limit=1)
    syn.delete(row)
    spanTableDefinitionsSchema = syn.get(spanTableDefinitionsSynId)
    data = pd.DataFrame([
        {
            "tableName": tableName,
            "spanTableDefinitions": json.dumps(spanTableDefinitions)
        }
    ])
    syn.store(Table(spanTableDefinitionsSchema, data))

def drop_span_table(syn, projectName, tableName):
    # @TODO
    return

def create_span_table_record(syn, projectName, tableName, data, columnLimit=152):
    # Store in base table first.
    row = {
        "id": data['id']
    }
    synId = syn.findEntityId(tableName, projectName)
    baseTableSchema = syn.get(synId)
    df = pd.DataFrame([
        row
    ])
    syn.store(Table(baseTableSchema, df))
    # Now trickle out into span tables.
    spanTableDefinitions = get_span_table_definitions(syn, projectName, tableName)
    for spanTableDefinition in spanTableDefinitions :
        row = {}
        for columnName in spanTableDefinition['columns'] :
            if (columnName in data) :
                row[columnName] = data[columnName]
            else :
                row[columnName] = ''
        synId = syn.findEntityId(spanTableDefinition['spanTableName'], projectName)
        spanTableSchema = syn.get(synId)
        df = pd.DataFrame([
            row
        ])
        syn.store(Table(spanTableSchema, df))
    return

def exists_span_table_record(syn, projectName, tableName, id):
    synId = syn.findEntityId(tableName, projectName)
    row = syn.tableQuery("select * from " + synId + " where id='" + id + "'",
                         resultsAs="rowset", limit=1)
    if row.count == 0:
        return False
    else:
        return True

def read_span_table_record(syn, projectName, tableName, id):
    synId = syn.findEntityId(tableName, projectName)
    query = syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset", limit=1)
    if query.count == 0:
        return None
    else:
        data = {}
        spanTableDefinitions = get_span_table_definitions(syn, projectName, tableName)
        for spanTableDefinition in spanTableDefinitions:
            synId = syn.findEntityId(spanTableDefinition['spanTableName'], projectName)
            query = syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset", limit=1)
            if query.count > 0 :
                row = query.rowset.rows[0]
                for headerDefinition in query.rowset.headers :
                    data[headerDefinition.name] = row['values'].pop(0)
        return data

def update_span_table_record(syn, projectName, tableName, data, columnLimit=152):
    delete_span_table_record(syn, projectName, tableName, data['id'])
    create_span_table_record(syn, projectName, tableName, data, columnLimit=152)
    return

def delete_span_table_record(syn, projectName, tableName, id):
    # First delete record in base table.
    #synId = syn.findEntityId(tableName, projectName)
    #row = syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset", limit=1)
    #syn.delete(row)
    # Now delete record in span tables.
    spanTableDefinitions = get_span_table_definitions(syn, projectName, tableName)
    for spanTableDefinition in spanTableDefinitions:
        synId = syn.findEntityId(spanTableDefinition['spanTableName'], projectName)
        row = syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset", limit=1)
        syn.delete(row)
    return

def upsert_span_table_record(syn, projectName, tableName, data, columnLimit=152) :
    exists = exists_span_table_record(syn, projectName, tableName, data['id'])
    if exists is True :
        update_span_table_record(syn, projectName, tableName, data, columnLimit)
    else :
        create_span_table_record(syn, projectName, tableName, data, columnLimit)
    return

def flexsert_span_table_record(syn, projectName, tableName, data, columnLimit=152):
    requiredColumns = list(set(list(data.keys())) - set(['id']))
    spanTableDefinitions = get_span_table_definitions(syn, projectName, tableName)
    if spanTableDefinitions:
        update_span_table(syn, projectName, tableName, spanTableDefinitions, requiredColumns, columnLimit)
    else :
        create_span_table(syn, projectName, tableName, requiredColumns, columnLimit)
    upsert_span_table_record(syn, projectName, tableName, data)
