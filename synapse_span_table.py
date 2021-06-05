import json.encoder
import pandas as pd
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns, build_table, table


class SynapseSpanTable:
    # Span Table's regular table where knowledge of the Span Tables is stored.
    SPAN_TABLE_DEFINITIONS = 'span_table_definitions'
    COLUMN_LIMIT = 152
    QUEUE_TABLES = True
    TABLE_QUEUES = {}
    DOC_FLUSH_COUNT = 100

    def __init__(self, syn, projectName, columnLimit=152, maxStringLength=200, queueTables=False, docFlushCount=100):
        self.syn = syn
        self.projectName = projectName
        self.MAX_STRING_LEN = maxStringLength
        self.QUEUE_TABLES = queueTables
        self.COLUMN_LIMIT = columnLimit
        self.DOC_FLUSH_COUNT = docFlushCount

        self.install_span_table()

    def __dataframe_by_columns_intersection(self, dataframe, columns):
        return dataframe.reindex(columns=dataframe.columns.intersection(columns)).ffill()

    def __dataframe_by_columns_symmetric_difference(self, dataframe, columns):
        return dataframe.reindex(columns=dataframe.columns.symmetric_difference(columns)).ffill()

    def __dataframe_by_columns_union(self, dataframe, columns):
        return dataframe.reindex(columns=dataframe.columns.union(columns)).ffill()

    def __clear_span_table_queue(self):
        self.QUEUE_TABLES = False
        self.TABLE_QUEUES = {}

    # This method is required before using any other methods.
    # It creates Span Table's table where knowledge of Span Tables are stored.
    def install_span_table(self):
        spanTableSchemasSynId = self.syn.findEntityId(self.SPAN_TABLE_DEFINITIONS, self.projectName)
        if spanTableSchemasSynId is None:
            schema = Schema(self.SPAN_TABLE_DEFINITIONS, [Column(name='tableName', columnType='STRING'),
                                                          Column(name='spanTableDefinitions', columnType='LARGETEXT')],
                            parent=self.projectName)
            return self.syn.store(Table(schema, []))

    #
    # Table operations.
    #

    def get_span_table_definitions(self, tableName):
        spanTableDefinitionsSynId = self.syn.findEntityId(self.SPAN_TABLE_DEFINITIONS, self.projectName)
        row = self.syn.tableQuery("select * from " + spanTableDefinitionsSynId + " where tableName='" + tableName + "'",
                                  resultsAs="rowset", limit=1)
        if row.count == 0:
            return None
        else:
            spanTableDefinitions = json.loads(row.rowset.rows[0].get('values')[1])
            return spanTableDefinitions

    def create_span_table_base_table(self, tableName):
        baseTableSchema = Schema(tableName,
                        [Column(name='id', columnType='STRING')],
                        parent=self.projectName)
        self.syn.store(Table(baseTableSchema, []))

    def get_span_table_column_type(self, df, col):
        maxStrLen = df[col].str.len().max()
        if maxStrLen > 1000:
            return 'LARGETEXT'
        else:
            # change it so the column size is long enough for our needs
            return 'STRING'

    def create_span_table(self, tableName, requiredColumns, df):
        # Create the base table.
        self.create_span_table_base_table(tableName)

        # Calculate span table definitions for this table.
        spanTableDefinitions = []
        while len(requiredColumns) > 0:
            spanTableDefinition = {
                "tableName": tableName,
                "spanTableName": tableName + "_" + str(len(spanTableDefinitions) + 1),
                "columns": ['id']
            }
            while len(spanTableDefinition['columns']) < self.COLUMN_LIMIT and len(requiredColumns) > 0:
                spanTableDefinition['columns'].append(requiredColumns.pop())
            spanTableDefinitions.append(spanTableDefinition)
        # Save the span table definitions.
        spanTableDefinitionsSynId = self.syn.findEntityId(self.SPAN_TABLE_DEFINITIONS, self.projectName)
        spanTableDefinitionsSchema = self.syn.get(spanTableDefinitionsSynId)
        data = pd.DataFrame([
            {
                "tableName": tableName,
                "spanTableDefinitions": json.dumps(spanTableDefinitions)
            }
        ])
        self.syn.store(Table(spanTableDefinitionsSchema, data))
        # Create the span tables from the definitions.
        for spanTableDefinition in spanTableDefinitions:
            columns = []
            for column in spanTableDefinition['columns']:
                columnType = self.get_span_table_column_type(df, column)
                columns.append(Column(name=column, columnType=columnType))
            schema = Schema(spanTableDefinition['spanTableName'],
                            columns,
                            parent=self.projectName)
            table = self.syn.store(Table(schema, []))

    def update_span_table(self, tableName, spanTableDefinitions, requiredColumns, df):
        # Find all columns currently in definitions.
        currentColumns = []
        for spanTableDefinition in spanTableDefinitions:
            currentColumns = currentColumns + spanTableDefinition['columns']
        # Of the requiredColumns, figure out which ones need to be added given currentColumns.
        columnsToAdd = []
        for requiredColumn in requiredColumns:
            if requiredColumn not in currentColumns:
                columnsToAdd.append(requiredColumn)
        # Fill up span tables with room with newColumns and update corresponding entry in spanTableDefinitions.
        for spanTableDefinition in spanTableDefinitions:
            while len(spanTableDefinition['columns']) < self.COLUMN_LIMIT and len(columnsToAdd) > 0:
                columnName = columnsToAdd.pop()
                spanTableDefinition['columns'].append(columnName)
                columnType = self.get_span_table_column_type(df, column)
                newColumn = self.syn.store(Column(name=columnName, columnType=columnType))
                synId = self.syn.findEntityId(spanTableDefinition['spanTableName'], self.projectName)
                hadSuccess = False
                while hadSuccess is False:
                    try:
                        schema = self.syn.get(synId)
                        schema.addColumn(newColumn)
                        self.syn.store(schema)
                        hadSuccess = True
                    except Exception as e:
                        if 'Duplicate' in str(e):
                            print('Duplicate column ' + columnName + ', skipping.')
                            hadSuccess = True
                        else:
                            print(e)
        # All span tables filled up and still columns to add? Lets create some more span tables.
        while len(columnsToAdd) > 0:
            # Add the span table to the definitions list.
            spanTableDefinition = {
                "tableName": tableName,
                "spanTableName": tableName + "_" + str(len(spanTableDefinitions) + 1),
                "columns": ['id']
            }
            while len(spanTableDefinition['columns']) < self.COLUMN_LIMIT and len(columnsToAdd) > 0:
                spanTableDefinition['columns'].append(columnsToAdd.pop())
            spanTableDefinitions.append(spanTableDefinition)
            # Now actually create the span table.
            columns = []
            for column in spanTableDefinition['columns']:
                columns.append(Column(name=column, columnType='LARGETEXT'))
            schema = Schema(spanTableDefinition['spanTableName'],
                            columns,
                            parent=self.projectName)
            self.syn.store(Table(schema, []))
        # Save updated spanTableDefinitions to SPAN_TABLE_DEFINITIONS table.
        spanTableDefinitionsSynId = self.syn.findEntityId(self.SPAN_TABLE_DEFINITIONS, self.projectName)
        row = self.syn.tableQuery("select * from " + spanTableDefinitionsSynId + " where tableName='" + tableName + "'",
                                  resultsAs="rowset", limit=1)
        self.syn.delete(row)
        spanTableDefinitionsSchema = self.syn.get(spanTableDefinitionsSynId)
        data = pd.DataFrame([
            {
                "tableName": tableName,
                "spanTableDefinitions": json.dumps(spanTableDefinitions)
            }
        ])
        self.syn.store(Table(spanTableDefinitionsSchema, data))

    def drop_span_table(self, tableName):
        # @TODO
        return

    #
    # Record operations.
    #

    def insert_span_table_base_record(self, tableName, data):
        row = {
            "id": data['id']
        }
        synId = self.syn.findEntityId(tableName, self.projectName)
        baseTableSchema = self.syn.get(synId)
        df = pd.DataFrame([
            row
        ])
        self.syn.store(Table(baseTableSchema, df))

    def create_span_table_record(self, tableName, data):
        # Make sure all values are strings.
        for key in data.keys():
            data[key] = str(data[key])[:self.MAX_STRING_LEN]

        # Store in base table first.
        self.insert_span_table_base_record(tableName, data)

        # Now trickle out into span tables.
        spanTableDefinitions = self.get_span_table_definitions(tableName)
        for spanTableDefinition in spanTableDefinitions:
            row = {}
            for columnName in spanTableDefinition['columns']:
                if columnName in data:
                    row[columnName] = data[columnName]
                else:
                    row[columnName] = ''
            synId = self.syn.findEntityId(spanTableDefinition['spanTableName'], self.projectName)
            spanTableSchema = self.syn.get(synId)
            df = pd.DataFrame([
                row
            ])
            self.syn.store(Table(spanTableSchema, df))
        return

    def exists_span_table_record(self, tableName, id):
        synId = self.syn.findEntityId(tableName, self.projectName)
        row = self.syn.tableQuery("select * from " + synId + " where id='" + id + "'",
                                  resultsAs="rowset", limit=1)
        if row.count == 0:
            return False
        else:
            return True

    def read_span_table_record(self, tableName, id):
        synId = self.syn.findEntityId(tableName, self.projectName)
        query = self.syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset", limit=1)
        if query.count == 0:
            return None
        else:
            data = {}
            spanTableDefinitions = self.get_span_table_definitions(tableName)
            for spanTableDefinition in spanTableDefinitions:
                synId = self.syn.findEntityId(spanTableDefinition['spanTableName'], self.projectName)
                query = self.syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset",
                                            limit=1)
                if query.count > 0:
                    row = query.rowset.rows[0]
                    for headerDefinition in query.rowset.headers:
                        data[headerDefinition.name] = row['values'].pop(0)
            return data

    def update_span_table_record(self, tableName, data):
        self.delete_span_table_record(tableName, data['id'])
        self.create_span_table_record(tableName, data)
        return

    def delete_span_table_record(self, tableName, tableId):
        # First delete record in base table.
        # synId = self.syn.findEntityId(tableName, self.projectName)
        # row = self.syn.tableQuery("select * from " + synId + " where id='" + id + "'", resultsAs="rowset", limit=1)
        # self.syn.delete(row)
        # Now delete record in span tables.
        spanTableDefinitions = self.get_span_table_definitions(tableName)
        for spanTableDefinition in spanTableDefinitions:
            synId = self.syn.findEntityId(spanTableDefinition['spanTableName'], self.projectName)
            row = self.syn.tableQuery("select * from " + synId + " where id='" + tableId + "'", resultsAs="rowset", limit=1)
            self.syn.delete(row)
        return

    def upsert_span_table_record(self, tableName, data):
        exists = self.exists_span_table_record(tableName, data['id'])
        if exists is True:
            self.update_span_table_record(tableName, data)
        else:
            self.create_span_table_record(tableName, data)
        return

    def queue_upsert_span_table_record(self, tableName, data):
        self.QUEUE_TABLES = True
        self.upsert_span_table_record(tableName, data)

    def flexsert_span_table_record(self, tableName, data):
        requiredColumns = list(set(list(data.keys())) - set(['id']))
        spanTableDefinitions = self.get_span_table_definitions(tableName)

        df = pd.DataFrame([data])
        if spanTableDefinitions:
            self.update_span_table(tableName, spanTableDefinitions, requiredColumns, df)
        else:
            self.create_span_table(tableName, requiredColumns, df)
        self.upsert_span_table_record(tableName, data)

    def queue_span_table_record(self, tableName, data):
        self.QUEUE_TABLES = True
        df = pd.DataFrame([data])
        try:
            spanTableDf = self.TABLE_QUEUES[tableName]
            spanTableDf = spanTableDf.append(df)
        except KeyError:
            spanTableDf = df
            self.create_span_table_base_table(tableName)

        # Store id in base table
        self.insert_span_table_base_record(tableName, data)

        # Cache data in TABLE_QUEUES
        self.TABLE_QUEUES[tableName] = spanTableDf

    def flush_span_table(self, tableName):
        try:
            df = self.TABLE_QUEUES[tableName]
        except KeyError:
            print('No table named {}').format(tableName)
            return

        requiredColumns = list(set(list(df.columns)) - set(['id']))
        spanTableDefinitions = self.get_span_table_definitions(tableName)
        if spanTableDefinitions:
            self.update_span_table(tableName, spanTableDefinitions, requiredColumns, df)
        else:
            self.create_span_table(tableName, requiredColumns, df)

        spanTableDefinitions = self.get_span_table_definitions(tableName)
        for idx in range(len(spanTableDefinitions)):
            spanTableName = tableName + "_" + str(idx + 1)
            synId = self.syn.findEntityId(spanTableName, self.projectName)
            tableSchema = self.syn.get(synId)
            cols = self.syn.getTableColumns(tableSchema)
            colNames = []
            for column in cols:
                colNames.append(column.name)
            tableData = self.__dataframe_by_columns_intersection(df, colNames)
            self.syn.store(Table(tableSchema, tableData))

    def flush_span_tables(self):
        for table in self.TABLE_QUEUES:
            self.flush_span_table(table)
        self.TABLE_QUEUES.clear()
