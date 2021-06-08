import json.encoder
import pandas as pd
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns, build_table, table


class SynapseSpanTable:
    # Span Table's regular table where knowledge of the Span Tables is stored.
    COLUMN_LIMIT = 152
    MAX_STRING_LEN = 50
    QUEUE_TABLES = True
    TABLE_QUEUES = {}
    DOC_FLUSH_COUNT = 100
    FLUSH_BYTE_LIMIT = 100000

    def __init__(self, syn, projectName, columnLimit=152, maxStringLength=50, queueTables=False, docFlushCount=100):
        self.syn = syn
        self.projectName = projectName
        self.MAX_STRING_LEN = maxStringLength
        self.QUEUE_TABLES = queueTables
        self.COLUMN_LIMIT = columnLimit
        self.DOC_FLUSH_COUNT = docFlushCount

    def __dataframe_by_columns_intersection(self, dataframe, columns):
        return dataframe.reindex(columns=dataframe.columns.intersection(columns)).ffill()

    def __dataframe_by_columns_symmetric_difference(self, dataframe, columns):
        return dataframe.reindex(columns=dataframe.columns.symmetric_difference(columns)).ffill()

    def __dataframe_by_columns_union(self, dataframe, columns):
        return dataframe.reindex(columns=dataframe.columns.union(columns)).ffill()

    def __get_cleaned_data_in_dataframe(self, data):
        df = pd.DataFrame([data])
        df = df.astype(str).apply(lambda x: x.str[:self.MAX_STRING_LEN])
        return df

    def __clear_span_table_queue(self):
        self.QUEUE_TABLES = False
        self.TABLE_QUEUES.clear()

    #
    # Base Table operations.
    #

    def create_span_table_base_table(self, tableName):
        baseTableSchema = Schema(tableName,
                        [Column(name='id', columnType='STRING')],
                        parent=self.projectName)
        self.syn.store(Table(baseTableSchema, []))

        # this may be faster
        #response_table_df = pd.DataFrame([{'id': respid}])
        #table = synapseclient.table.build_table(tableName, self.projectName, response_table_df)
        #syn.store(table)

    def delete_span_table_base_table_record(self, tableName, recordId):
        synId = self.syn.findEntityId(tableName, self.projectName)
        row = self.syn.tableQuery("select * from " + synId + " where id='" + recordId + "'",
                                  resultsAs="rowset", limit=1)
        self.syn.delete(row)

    def insert_span_table_base_table_records(self, tableName, df):
        synId = self.syn.findEntityId(tableName, self.projectName)
        baseTableSchema = self.syn.get(synId)
        self.syn.store(Table(baseTableSchema, df))

    def insert_span_table_base_record(self, tableName, recordId):
        synId = self.syn.findEntityId(tableName, self.projectName)
        baseTableSchema = self.syn.get(synId)
        df = pd.DataFrame([{'id': recordId}])
        self.syn.store(Table(baseTableSchema, df))

    #
    # Record operations.
    #

    # create the table with the metadata
    def create_span_table_record_table(self, tableName, df, index=1):
        if len(df.columns) > self.COLUMN_LIMIT:
            # safeguard adding too many columns
            df = df.iloc[:, [0, self.COLUMN_LIMIT]]

        print('Creating response form data table: %s_%d' % (tableName, index))
        table_name = tableName + "_" + str(index)
        columns = []
        for column in df.columns:
            columns.append(Column(name=column, columnType='STRING'))
        schema = Schema(table_name, columns, parent=self.projectName)
        self.syn.store(Table(schema, df))

        # return the columns that were used
        return df.columns

    def add_response_data_to_tables(self, tableName, df):
        # put the response data into a pandas dataframe for easy manipulation
        full_df = df
        metadata_df = pd.DataFrame(full_df['id'])
        response_df = df.drop('id', 1)

        table = None
        table_index = 0
        unused_columns = response_df.columns
        while len(unused_columns) > 0:
            # Loop through the tables, adding data to existing full tables in place
            table_index += 1
            table_name = tableName + "_" + str(table_index)
            synID = self.syn.findEntityId(table_name, self.projectName)
            try:
                # get the synapse columns for the table current table
                schema = self.syn.get(synID)
                print("Updating response form table: %s" % schema.name)
                cols = self.syn.getTableColumns(schema)
                colNames = []
                for column in cols:
                    colNames.append(column.name)
                table_columns = len(colNames)
                # use pandas to split the new df with only the columns in table
                # table_df has the columns for this table including metadata
                table_df = self.__dataframe_by_columns_intersection(full_df, colNames)
                unused_columns = list(set(unused_columns) - set(table_df.columns))

                if len(unused_columns) > 0 and table_columns < self.COLUMN_LIMIT:
                    # doing some math here
                    # the existing table data takes up the first x columns
                    # so the new_table_df should be at most self.COLUMN_LIMIT minus the number of existing columns
                    lst_idx = self.COLUMN_LIMIT - table_columns
                    unused_df = self.__dataframe_by_columns_intersection(response_df,
                                                                         unused_columns[:lst_idx])
                    columns = []
                    for column in unused_columns[:lst_idx]:
                        columns.append(Column(name=column, columnType='STRING'))
                    schema.addColumns(columns)
                    schema = self.syn.store(schema)
                    table_df = table_df.join(unused_df)
                    unused_columns = list(set(unused_columns) - set(table_df.columns))

                # store the table: it is either full or there are no more unused columns
                self.syn.store(Table(schema, table_df))

            except TypeError:
                if synID is None:
                    # doing some math here
                    # the metadata takes up the first x columns
                    # so the new_table_df should be at most self.COLUMN_LIMIT minus the number of metadata columns

                    lst_idx = self.COLUMN_LIMIT - len(metadata_df.columns)
                    columns = list(metadata_df.columns) + list(unused_columns[:lst_idx])
                    new_table_df = self.__dataframe_by_columns_intersection(full_df, columns)
                    used_columns = self.create_span_table_record_table(tableName, new_table_df, table_index)
                    unused_columns = list(set(unused_columns) - set(used_columns))
                    continue
                else:
                    raise TypeError

        return

    def create_span_table_record(self, tableName, df):
        # Store the doc ID in base table first.
        self.insert_span_table_base_record(tableName, df['id'][0])

        # Now trickle out into span tables.
        self.add_response_data_to_tables(tableName, df)

    def exists_span_table_record(self, tableName, recordId):
        synId = self.syn.findEntityId(tableName, self.projectName)
        row = self.syn.tableQuery("select * from " + synId + " where id='" + recordId + "'",
                                  resultsAs="rowset", limit=1)
        if row.count == 0:
            return False
        else:
            return True

    def read_span_table_record(self, tableName, recordId):
        synId = self.syn.findEntityId(tableName, self.projectName)
        query = self.syn.tableQuery("select * from " + synId + " where id='" + recordId + "'", resultsAs="rowset", limit=1)
        if query.count == 0:
            return None
        else:
            data = {}
            table_index = 1
            table_name = tableName + "_" + str(table_index)
            synID = self.syn.findEntityId(table_name, self.projectName)
            while synID is not None:
                # Loop through the tables, adding data to existing full tables in place
                try:
                    # get the synapse columns for the table current table
                    query = self.syn.tableQuery("select * from " + synID + " where id='" + recordId + "'",
                                         resultsAs="rowset", limit=1)
                    if query.count > 0:
                        row = query.rowset.rows[0]
                        for headerDefinition in query.rowset.headers:
                            try:
                                data[headerDefinition.name] = row['values'].pop(0)
                            except IndexError:
                                data[headerDefinition.name] = ''
                    table_index += 1
                    table_name = tableName + "_" + str(table_index)
                    synID = self.syn.findEntityId(table_name, self.projectName)
                except TypeError:
                    if synID is None:
                        break
                    else:
                        raise TypeError

            return data

    def update_span_table_record(self, tableName, df):
        recordId = df['id'][0]
        self.delete_span_table_record(tableName, recordId)
        self.create_span_table_record(tableName, df)
        return

    def delete_span_table_record(self, tableName, recordId):
        # delete record in span tables.
        table_index = 1
        table_name = tableName + "_" + str(table_index)
        synID = self.syn.findEntityId(table_name, self.projectName)
        while synID is not None:
            # Loop through the tables, adding data to existing full tables in place
            try:
                # get the synapse columns for the table current table
                row = self.syn.tableQuery("select * from " + synID + " where id='" + recordId + "'",
                                          resultsAs="rowset", limit=1)
                print('Existing form response is being deleted then updated in response form data table: %s'
                      % tableName + "_" + str(table_index))
                self.syn.delete(row)
                table_index += 1
                table_name = tableName + "_" + str(table_index)
                synID = self.syn.findEntityId(table_name, self.projectName)
            except TypeError:
                if synID is None:
                    break
                else:
                    raise TypeError
        return

    def upsert_span_table_record(self, tableName, data):
        df = self.__get_cleaned_data_in_dataframe(data)

        recordId = df['id'][0]
        exists = self.exists_span_table_record(tableName, recordId)
        if exists is True:
            self.update_span_table_record(tableName, df)
        else:
            self.create_span_table_record(tableName, df)
        return

    def flexsert_span_table_record(self, tableName, data):
        df = self.__get_cleaned_data_in_dataframe(data)
        requiredColumns = list(df.drop('id', 1).columns)

        respid = data.get('id')


        # get the list of known response tables
        table_name = tableName
        synID = self.syn.findEntityId(table_name, self.projectName)
        if synID is None:
            # this is the first upload of a form of this response type
            print('First upload of response type with table name: %s' % tableName)
            self.create_span_table_base_table(tableName)
        else:
            schema = self.syn.get(synID)

        # check to see if the record already exists, if it does, do an update, otherwise do an add
        if not self.exists_span_table_record(tableName, respid):
            self.insert_span_table_base_record(tableName, respid)
        else:
            # this is an update to an existing entry - delete it in all tables
            self.delete_span_table_record(tableName, respid)

        self.add_response_data_to_tables(tableName, df)

    def queue_span_table_record(self, tableName, data):
        self.QUEUE_TABLES = True
        df = self.__get_cleaned_data_in_dataframe(data)
        try:
            spanTableDf = self.TABLE_QUEUES[tableName]
            spanTableDf = spanTableDf.append(df)
        except KeyError:
            spanTableDf = df
            self.create_span_table_base_table(tableName)

        # Cache data in TABLE_QUEUES
        self.TABLE_QUEUES[tableName] = spanTableDf

        if self.TABLE_QUEUES[tableName].memory_usage(index=True).sum() >= self.FLUSH_BYTE_LIMIT:
            print('Immediately flushing table over %d bytes' % self.FLUSH_BYTE_LIMIT)
            self.flush_span_table(tableName)
            del self.TABLE_QUEUES[tableName]

    def flush_span_table(self, tableName):
        try:
            df = self.TABLE_QUEUES[tableName]
            print('Flushing %d docs in %s' % (len(df.index), tableName))
        except KeyError:
            print('No table named {}').format(tableName)
            return

        # Store ids in base table
        idRecordsDf = self.__dataframe_by_columns_intersection(df, ['id'])
        self.insert_span_table_base_table_records(tableName, idRecordsDf)

        # store records in record tables
        self.add_response_data_to_tables(tableName, df)

    def flush_span_tables(self):
        for table in self.TABLE_QUEUES:
            self.flush_span_table(table)
        self.TABLE_QUEUES.clear()
