# Synapse Span Table

A Synapse Span Table is a Python based abstraction layer built on top of Synapse Tables. The difference from standard Synapse Tables is there is no column limit to a Synapse Span Table like there is on a Synapse Table. This library also contains a `flexsert_span_table_record` method that is forgiving if the underlying table does not exist or the data has different schema than the current Synapse Span Table. 

## Modes

This library uses two 'modes' to split JSON data into span-tables and then upload the data to Synapse. If the `QUEUE_TABLES` flag is set to True, then the library will queue the data in memory in a list of pandas dataframes until the `flush_data_table()` or `flush_data_tables()` is called. Note, the table will be flushed immediately if the `FLUSH_BYTE_LIMIT` value is reached as the size of one of the dataframes in the queued list of dataframes. This is to prevent to data from growing to large and dropping connections with the host.

The second mode is used during normal operation when data needs to be uploaded continuously. If `QUEUE_TABLES` is set to False, data fed into `flexsert_span_table_record()` or `upsert_span_table_record()` are uploaded immediately to Synapse, one record at a time. 

## Limitations
- For the sake of flexibility, every column in a Synapse Span Table is stored as type `STRING`. Thus, if you store numbers, they will come back out as strings when you read them.
- Not all methods are feature complete yet. This library is a work in progress.

## Test

### test.py

Tests the general functionality of the span table library. Note the column width for the tests is set to three columns so that the data is split into many tables.

To run the tests:

Copy `config.ini_example` to `config.ini`, add necessary information, then run `python3 test.py`.


### dup_test.py

Tests whether duplicates are created in the batch data upload process. 

Copy `config.ini_example` to `config.ini`, add necessary information, then run `python3 dup_test.py`.
