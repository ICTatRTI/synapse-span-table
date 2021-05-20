# Synapse Span Table

A Synapse Span Table is a Python based abstraction layer built on top of Synapse Tables. The difference from standard Synapse Tables is there is no column limit to a Synapse Span Table like there is on a Synapse Table. This library also contains a `flexsert_span_table_record` method that is forgiving if the underlying table does not exist or the data has different schema than the current Synapse Span Table. 

## Limitations
- For the sake of flexibility, every column in a Synapse Span Table is stored as type `LARGETEXT`. Thus, if you store numbers, they will come back out as strings when you read them.
- Not all methods are feature complete yet. This library is a work in progress.

## Test
Copy `config.ini_example` to `config.ini`, add necessary information, then run `python3 test.py`.
