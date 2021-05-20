# Synapse Span Table

A Synapse Span Table is a Python based abstraction layer built on top of Synapse Tables. The difference from standard Synapse Tables is there is no column limit to a Synapse Span Table like there is on a Synapse Table. This library also contains a `flexsert_span_table_record` method that allows for the underlying structure of the Synapse Span Table to change if inserting data that conforms to a new schema since the last time data was inserted into a Synapse Span Table.

## Test
Copy `config.ini_example` to `config.ini`, add necessary information, then run `python3 test.py`.
