# Synapse Span Table

A Synapse Span Table can be thought of a type of table in Synapse that has no column limit. Write records to a Span Table and don't worry about the number of columns. This library also contains a `flexsert_span_table_record` method that allows for the underlying structure of the Span Table to change if inserting data that conforms to a new schema since the last time data was inserted into a Span Table.

## Test
Copy `config.ini_example` to `config.ini`, add necessary information, then run `python3 test.py`.
