# Schema

User-defined field schemas defined in the preprocessor component.  

## Overview
The preprocessor component reads the bulk API request and if a schema field is defined respects the field definition.
If not defined the preprocessor will use the default heuristics to associate a field with a data type.

To configure the schema file define the [preprocessorConfig.schemaFile](Config-options.md#schemafile)  and set the value to the path of the schema file. The file should be in JSON or YAML format.

## Schema Field Types

The `schema.proto` file is the best place to see the supported field types and related configuration options.

Here's the current list of fields that can be defined in the schema file:

{type="medium"}
KEYWORD
: A text field that is not tokenized. It is used for searching as a whole and supports exact matches

TEXT
: A text field that is tokenized. It is used for searching and supports partial matches

IP
: An IP address field. Supports IPv4 and IPv6 and range searches along with CIDR notation.

DATE
: A date field. Supports date range searches. We plan to support `format` in the future. Currently we expect the value to be a long that represents the number of milliseconds since epoch.

BOOLEAN
: A boolean field. We use `Boolean.parseBoolean(value.toString())` to convert the value to a boolean.

DOUBLE
: Numeric field that supports double values.

FLOAT
: Numeric field that supports float values.

HALF_FLOAT
: Numeric field that supports half float values. `HalfFloat` is a 16-bit floating point number.

INTEGER
: Numeric field that supports integer values.

LONG
: Numeric field that supports long values.

SCALED_LONG
: Numeric field that supports long values. The value is multiplied by the scaling factor before indexing. WIP: `scaling_factor` is not supported yet.

SHORT
: Numeric field that supports short values.

BYTE
: Numeric field that supports byte values.

BINARY 
: Binary field.

> All fields except `TEXT` and `BINARY` support filtering, aggregations, and sorting.
> {style="note"}

## Field Configuration Options

{type="full"}
`ignore_above`
: This is used for the `KEYWORD` field. If the length of the value is greater than `ignore_above` then the value is not indexed.


### Future work
1. Support for `format` in the date field.
2. Support for `scaling_factor` in the `SCALED_LONG` field.
3. Load the schema field in ZooKeeper and provide a REST API to update the schema. This means the schema info can be shared between the preprocessor and the indexer.
4. Support configuring options like `index`, `store`, `doc_values`, `analyzer`, `search_analyzer`, `search_quote_analyzer` etc. We will work on this only after the schema file in ZK, so that this information does not need to set on a per document basis.

### Example Schema Snippet of JSON AND YAML

```json
{
  "fields": {
    "host": {
      "type": "KEYWORD"
    },
    "message": {
      "type": "TEXT",
      "fields": {
        "keyword": {
          "type": "KEYWORD",
          "ignore_above": 256
        }
      }
    },
    "ip": {
      "type": "IP"
    }
  }
}
```

```yaml
fields:
  host:
    type: KEYWORD
  message:
    type: TEXT
    fields:
      keyword:
        type: KEYWORD
        ignore_above: 256
  ip:
    type: IP
```