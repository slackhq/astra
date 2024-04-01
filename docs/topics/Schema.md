# Schema

Astra supports defining a schema in the preprocessor component.

The preprocessor component reads the bulk API request and if a schema field is defined respects the field definition.
If not defined the preprocessor will use the default heuristics to associate a field with a data type.

To configure the schema file define the following env variable `PREPROCESSOR_SCHEMA_FILE` and set the value to the path of the schema file. The file should be in JSON or YAML format.

## Schema Field Types

schema.proto is the best place to see the supported field types and related configuration options.

Here's the current list of fields that can be defined in the schema file:
1. `KEYWORD` - A text field that is not tokenized. It is used for searching as a whole and supports exact matches
2. `TEXT` - A text field that is tokenized. It is used for searching and supports partial matches
3. `IP` - An IP address field. Supports IPv4 and IPv6 and range searches along with CIDR notation.
4. `DATE` - A date field. Supports date range searches. We plan to support `format` in the future. Currently we expect the value to be a long that represents the number of milliseconds since epoch.
5. `BOOLEAN` - A boolean field. We use `Boolean.parseBoolean(value.toString()` to convert the value to a boolean.
6. `DOUBLE` - Numeic field that supports double values.
7. `FLOAT` - Numeric field that supports float values.
8. `HALF_FLOAT` - Numeric field that supports half float values. `HalfFloat` is a 16-bit floating point number.
8. `INTEGER` - Numeric field that supports integer values.
9. `LONG` - Numeric field that supports long values.
10. `SCALED_LONG` - Numeric field that supports long values. The value is multiplied by the scaling factor before indexing. WIP: `scaling_factor` is not supported yet.
10. `SHORT` - Numeric field that supports short values.
11. `BYTE` - Numeric field that supports byte values.
12. `BINARY` - Binary field.


NOTE: All fields except TEXT and BINARY support filtering, aggregations, and sorting. 

# Field Configuration Options

1. `ignore_above` - This is used for the `KEYWORD` field. If the length of the value is greater than `ignore_above` then the value is not indexed.

## Future work
1. Support for `format` in the date field.
2. Support for `scaling_factor` in the `SCALED_LONG` field.
3. Load the schema field in ZooKeeper and provide a REST API to update the schema. This means the schema info can be shared between the preprocessor and the indexer.
4. Support configuring options like `index`, `store`, `doc_values`, `analyzer`, `search_analyzer`, `search_quote_analyzer` etc. We will work on this only after the schema file in ZK, so that this information does not need to set on a per document basis.

## Example Schema Snippet if JSON AND YAML

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