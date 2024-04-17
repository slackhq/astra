# Schema

User-defined field schemas defined in the preprocessor component.

<tldr>experimental</tldr>

## Overview
The preprocessor component reads the [bulk API request](API-opensearch.md#preprocessor-node-apis) and if a schema field is defined respects the field definition.
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

### Known limitations
* The date field `format` option is not currently supported
* The scaled long `scaling_factor` option is not currently supported

### Example

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