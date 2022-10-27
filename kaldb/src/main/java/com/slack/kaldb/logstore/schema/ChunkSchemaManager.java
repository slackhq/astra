package com.slack.kaldb.logstore.schema;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/** ChunkSchemaManager class manages the schema for a chunk. */
public class ChunkSchemaManager {

  // Serialize the field def map.
  // De-serialize the field def map.

  // Initialize the default field schema. Static for now but read from a file in the future.
  static void serializeSchemaToFile(
      Map<String, SchemaAwareLogDocumentBuilderImpl.FieldDef> fieldDefMap, File fileName)
      throws IOException {
    // Make a fieldMap string

    CharSequence serializedFieldDef = null;
    Files.asCharSink(fileName, Charset.forName("UTF-8")).write(serializedFieldDef);
  }
}
