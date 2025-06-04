package com.slack.astra.metadata.search;

/** Enum representing the type of SearchMetadataStore. */
public enum SearchMetadataStoreType {
  /** The legacy non-partitioned store using the /search path. */
  LEGACY,

  /** The partitioned store using the /partitioned_search path. */
  PARTITIONED
}
