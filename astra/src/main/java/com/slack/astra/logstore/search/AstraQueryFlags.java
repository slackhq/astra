package com.slack.astra.logstore.search;

import org.apache.commons.lang3.tuple.Pair;

public class AstraQueryFlags {

  // Determines whether a particular query flag is enabled (that is, a flag that is in the query
  // itself, e.g. "foo: bar !astra.QueryFlag").
  // Returns a pair containing whether the flag is enabled and the new query string without the flag
  // if it exists
  public static Pair<Boolean, String> isQueryFlagEnabled(String query, String queryFlag) {
    if (query == null) {
      return Pair.of(false, null);
    }

    if (queryFlag == null) {
      return Pair.of(false, query);
    }

    if (!queryFlag.startsWith("!")) {
      queryFlag = "!" + queryFlag;
    }

    if (query.contains(queryFlag)) {
      String newQueryString = query.replace(queryFlag, "");
      return Pair.of(true, newQueryString);
    }

    return Pair.of(false, query);
  }
}
