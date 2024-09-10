package com.slack.astra.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;

import java.io.IOException;
import java.util.List;

public class QueryBuilderUtil {
  public static QueryBuilder generateQueryBuilder(String queryString, long startTime, long endTime) throws IOException {
    if (queryString == null || queryString.isEmpty() || queryString.equals("*") || queryString.equals("*.*")) {
      return null;
    }

    SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
    ObjectMapper objectMapper = new ObjectMapper();
    NamedXContentRegistry namedXContentRegistry =
        new NamedXContentRegistry(searchModule.getNamedXContents());
    JsonXContentParser jsonXContentParser =
        new JsonXContentParser(
            namedXContentRegistry,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            objectMapper.createParser(
                """
                    {"bool":{"filter":[{"range":{"_timesinceepoch":{"gte":%d,"lte":%d,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"%s"}}]}}
                    """.formatted(startTime, endTime, queryString)
            ));
    return AbstractQueryBuilder.parseInnerQueryBuilder(jsonXContentParser);
  }
}
