package com.slack.astra.logstore.opensearch;

import static java.util.Collections.emptyList;

import java.util.Collections;
import java.util.Map;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;

public class AstraMapperRegistry {
  private static Map<String, Mapper.TypeParser> mappers;
  private static Map<String, MetadataFieldMapper.TypeParser> metadataMappers;

  private AstraMapperRegistry() {}

  public static MapperRegistry buildNewInstance() {
    if (mappers == null || metadataMappers == null) {
      mappers = Collections.unmodifiableMap(IndicesModule.getMappers(emptyList()));
      metadataMappers = Collections.unmodifiableMap(IndicesModule.getMetadataMappers(emptyList()));
    }
    return new MapperRegistry(mappers, metadataMappers, MapperPlugin.NOOP_FIELD_FILTER);
  }
}
