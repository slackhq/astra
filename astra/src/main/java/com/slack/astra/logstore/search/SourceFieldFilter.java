package com.slack.astra.logstore.search;

import com.slack.astra.proto.service.AstraSearch;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SourceFieldFilter {
  private final Optional<Boolean> includeAll;
  private final Optional<Boolean> excludeAll;
  private final Map<String, Boolean> includeFields;
  private final Map<String, Boolean> excludeFields;
  private final List<String> includeWildcards;
  private final List<String> excludeWildcards;
  private final FilterType filterType;

  public enum FilterType {
    INCLUDE,
    EXCLUDE
  }

  public SourceFieldFilter(
      Optional<Boolean> includeAll,
      Optional<Boolean> excludeAll,
      Map<String, Boolean> includeFields,
      Map<String, Boolean> excludeFields,
      List<String> includeWildcards,
      List<String> excludeWildcards) {
    this.includeAll = includeAll;
    this.excludeAll = excludeAll;
    this.includeFields = includeFields;
    this.excludeFields = excludeFields;
    this.includeWildcards = includeWildcards;
    this.excludeWildcards = excludeWildcards;

    if (includeAll.isPresent() || !includeFields.isEmpty() || !includeWildcards.isEmpty()) {
      this.filterType = FilterType.INCLUDE;
    } else {
      this.filterType = FilterType.EXCLUDE;
    }
  }

  public static SourceFieldFilter fromProto(
      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilterProto) {
    return new SourceFieldFilter(
        Optional.of(
            sourceFieldFilterProto.hasIncludeAll() && sourceFieldFilterProto.getIncludeAll()),
        Optional.of(
            sourceFieldFilterProto.hasExcludeAll() && sourceFieldFilterProto.getExcludeAll()),
        sourceFieldFilterProto.getIncludeFieldsMap(),
        sourceFieldFilterProto.getExcludeFieldsMap(),
        sourceFieldFilterProto.getIncludeWildcardsList(),
        sourceFieldFilterProto.getExcludeWildcardsList());
  }

  public boolean appliesToField(String fieldname) {
    Optional<Boolean> all =
        this.filterType == FilterType.INCLUDE ? this.includeAll : this.excludeAll;
    Map<String, Boolean> fields =
        this.filterType == FilterType.INCLUDE ? this.includeFields : this.excludeFields;
    List<String> wildcards =
        this.filterType == FilterType.INCLUDE ? this.includeWildcards : this.excludeWildcards;

    if (all.isPresent()) {
      return all.get();
    }

    if (fields.containsKey(fieldname)) {
      return fields.get(fieldname);
    }

    for (String wildcard : wildcards) {
      if (fieldname.matches(wildcard)) {
        return true;
      }
    }

    return false;
  }

  public List<String> getExcludeWildcards() {
    return excludeWildcards;
  }

  public List<String> getIncludeWildcards() {
    return includeWildcards;
  }

  public Map<String, Boolean> getExcludeFields() {
    return excludeFields;
  }

  public Map<String, Boolean> getIncludeFields() {
    return includeFields;
  }

  public Optional<Boolean> getExcludeAll() {
    return excludeAll;
  }

  public Optional<Boolean> getIncludeAll() {
    return includeAll;
  }

  public FilterType getFilterType() {
    return filterType;
  }
}
