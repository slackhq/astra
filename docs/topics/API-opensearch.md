# OpenSearch compatible API

Definitions for the OpenSearch compatible API, queryable via the Grafana and end users.

<api-doc openapi-path="../api/query_api.yaml">
    <api-endpoint endpoint="/" method="GET">
        <response type="200">
            <sample lang="JSON">
                {
                    "version": {
                        "distribution": "astra",
                        "number": "0.0.1",
                        "lucene_version": "9.7.0"
                    }
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="_msearch" method="POST">
        <request>
            <sample lang="JSON">
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/{indexName}/_mapping" method="GET">
        <response type="200">
            <sample lang="JSON">
                {
                    "_all": {
                        "mappings": {
                            "properties": {
                                "_timesinceepoch": {
                                    "type": "date"
                                }
                                "@version": {
                                    "type": "keyword"
                                },
                                "count": {
                                    "type": "integer"
                                },
                                "payload": {
                                    "type": "keyword"
                                },
                                "has_cookie": {
                                    "type": "boolean"
                                }
                            }
                        }
                    }
                }
            </sample>
        </response>
    </api-endpoint>
</api-doc>