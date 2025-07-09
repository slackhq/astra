# OpenSearch compatible API

Definitions for the OpenSearch compatible API, queryable via the Grafana and end users.

<a href="API-opensearch.md#query-node-apis">Query node</a> | <a href="API-opensearch.md#preprocessor-node-apis">Preprocessor node</a>

## Query Node APIs 

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
    <api-endpoint endpoint="/_msearch" method="POST"> 
        <request>
            <sample lang="JSON" title="JSON example">
                {"index": "_all"}
                {"size":0,"query":{"bool":{"filter":[{"range":{"_timesinceepoch":{"gte":1713287603983,"lte":1713291203983}}},{"query_string":{"query":"*"}}]}},"aggs":{"2":{"date_histogram":{"interval":"10m","field":"_timesinceepoch","min_doc_count":0,"extended_bounds":{"min":1713287603983,"max":1713291203983},"format":"epoch_millis"},"aggs":{}}}}
            </sample>
            <sample lang="JSON" title="JSON expanded">
                {
                    "index": "_all" 
                }
                {
                    "size": 0,
                    "query":
                    {
                        "bool": 
                        { 
                            "filter":
                            [ 
                                {
                                    "range": 
                                    {
                                        "_timesinceepoch":   
                                        {
                                            "gte": 1713287603983, 
                                            "lte": 1713291203983,
                                        }
                                    }
                                },
                                {
                                    "query_string":
                                    {
                                        "query": "*"
                                    }
                                }
                            ]
                        }
                    },
                    "aggs":
                    { 
                        "2":  
                        {
                            "date_histogram":
                            {  
                                "interval": "10m", 
                                "field": "_timesinceepoch",
                                "min_doc_count": 0,
                                "extended_bounds": 
                                {  
                                    "min": 1713287603983, 
                                    "max": 1713291203983  
                                }, 
                                "format": "epoch_millis" 
                            },
                            "aggs": 
                            {}
                        }
                    }
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                    "took": 0,
                    "responses":
                    [
                        { 
                            "took": 52,  
                            "timed_out": false, 
                            "_shards": 
                            {
                                "failed": 0,
                                "total": 3
                            },
                            "_debug":
                            {},
                            "hits":
                            {
                                "total":
                                {
                                    "value": 0,
                                    "relation": "eq"
                                },
                                "max_score": null,
                                "hits":
                                []
                            },
                            "aggregations":
                            {
                                "2":
                                {
                                    "buckets":
                                    [
                                        {
                                            "key": 1713287400000,
                                            "doc_count": 343718
                                        },
                                        { 
                                            "key": 1713288000000,
                                            "doc_count": 591515
                                        },   
                                        {
                                            "key": 1713288600000, 
                                            "doc_count": 547228
                                        }, 
                                        {
                                            "key": 1713289200000,
                                            "doc_count": 467128 
                                        }, 
                                        { 
                                            "key": 1713289800000, 
                                            "doc_count": 539753    
                                        }, 
                                        { 
                                            "key": 1713290400000,  
                                            "doc_count": 481929
                                        },
                                        {
                                            "key": 1713291000000,
                                            "doc_count": 170998
                                        }
                                    ]
                                }
                            },
                            "status": 200
                        }
                    ],
                    "_debug":
                    {
                        "traceId": "d691f1b383052b8d" 
                    }
                }
            </sample>
            <sample lang="JSON">
                {
                    "took": 0,
                    "responses":
                    [
                        {
                            "took": 511,
                            "timed_out": false,
                            "_shards":
                            {
                                "total": 657,
                                "failed": 0
                            },
                            "_debug":
                            {},
                            "hits":
                            {
                                "total":
                                {
                                    "value": 2,
                                    "relation": "eq"
                                },
                                "max_score": null,
                                "hits":
                                [
                                    {
                                        "_index": "desktop",
                                        "_type": "_doc",
                                        "_id": "4fc5c2bda15efe6b",
                                        "_score": null,
                                        "_timesinceepoch": 1713377341.950000000,
                                        "_source":
                                        {
                                            "user_locale": "en-US",
                                            "browser_version": "122.0",
                                            "service_name": "desktop",
                                            "name": "metrics:perf-metrics",
                                            "_timesinceepoch": "2024-04-17T18:09:01.950Z",
                                            "js_heap_used": "",
                                            "duration": 1000,
                                            "browser": "chrome",
                                            "trace_id": "6f6b39ad4e03dbdf7c7842f6b06847d0",
                                            "sub_app_name": "client",
                                            "session_id": "df42e045-2d3b-4e69-b077-e3959ec30745"
                                        },
                                        "sort":
                                        [
                                            1713377341950
                                        ]
                                    },
                                    {
                                        "_index": "desktop",
                                        "_type": "_doc",
                                        "_id": "28894fe29caeced7",
                                        "_score": null,
                                        "_timesinceepoch": 1713377341.950000000,
                                        "_source":
                                        {
                                            "trace_id": "4484772e5f85c93ac8f3df07534c5396",
                                            "device_id": "d6102efb-ccd7-4c56-a445-15ea9267f472",
                                            "app_version": "4.37.101",
                                            "os": "windows",
                                            "_timesinceepoch": "2024-04-17T18:09:01.950Z",
                                            "service_name": "desktop",
                                            "sub_app_name": "client",
                                            "duration": 13507,
                                            "browser": "chrome",
                                            "name": "metrics:virtual-list",
                                            "browser_version": "122.0",
                                        },
                                        "sort":
                                        [
                                            1713377341950
                                        ]
                                    }
                                ]
                            },
                            "aggregations": null,
                            "status": 200
                        }
                    ],
                    "_debug":
                    {
                        "traceId": "5d3150b51ad9e45a"
                    }
                }
            </sample>
        </response>
        <response type="400">
            <sample lang="JSON">
                {
                    "error": "Invalid request format: Unexpected character ('{' (code 123)): was expecting double-quote to start field name"
                }
            </sample>
        </response>
        <response type="500">
            <sample lang="JSON">
                {
                    "took": 0,
                    "responses":
                    [
                        {
                            "took": 0,
                            "status": 500
                        }
                    ]
                }
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

## Preprocessor Node APIs
<api-doc openapi-path="../api/preprocessor_api.yaml">
    <api-endpoint endpoint="/_bulk" method="POST">
        <request>
            <sample lang="JSON" title="JSON example">
                    { "index": {"_index": "testindex", "_id": "1"} }
                    { "field1" : "value1" },
                    { "index": {"_index": "testindex", "_id": "2"} }
                    { "field1" : "value2" }
            </sample>
            <sample lang="JSON" title="JSON expanded">
                    {
                        "index":
                        {
                            "_index": "testindex",
                            "_id": "1"
                        }
                    }
                    {
                        "field1": "value1"
                    }
                     {
                        "index":
                        {
                            "_index": "testindex",
                            "_id": "2"
                        }
                    }
                    {
                        "field1": "value2"
                    }
            </sample>
        </request> 
        <response type="200">
            <sample lang="JSON">
                {
                    "totalDocs": 2,
                    "failedDocs": 0,
                    "errorMsg": ""
                } 
            </sample> 
        </response>
        <response type="400"> 
            <sample lang="JSON">
                {
                    "totalDocs": 0,
                    "failedDocs": 0,
                    "errorMsg": "rate limit exceeded"
                }
            </sample>
        </response>
        <response type="500">
            <sample lang="JSON">
                {
                    "totalDocs": 0,
                    "failedDocs": 0,
                    "errorMsg": "request must contain only 1 unique index"
                }
            </sample>
        </response>
    </api-endpoint>
</api-doc>