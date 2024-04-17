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
    <api-endpoint endpoint="/_msearch" method="POST"> 
        <request>
            <sample lang="JSON">
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