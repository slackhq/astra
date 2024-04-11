# Zipkin compatible API

Definitions for Zipkin compatible API, queryable via the Grafana datasource integration. 

<api-doc openapi-path="../api/zipkin_api.yaml">
    <api-endpoint endpoint="/api/v2/trace/{traceId}" method="GET">
        <response type="200">
            <sample lang="JSON">
               [
                    {
                        "id": "7c52ad9bb8f83565",
                        "name": "GET",
                        "remoteEndpoint": {
                            "serviceName": "astraQuery" 
                        },
                        "tags": {
                            "remote_endpoint_ipv4": "10.100.10.100",
                            "_timesinceepoch": "2024-04-11T22:04:44.793Z",
                            "http.path": "/metrics",
                            "http.url": "http://10.199.199.199:8080/metrics",
                            "http.protocol": "h1c",
                            "env": "prod",
                            "http.host": "10.199.199.199:8080",
                            "duration": "3516",
                            "local_endpoint_ipv4": "10.199.199.199",
                            "_span_type": "SERVER",
                            "clusterName": "index-example",
                            "address.remote": "/10.100.10.100:47736",
                            "address.local": "/10.199.199.199:8080",
                            "http.method": "GET"
                        },
                        "timestamp": 1712873084793000,
                        "traceId": "7c52ad9bb8f83565"
                    }
                ]
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/api/v2/traces" method="GET">
        <response type="200">
            <sample lang="JSON">
               []
            </sample> 
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/api/v2/services" method="GET">
        <response type="200"> 
            <sample lang="JSON">
               []
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/api/v2/spans" method="GET">
        <response type="200">
            <sample lang="JSON">
               []
            </sample>
        </response>
    </api-endpoint>
</api-doc>  