# Manager API

API definitions for manager nodes, accessed via manager Docs service and admin tools.

<api-doc openapi-path="../api/manager_api.yaml">
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/CreateDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "name": "indexName",
                  "owner": "Index owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "name": "indexName",
                  "owner": "Index owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/GetDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "name": "indexName"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "name": "indexName",
                  "owner": "Index owner",
                  "throughputBytes": "12000000",
                  "partitionConfigs": [
                    {
                      "startTimeEpochMs": "1706646163791",
                      "endTimeEpochMs": "1706646250152",
                      "partitions": [
                        "0",
                        "1"
                      ]
                    },
                    {
                      "startTimeEpochMs": "1706646250153",
                      "endTimeEpochMs": "9223372036854775807",
                      "partitions": [
                        "0",
                        "1",
                        "2"
                      ]
                    }
                  ],
                  "serviceNamePattern": "_all"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/ListDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/RestoreReplica" method="POST">
        <request> 
            <sample lang="JSON">
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON"> 
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/RestoreReplicaIds" method="POST">
        <request>
            <sample lang="JSON">
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/UpdateDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment" method="POST">
        <request>
            <sample lang="JSON">
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
            </sample>
        </response>
    </api-endpoint>
</api-doc> 