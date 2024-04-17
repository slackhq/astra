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
        <response type="200">
            <sample lang="JSON">
                {
                  "datasetMetadata": [
                    {
                      "name": "example",
                      "owner": "example",
                      "throughputBytes": "4000000",
                      "partitionConfigs": [
                        {
                          "startTimeEpochMs": "1698967727980",
                          "endTimeEpochMs": "1698969132808",
                          "partitions": [
                            "0"
                          ]
                        },
                        {
                          "startTimeEpochMs": "1698969132809",
                          "endTimeEpochMs": "1699035709531",
                          "partitions": [
                            "0"
                          ]
                        },
                        {
                          "startTimeEpochMs": "1699035709532",
                          "endTimeEpochMs": "9223372036854775807",
                          "partitions": [
                            "0",
                            "1"
                          ]
                        }
                      ],
                      "serviceNamePattern": "_all"
                    }
                  ]
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/RestoreReplica" method="POST">
        <request> 
            <sample lang="JSON">
                {
                  "serviceName": "example",
                  "startTimeEpochMs": "1713375159221",
                  "endTimeEpochMs": "1713378759221"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                    "status": "success"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/RestoreReplicaIds" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "idsToRestore": [
                    "f3a94795-71a1-4735-9eb1-60968fbae196"
                  ]
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                    "status": "success"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/UpdateDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "name": "example",
                  "owner": "Updated owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "name": "example",
                  "owner": "Updated owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment" method="POST">
        <request>
            <sample lang="JSON" title="Complete update">
                {
                  "name": "example",
                  "throughputBytes": "4000000",
                  "partitionIds": [
                    "0"
                  ]
                }
            </sample>
            <sample lang="JSON" title="Throughput only">
                {
                  "name": "example",
                  "throughputBytes": "4000000",
                  "partitionIds": []
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "assignedPartitionIds": [
                    "0"
                  ]
                }
            </sample>
        </response>
    </api-endpoint>
</api-doc> 