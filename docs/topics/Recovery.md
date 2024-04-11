# Recovery

The recovery nodes are responsible for back-filling data when the indexers make a determination to skip to the
current HEAD of the Kafka topic. A recovery task will be created by the indexer, and then the manager will assign this
task to individual recovery nodes. Recovery nodes do not serve data, and are tuned to quickly index and upload the
missing data, so that they can be served by cache nodes.

## System design
A recovery node indexes the data in the Kafka partition from fixed offsets, such as when an indexer is significantly
behind. This indexer type is typically used to handle log volume spikes.

### Recovery task protocol
* If the indexer is too far behind from the current head, the indexer would create a ***RecoveryTask*** node in ZK.
* A pool of ***RecoveryNode*** are waiting for an assignment in *Free* state.
* Cluster manager assigns a recovery task to the recovery node and sets it to *Assigned* state.
* On a notification, the recovery node looks at its assigned task, sets its state to ***Recovering***. Then the *RecoveryNode* starts indexing the data.
* Once the recovery task is complete, the ***RecoveryNode*** sets its state to Free and removes the recovery task.

## Basic operation
### Build & deploy
Recovery nodes should be enrolled in an auto-scaling Kubernetes policy, and make use of the CPU utilization to inform scaling
decisions. At least one recovery node must be provisioned at all times, so the cluster can perform scaling as necessary.
The recovery nodes can be increased or decreased in size, and if they are restarted any work-in-progress will be
re-assigned to another node. The recommended CPU utilization to scale on is approximately > 10%.