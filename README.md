# KalDb

Observabilty data consists of 4 kinds of time series data collectively referred by the acronym MELT(Metrics, Logs, Events and Traces). Kaldb aims to unify Events, Logs and Traces(ELT) data under a single system. This unification simplifies data management, reduces data duplication, allows for more powerful and expressive queries and reduces infrastructure costs.

Internally, KalDb stores all the data produced in the `SpanEvent` format. Further, these events can be grouped in causal graphs to represent traces. Storing  events, logs and traces as SpanEvents internally, not only simplifies the data ingestion but also encourages healthy data modelling practices while simplifing querying the data since the data is in a standard format.
