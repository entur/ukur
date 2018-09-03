# Ukur - Technical documentation

The details are in the code...! But hopefully it helps with a quick overview of the main architecture and its technical components:


## Main components

Ukur is a Spring Boot application with Apache Camel routes to handle incoming SIRI messages and subscription requests
and various periodic tasks like managing the connectivity with Anshar (the real-time hub). An external ActiveMQ server
(cluster) is used in the Camel routes to distribute load between nodes (as Ukur is intended to run in a cluster with 
multiple nodes). Hazelcast is used to organize which cluster-member that is master of the different periodic tasks, as 
well as to hold shared configuration and to distribute subscription-updates between nodes. Finally Googles Datastore 
is used to persist subscriptions, but as querying the datastore is rather slow each node holds all subscriptions in 
memory for fast handling.

The class org.entur.ukur.camelroute.UkurCamelRouteBuilder is responsible for setting up the camel routes. It is started
by spring using standard annotations. The class that starts everything is org.entur.ukur.App. The handling of SIRI messages 
consumes lots of memory, and the pods have been tested to require about 3.5 gb of heap (-Xmx3500m).


## Deployment

A docker image is built using fabric8 and friends. This image is deployed to Google Cloud with service and deployment
configurations from a different environment git-repo. Normally Ukur is deployed with two pods.

There are liveness and readiness probes set up to the corresponding endpoints in camel (internal/health/live and 
internal/health/ready). P.t. the liveness probe simply responds 200 without any further checking, the readiness probe 
responds 200 as soon as stopplaces has been retrieved from Tiamat once.

Also the api gateway is set up to route external calls to the external context, so that internal urls like health-checks
and subscription-sync paths are inaccessible from the outside. 


## Monitoring

There are a number of metrics gathered by Ukur, and they are periodically pushed to Graphite. See org.entur.ukur.service.MetricsService
for details on how that is set up.
Also there is a separate statistics route/http-call that is useful to see details of a particular pod/node:
http://localhost:8080/internal/health/routes


## Things to be aware of

- Since Ukur uses Hazelcast for shared information, one must be careful when modifying objects that is put on a distributed
collection: All pods must be taken down before the change is deployed.

- The subscription-handling is a little bit complex: All nodes have all subscriptions in memory in various maps. This makes
it fast to find subscriptions matching a SIRI message, but since all nodes/pods must be in sync it is also a little bit complicated.
The nodes uses a Hazelcast topic to inform each other about modification to a subscriptions, and the receiving nodes read that 
specific subscription from datastore to get the last version (or just deletes it from the local cache). If nodes get out of
sync, the damage is limited as the subscription handling allows for that. Also each node will read all subscriptions from
datastore at a fixed rate (p.t. hourly) to make sure two nodes never will have very different versions of the subscriptions.  

