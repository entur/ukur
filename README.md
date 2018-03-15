# ukur
Ukur detects and enable subscriptions for deviations in traffic based on real time information from Anshar.


## Subscriptions
To subscribe, **post** subscription data to https://{BASE_URL}/subscription as `application/json`. 

The subscription must contain a logical name, a public reachable push address and a list of to and from stops:
```json
{
   "name" : "Test subscription",
   "pushAddress": "https://myserver/push",
   "fromStopPoints" : [ "NSR:Quay:551", "NSR:Quay:553", "NSR:Quay:550" ],
   "toStopPoints" : [ "NSR:Quay:695", "NSR:Quay:696" ],
   "lineRefs" : ["NSB:Line:L14", "NSB:Line:R11"],
   "vehicleRefs" : ["504", "806"]
 }
 ```   
After successfull creation of the new subscription, Ukur responds with the same object with
the id-attribute set. This id can be used to remove the subscription by issuing a http **delete** 
to https://{BASE_URL}/subscription/{subscriptionId}.

StopPoints are fully qualified national ids on stop places and quays, use 
[Stoppestedsregisteret](https://stoppested.entur.org) to look them up. The SIRI
messages received from Anshar uses both stop places and quays to identify affected
stops, so both must be provided to receive all messages regarding a stop (no mapping
between quays and stop places in Ukur - yet...). Stops not following the national id format 
are ignored (as they never will be referenced). Also both from and to StopPoints must be 
present to receive push messages.

LineRefs and vehicleRefs are used to subscribe on entire lines, vehicles or limit from-to 
messages to just those regarding one or more lines and/or vehicles. 

StopPoints (fromStopPoints and toStopPoints is treated as one group), lineRefs and vehicleRefs
is combined to an AND criteria. But we only require one 'hit' from each of them, so inside them
the values are treated as an OR criteria. The json example above will result in a pushmessage only 
if it involves a stop from ("NSR:Quay:551" OR "NSR:Quay:553" OR "NSR:Quay:550") AND to ("NSR:Quay:695" OR
"NSR:Quay:696") AND lineRefs is ("NSB:Line:L14" OR "NSB:Line:R11"]) AND vehicleRefs is ("504" OR "806").


### The push endpoint  
Ukur will **post** SIRI data as `application/xml` to the per subscription configured pushAddress after 
adjusting the url somewhat:
- `/et` is added to the push address for Estimated Timetable messages and an EstimatedVehicleJourney is posted.
- `/sx` is added to the push address for Situation Exchange messages and a PtSituationElement is posted.

When data is posted, Ukur expects a 200 response. If Ukur posts 4 times in a row for a subscription and
receives any other response, the subscription is removed. The push endpoint can also respond 205 
(RESET-CONTENT) and Ukur will remove the subscription instantly.

### When and what data is sent
Ukur polls Anshar for ET and SX data each minute. Currently only ET messaged regarding NSB as operator or SX
messages from NSB is processed - all operators and producers will be processed in the future.

For **SX messages** that reference a VehicleJourney, we attempt to use a route table based on ET messages to 
determine if the message regards a subscription (correct direction, line, etc) or not. Other SX messages only 
regards stops and is sent to affected subscriptions (unless the exact same message has already been sent). 
For subscriptions that contains stops, the PtSituationElement will have all other stops removed from Affects 
to make the payload smaller, before it is sent to the various subscription endpoints. We will also remove
affected journeys not matching the subscriptions constraint on lines and vehicles.

For **ET messages**, the logic is more complex to decide if a message should be pushed. Both a from and a to 
stop must be present in the correct order in an EstimatedVehicleJourney with one of these deviations:
 - DepartureStatus=delayed for an EstimatedCall a subscription has in its from-list
 - ArrivalStatus=delayed for an EstimatedCall a subscription has in its to-list 
 - A subscribed EstimatedCall is marked as cancelled 
When a subscription has stops, the EstimatedVehicleJourney will have stops not subscribed upon removed 
from RecordedCalls and EstimatedCalls to make the payload smaller, before it is sent to the subscriptions 
push address. If only lineRefs and/or vehicleRefs are present in a subscription, the entire EstimatedVehicleJourney
will be pushed.

## More info
See [Norsk SIRI Profil](https://rutebanken.atlassian.net/wiki/spaces/PUBLIC/pages/13729888/SIRI+profil+Norge) 
(will be published soon) for more details on the pushmessage payload.
 
Also, see [siri-java-model](https://github.com/entur/siri-java-model) for a java implementation 
of the SIRI model (Ukur uses version 2.0 of the model).
  
Often examples are the best documentation, we have therefore created a simple Java [demo project](https://github.com/entur/ukur-demo)
to show how subscriptions are added/removed and how to receive push messages. 
    
    