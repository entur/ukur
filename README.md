# ukur
Ukur detects and enable subscriptions for deviations in traffic based on real time information from Anshar.


## Subscriptions
To subscribe, **post** subscription data to https://{BASE_URL}/subscription as `application/json`. 

The subscription must contain a logical name, a public reachable push address and a list of to and from stops:
```json
{
   "name" : "Test subscription",
   "fromStopPoints" : [ "NSR:Quay:551", "NSR:Quay:553", "NSR:Quay:550" ],
   "toStopPoints" : [ "NSR:Quay:695", "NSR:Quay:696" ],
   "pushAddress": "https://myserver/push"   
 }
 ```   
After successfull creation of the new subscription, Ukur responds with the same object with
the id-attribute set. This id can be used to remove the subscription by issuing a http **delete** 
to https://{BASE_URL}/subscription/{subscriptionId}.

StopPoints are fully qualified national ids on stop places and quays, use 
[Stoppestedsregisteret](https://stoppested.entur.org) to look them up. The SIRI
messages received from Anshar uses both stop places and quays to identify affected
stops, so both must be provided to receive all messages regarding a stop (no mapping
between quays and stop places in Ukur). Unknown stops are ignored (as they never will be
referenced). 

### The push endpoint  
Ukur will **post** SIRI data as `application/xml` to the per subscription configured pushAddress after 
adjusting the url somewhat:
- `/et` is added to the push address for Estimated Timetable messages and an EstimatedVehicleJourney is posted.
- `/sx` is added to the push address for Situation Exchange messages and a PtSituationElement is posted.

When data is posted, Ukur expects a 200 response with text/plain and `OK` as response. If 
Ukur posts 4 times in a row for a subscription and receives any other response, the subscription 
is removed. The push endpoint can also respond 200, text/plain and `FORGET_ME` and Ukur will 
remove the subscription instantly.

### When and what data is sent
Ukur polls Anshar for ET and SX data each minute. Currently only ET messaged regarding NSB as operator or SX
messages from NSB is processed - all operators and producers will be processed in the future.

For SX messages, all messages where one or more stops from a subscription is affected is sent (unless the exact same 
message has already been sent). The PtSituationElement will have all other stops removed from Affetcs to make the 
payload smaller, before it is sent to the various subscription endpoints.

For ET messages, the logic is more complex to decide if a message should be pushed. Both a from and a to stop must be 
present in the correct order in an EstimatedVehicleJourney with one of these deviations:
 - DepartureStatus=delayed for an EstimatedCall a subscription has in its from-list
 - ArrivalStatus=delayed for an EstimatedCall a subscription has in its to-list 
 - A subscribed EstimatedCall is marked as cancelled 
The EstimatedVehicleJourney will have stops not subscribed upon removed from RecordedCalls 
and EstimatedCalls to make the payload smaller, before it is sent to the subscriptions push address.

## More info
See [Norsk SIRI Profil](https://rutebanken.atlassian.net/wiki/spaces/PUBLIC/pages/13729888/SIRI+profil+Norge) 
(will be published soon) for more details on the pushmessage payload.
 
Also, see [siri-java-model](https://github.com/entur/siri-java-model) for a java implementation 
of the SIRI model (Ukur uses version 2.0 of the model).
  
Often examples are the best documentation, we have therefore created a simple Java [demo project](https://github.com/entur/ukur-demo)
to show how subscriptions are added/removed and how to receive push messages. 
    
    