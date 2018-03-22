/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *  https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */

package org.entur.ukur.service;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.collect.Iterators;
import com.hazelcast.core.IMap;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.subscription.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class GoogleDatastoreService implements DataStorageService {

    private static final String KIND_SUBSCRIPTIONS = "Ukur-subscriptions";
    private static final String KIND_STOPPLACES = "Ukur-stopplaces";
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Datastore datastore;
    private final KeyFactory subscriptionkeyFactory;
    private final KeyFactory stopPlacekeyFactory;
    private final IMap<String, LiveJourney> currentJourneys; //TODO: replace with datastore entity with children....

    public GoogleDatastoreService(Datastore datastore,
                                  IMap<String, LiveJourney> currentJourneys) {
        this.datastore = datastore;
        subscriptionkeyFactory = datastore.newKeyFactory().setKind(KIND_SUBSCRIPTIONS);
        stopPlacekeyFactory = datastore.newKeyFactory().setKind(KIND_STOPPLACES);
        this.currentJourneys = currentJourneys;
    }


    @Override
    public Collection<Subscription> getSubscriptions() {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(KIND_SUBSCRIPTIONS)
                .setOrderBy(StructuredQuery.OrderBy.asc("created"))
                .build();
        return convertSubscription(datastore.run(query));
    }

    @Override
    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef) {
        // De to spørringene under kan muligens erstattes med en composite index
        Collection<Subscription> toStopPlaces = convertSubscription(findContaining("toStopPlaces", stopPointRef));
        Collection<Subscription> fromStopPlaces = convertSubscription(findContaining("fromStopPlaces", stopPointRef));
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.addAll(toStopPlaces);
        subscriptions.addAll(fromStopPlaces);
        logger.trace("Found {} unique subscriptions containing '{}' ({} in toStopPlaces and {} in fromStopPlaces)",
                stopPointRef,subscriptions.size(), toStopPlaces.size(), fromStopPlaces.size());
        return subscriptions;
    }

    @Override
    public Set<Subscription> getSubscriptionsForLineRefAndNoStops(String lineRef) {
        Set<Subscription> subscriptions = convertSubscription(findContainingWithoutStops("lineRefs", lineRef));
        logger.trace("Found {} unique subscriptions containing '{}' in lineRefs", lineRef, subscriptions.size());
        return subscriptions;
    }

    @Override
    public Set<Subscription> getSubscriptionsForvehicleRefAndNoStops(String vehicleRef) {
        Set<Subscription> subscriptions = convertSubscription(findContainingWithoutStops("vehicleRefs", vehicleRef));
        logger.trace("Found {} unique subscriptions containing '{}' in vehicleRefs", vehicleRef, subscriptions.size());
        return subscriptions;
    }

    @Override
    public Subscription addSubscription(Subscription subscription) {
        Key key = datastore.allocateId(subscriptionkeyFactory.newKey());
        Entity task = convertEntity(subscription, key);
        //No need for a transaction when adding
        datastore.put(task);
        return convertSubscription(task);
    }

    @Override
    public void removeSubscription(String subscriptionId) {
        datastore.delete(subscriptionkeyFactory.newKey(Long.parseLong(subscriptionId)));
    }

    @Override
    public void updateSubscription(Subscription subscription) {
        Key key = subscriptionkeyFactory.newKey(Long.parseLong(subscription.getId()));
        Entity task = convertEntity(subscription, key);
        Transaction transaction = datastore.newTransaction();
        try {
            transaction.update(task);
            transaction.commit();
        } catch (Exception e) {
            logger.error("Could not update subscription", e);
            transaction.rollback();
        }
    }

    @Override
    public long getNumberOfSubscriptions() {
        //TODO: This query takes forever (at least locally) when there are many subscriptions
        KeyQuery query = Query.newKeyQueryBuilder().setKind(KIND_SUBSCRIPTIONS).build();
        QueryResults<Key> result = datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        return keys.length;
    }

    @Override
    public void putCurrentJourney(LiveJourney liveJourney) {
        currentJourneys.set(liveJourney.getVehicleRef(), liveJourney);
    }

    @Override
    public Collection<LiveJourney> getCurrentJourneys() {
        return currentJourneys.values();
    }

    @Override
    public int getNumberOfCurrentJourneys() {
        return currentJourneys.size();
    }

    @SuppressWarnings("Duplicates") //TODO: This will be replaced with datastore....
    @Override
    public void removeJourneysOlderThan(ZonedDateTime time) {
        Collection<LiveJourney> values = getCurrentJourneys();
        HashSet<String> toFlush = new HashSet<>();
        for (LiveJourney journey : values) {
            if (time.isAfter(journey.getLastArrivalTime())) {
                toFlush.add(journey.getVehicleRef());
            }
        }
        logger.trace("Will flush {} journeys out of a total of {}", toFlush.size(), values.size());
        for (String flush : toFlush) {
            currentJourneys.delete(flush);
        }
    }

    @Override
    public void updateStopsAndQuaysMap(Map<String, Collection<String>> hashMap) {
        ArrayList<Entity> tasks = new ArrayList<>();
        for (Map.Entry<String, Collection<String>> stopPlaceEntry : hashMap.entrySet()) {
            //since reads are cheap and writes are slow (combined with the fact that stopplace data seldom change...) we compare to what we have before anything is written
            Collection<String> storedQuays = mapStopPlaceToQuays(stopPlaceEntry.getKey());
            if (!sameContent(stopPlaceEntry.getValue(), storedQuays)) {
                Entity task = Entity.newBuilder(stopPlacekeyFactory.newKey(stopPlaceEntry.getKey()))
                        .set("quayIds", convertStringsToValueList(stopPlaceEntry.getValue()))
                        .build();
                tasks.add(task);
            }
            if (tasks.size() > 100) {
                datastore.put(tasks.toArray(new Entity[tasks.size()]));
                tasks.clear();
            }
        }
        if (!tasks.isEmpty()) {
            datastore.put(tasks.toArray(new Entity[tasks.size()]));
            tasks.clear();
        }
    }

    private boolean sameContent(Collection<String> collection1, Collection<String> collection2) {
        if (collection1 == null) {
            return collection2 == null || collection2.isEmpty();
        }
        if (collection2 == null) {
            return collection1.isEmpty();
        }
        return collection1.size() == collection2.size() && collection1.containsAll(collection2);
    }

    @Override
    public String mapQuayToStopPlace(String quayId) {
        KeyQuery query = Query.newKeyQueryBuilder()
            .setKind(KIND_STOPPLACES)
            .setFilter(StructuredQuery.PropertyFilter.eq("quayIds", quayId))
            .build();
        QueryResults<Key> queryResults = datastore.run(query);
        if (queryResults.hasNext()) {
            //Expects only one entity
            Key key = queryResults.next();
            return key.getName();
        }
        logger.warn("Did not find quayId '{}' on any stopplace", quayId);
        return null;
    }

    @Override
    public Collection<String> mapStopPlaceToQuays(String stopPlaceId) {
        Entity entity = datastore.get(stopPlacekeyFactory.newKey(stopPlaceId));
        if (entity != null) {
            return convertValueListToStrings(entity, "quayIds");
        }
        logger.warn("Did not find any stopPlace with stopPlaceId '{}'", stopPlaceId);
        return Collections.emptySet();
    }

    @Override
    public long getNumberOfStopPlaces() {
        EntityQuery query = Query.newEntityQueryBuilder()
                .setKind("__Stat_Kind__")
                .setFilter(StructuredQuery.PropertyFilter.eq("kind_name", KIND_STOPPLACES))
                .build();
        try {
            QueryResults<Entity> results = datastore.run(query);
            if (results.hasNext()) {
                Entity entity = results.next();
                return entity.getLong("count");
            }
        } catch (Exception e) {
            logger.error("Could not get size of {}", KIND_STOPPLACES, e);
            return -2;
        }
        return -1; //When running locally __Stat_Kind__ it not generated and we get this
    }

    private QueryResults<Entity> findContaining(String property, String value) {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(KIND_SUBSCRIPTIONS)
                .setFilter(StructuredQuery.PropertyFilter.eq(property, value))
                .build();
        return datastore.run(query);
    }

    private QueryResults<Entity> findContainingWithoutStops(String property, String value) {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(KIND_SUBSCRIPTIONS)
                .setFilter(StructuredQuery.CompositeFilter.and(
                        StructuredQuery.PropertyFilter.eq(property, value),
                        StructuredQuery.PropertyFilter.isNull("fromStopPlaces"),
                        StructuredQuery.PropertyFilter.isNull("toStopPlaces")))
                .build();
        return datastore.run(query);
    }

    private Entity convertEntity(Subscription s, Key key) {
        Entity.Builder builder = Entity.newBuilder(key)
                .set("created", Timestamp.now())
                .set("name", StringValue.newBuilder(s.getName()).setExcludeFromIndexes(true).build())
                .set("pushAddress", StringValue.newBuilder(s.getPushAddress()).setExcludeFromIndexes(true).build())
                .set("failedPushCounter", LongValue.newBuilder(s.getFailedPushCounter()).setExcludeFromIndexes(true).build())
                .set("fromStopPlaces", convertStringsToValueList(s.getFromStopPoints()))
                .set("toStopPlaces", convertStringsToValueList(s.getToStopPoints()))
                .set("lineRefs", convertStringsToValueList(s.getLineRefs()))
                .set("vehicleRefs", convertStringsToValueList(s.getVehicleRefs()));
        return builder.build();
    }

    private Set<Subscription> convertSubscription(Iterator<Entity> subscriptionEntities) {
        HashSet<Subscription> subscriptions = new HashSet<>();
        while (subscriptionEntities.hasNext()) {
            subscriptions.add(convertSubscription(subscriptionEntities.next()));
        }
        return subscriptions;
    }

    private Subscription convertSubscription(Entity entity) {
        Subscription subscription = new Subscription();
        subscription.setId(Long.toString(entity.getKey().getId()));
        subscription.setName(entity.getString("name"));
        subscription.setPushAddress(entity.getString("pushAddress"));
        subscription.setFailedPushCounter(entity.getLong("failedPushCounter"));
        subscription.setFromStopPlaces(convertValueListToStrings(entity, "fromStopPlaces"));
        subscription.setToStopPlaces(convertValueListToStrings(entity, "toStopPlaces"));
        subscription.setLineRefs(convertValueListToStrings(entity, "lineRefs"));
        subscription.setVehicleRefs(convertValueListToStrings(entity, "vehicleRefs"));
        return subscription;
    }

    private List<StringValue> convertStringsToValueList(Collection<String> stops) {
        return stops.stream().map(StringValue::of).collect(Collectors.toList());
    }

    private Set<String> convertValueListToStrings(Entity entity, String property) {
        if (entity.contains(property)) {
            List<StringValue> values = entity.getList(property);
            if (values != null) {
                return values.stream().map(StringValue::get).collect(Collectors.toSet());
            }
        }
        return Collections.emptySet();
    }

}
