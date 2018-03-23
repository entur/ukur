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
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Datastore datastore;
    private final KeyFactory subscriptionkeyFactory;
    private final IMap<String, LiveJourney> currentJourneys;

    public GoogleDatastoreService(Datastore datastore,
                                  IMap<String, LiveJourney> currentJourneys) {
        this.datastore = datastore;
        subscriptionkeyFactory = datastore.newKeyFactory().setKind(KIND_SUBSCRIPTIONS);
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
