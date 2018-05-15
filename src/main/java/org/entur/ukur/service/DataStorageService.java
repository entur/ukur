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
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataStorageService {

    private static final String KIND_SUBSCRIPTIONS = "Ukur-subscriptions";
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Datastore datastore;
    private final KeyFactory subscriptionkeyFactory;
    private final IMap<String, LiveJourney> currentJourneys;

    public DataStorageService(Datastore datastore,
                              IMap<String, LiveJourney> currentJourneys) {
        this.datastore = datastore;
        subscriptionkeyFactory = datastore.newKeyFactory().setKind(KIND_SUBSCRIPTIONS);
        this.currentJourneys = currentJourneys;
    }


    public Collection<Subscription> getSubscriptions() {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(KIND_SUBSCRIPTIONS)
                .setOrderBy(StructuredQuery.OrderBy.asc("created"))
                .build();
        return convertSubscription(datastore.run(query));
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef, SubscriptionTypeEnum type) {
        // De to spørringene under kan muligens erstattes med en composite index
        Collection<Subscription> toStopPlaces = convertSubscription(findContaining("toStopPlaces", stopPointRef));
        Collection<Subscription> fromStopPlaces = convertSubscription(findContaining("fromStopPlaces", stopPointRef));
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.addAll(toStopPlaces);
        subscriptions.addAll(fromStopPlaces);
        //TODO: This should be done in the query (done like this now since we don't want to update existing subscriptions yet)
        subscriptions.removeIf(notMatchingType(type));
        logger.trace("Found {} unique subscriptions containing '{}' ({} in toStopPlaces and {} in fromStopPlaces)",
                stopPointRef,subscriptions.size(), toStopPlaces.size(), fromStopPlaces.size());
        return subscriptions;
    }

    public Set<Subscription> getSubscriptionsForLineRefAndNoStops(String lineRef, SubscriptionTypeEnum type) {
        Set<Subscription> subscriptions = convertSubscription(findContainingWithoutStops("lineRefs", lineRef));
        //TODO: This should be done in the query (done like this now since we don't want to update existing subscriptions yet)
        subscriptions.removeIf(notMatchingType(type));
        logger.trace("Found {} unique subscriptions containing '{}' in lineRefs", subscriptions.size(), lineRef);
        return subscriptions;
    }

    public Set<Subscription> getSubscriptionsForCodespaceAndNoStops(String codespace, SubscriptionTypeEnum type) {
        Set<Subscription> subscriptions = convertSubscription(findContainingWithoutStops("codespaces", codespace));
        //TODO: This should be done in the query (done like this now since we don't want to update existing subscriptions yet)
        subscriptions.removeIf(notMatchingType(type));
        logger.trace("Found {} unique subscriptions containing '{}' in codespaces", subscriptions.size(), codespace);
        return subscriptions;
    }

    private Predicate<Subscription> notMatchingType(SubscriptionTypeEnum type) {
        return s -> s.getType() != SubscriptionTypeEnum.ALL && s.getType() != type;
    }

    public Subscription addSubscription(Subscription subscription) {
        Key key = datastore.allocateId(subscriptionkeyFactory.newKey());
        Entity task = convertEntity(subscription, key);
        //No need for a transaction when adding
        datastore.put(task);
        return convertSubscription(task);
    }

    public void removeSubscription(String subscriptionId) {
        datastore.delete(subscriptionkeyFactory.newKey(Long.parseLong(subscriptionId)));
    }

    public boolean updateSubscription(Subscription subscription) {
        Key key = subscriptionkeyFactory.newKey(Long.parseLong(subscription.getId()));
        Entity task = convertEntity(subscription, key);
        Transaction transaction = datastore.newTransaction();
        try {
            transaction.update(task);
            transaction.commit();
        } catch (Exception e) {
            logger.error("Could not update subscription", e);
            transaction.rollback();
            return false;
        }
        return true;
    }

    public long getNumberOfSubscriptions() {
        //TODO: This query takes forever (at least locally) when there are many subscriptions
        KeyQuery query = Query.newKeyQueryBuilder().setKind(KIND_SUBSCRIPTIONS).build();
        QueryResults<Key> result = datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        return keys.length;
    }

    public void putCurrentJourney(LiveJourney liveJourney) {
        currentJourneys.set(liveJourney.getVehicleRef(), liveJourney);
    }

    public Collection<LiveJourney> getCurrentJourneys() {
        return currentJourneys.values();
    }

    public int getNumberOfCurrentJourneys() {
        try {
            return currentJourneys.size();
        } catch (Exception e) {
            logger.warn("Could not get currentJourneys' size - returns -1", e);
            return -1;
        }
    }

    @SuppressWarnings("Duplicates") //TODO: This will be replaced with datastore....
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
                .set("failedPushCounter", LongValue.newBuilder(s.getFailedPushCounter()).setExcludeFromIndexes(true).build());
        appendStringValueList(builder, "fromStopPlaces", s.getFromStopPoints());
        appendStringValueList(builder, "toStopPlaces", s.getToStopPoints());
        appendStringValueList(builder, "lineRefs", s.getLineRefs());
        appendStringValueList(builder, "codespaces", s.getCodespaces());
        appendStringValueList(builder, "types", toNameList(s.getType()));
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
        subscription.setCodespaces(convertValueListToStrings(entity, "codespaces"));
        subscription.setType(toTypeEnum(convertValueListToStrings(entity, "types")));
        return subscription;
    }

    private SubscriptionTypeEnum toTypeEnum(Set<String> types) {
        if (types.isEmpty() || types.contains(SubscriptionTypeEnum.ALL.name())) {
            return SubscriptionTypeEnum.ALL;
        }
        if (types.size() > 1) {
            logger.warn("There is a Subscription entity with more than one value in 'types' without ALL (we just pick one). Values: {}", types.toArray());
        }
        return SubscriptionTypeEnum.valueOf(types.iterator().next());
    }

    /**
     * Since Datastore does not support OR in queries, we store all values in a list. And since we also store ALL,
     * we are able to update subscriptions as we add more types.
     */
    private Collection<String> toNameList(SubscriptionTypeEnum type) {
        if (type == null || type == SubscriptionTypeEnum.ALL) {
            return Arrays.stream(SubscriptionTypeEnum.values()).map(Enum::name).collect(Collectors.toList());
        } else {
            return Collections.singletonList(type.name());
        }
    }

    private void appendStringValueList(Entity.Builder builder, String name, Collection<String> stops) {
        List<StringValue> values = stops.stream().filter(StringUtils::isNotBlank).map(StringValue::of).collect(Collectors.toList());
        if (values.isEmpty()) {
            builder.set(name, NullValue.of());
        } else {
            builder.set(name, values);
        }
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
