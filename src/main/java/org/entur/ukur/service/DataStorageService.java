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
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.Transaction;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DataStorageService implements MessageListener<String> {

    private static final String KIND_SUBSCRIPTIONS = "Ukur-subscriptions";
    private static final String SUBSCRIPTION_ACTION_ADDED = "ADDED";
    private static final String SUBSCRIPTION_ACTION_UPDATED = "UPDATED";
    private static final String SUBSCRIPTION_ACTION_REMOVED = "REMOVED";
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Datastore datastore;
    private final KeyFactory subscriptionkeyFactory;
    private ITopic<String> subscriptionCacheRenewerTopic;

    private HashMap<String, Subscription> idToSubscription = new HashMap<>();
    private HashMap<String, Set<String>> stopToSubscription = new HashMap<>();
    private HashMap<String, Set<String>> lineNoStopsToSubscription = new HashMap<>();
    private HashMap<String, Set<String>> codespaceNoStopsToSubscription = new HashMap<>();
    private long lastReloadedTime = 0;
    private static final int CONCURRENTMODIFICATION_ATTEMPTS = 3;
    private final String serviceId = UUID.randomUUID().toString();
    public DataStorageService(Datastore datastore, ITopic<String> subscriptionCacheRenewerTopic) {
        this.datastore = datastore;
        this.subscriptionkeyFactory = datastore.newKeyFactory().setKind(KIND_SUBSCRIPTIONS);
        this.subscriptionCacheRenewerTopic = subscriptionCacheRenewerTopic;
        this.subscriptionCacheRenewerTopic.addMessageListener(this);
        //To support that subscriptions are changed from the console (or we get out of sync...)
        Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> {
            try {
                populateSubscriptionCacheFromDatastore();
                logger.debug("Reloads subscriptions from datastore");
            } catch (Exception e) {
                logger.error("Got excetption while reloading subscriptions from datastore", e);
            }
        }, 1, 1, TimeUnit.HOURS);
    }

    @PostConstruct
    public void populateSubscriptionCacheFromDatastore() {
        populateSubscriptionCacheFromDatastore(System.currentTimeMillis());
    }

    private void populateSubscriptionCacheFromDatastore(long time) {
        lastReloadedTime = time;
        //it turned out datastore wasn't suited to our subscription needs - now we simply use it as persistence (maybe not the best usage of datatore...)
        //TODO: This aproach will not scale with massive amounts of subscriptions.... But it doesn't seem like we will have many anyway as we don't deal with clients directly
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(KIND_SUBSCRIPTIONS)
                .build();
        Set<Subscription> subscriptions = convertSubscription(datastore.run(query));
        HashMap<String, Subscription> idToSubscription = new HashMap<>();
        HashMap<String, Set<String>> stopToSubscription = new HashMap<>();
        HashMap<String, Set<String>> lineNoStopsToSubscription = new HashMap<>();
        HashMap<String, Set<String>> codespaceNoStopsToSubscription = new HashMap<>();
        for (Subscription subscription : subscriptions) {
            addOrUpdateSubscriptionInLocalStorage(idToSubscription, stopToSubscription, lineNoStopsToSubscription, codespaceNoStopsToSubscription, subscription);
        }
        updateSubscriptionCache(idToSubscription, stopToSubscription, lineNoStopsToSubscription, codespaceNoStopsToSubscription);
    }

    private void addOrUpdateSubscriptionInLocalStorage(HashMap<String, Subscription> idToSubscription, HashMap<String, Set<String>> stopToSubscription, HashMap<String, Set<String>> lineNoStopsToSubscription, HashMap<String, Set<String>> codespaceNoStopsToSubscription, Subscription subscription) {
        idToSubscription.put(subscription.getId(), subscription);
        if (subscription.hasNoStops()) {
            for (String lineref : subscription.getLineRefs()) {
                add(subscription, lineref, lineNoStopsToSubscription);
            }
            for (String codespace : subscription.getCodespaces()) {
                add(subscription, codespace, codespaceNoStopsToSubscription);
            }
        } else {
            HashSet<String> stops = new HashSet<>();
            stops.addAll(subscription.getFromStopPoints());
            stops.addAll(subscription.getToStopPoints());
            for (String stop : stops) {
                add(subscription, stop, stopToSubscription);
            }
        }
    }

    private void addOrUpdateSubscriptionInLocalStorage(Subscription subscription) {
        addOrUpdateSubscriptionInLocalStorage(idToSubscription, stopToSubscription, lineNoStopsToSubscription, codespaceNoStopsToSubscription, subscription);
    }

    private void removeSubscriptionFromLocalStorage(String subscriptionId) {
        idToSubscription.remove(subscriptionId);
    }

    private synchronized void updateSubscriptionCache(HashMap<String, Subscription> idToSubscription, HashMap<String, Set<String>> stopToSubscription,
                                                      HashMap<String, Set<String>> lineNoStopsToSubscription, HashMap<String, Set<String>> codespaceNoStopsToSubscription) {
        this.idToSubscription  = idToSubscription;
        this.stopToSubscription  = stopToSubscription;
        this.lineNoStopsToSubscription  = lineNoStopsToSubscription;
        this.codespaceNoStopsToSubscription  = codespaceNoStopsToSubscription;
    }


    public Collection<Subscription> getSubscriptions() {
        for (int i = 0; i< CONCURRENTMODIFICATION_ATTEMPTS; i++) {
            try {
                return new HashSet<>(idToSubscription.values());
            } catch (ConcurrentModificationException e) {
                //will attempt several times
                logger.debug("Got an ConcurrentModificationException while getting all subscriptions");
            }
        }
        logger.error("Could not get all subscriptions due to ConcurrentModificationExceptions - attempted {} times", CONCURRENTMODIFICATION_ATTEMPTS);
        return Collections.emptySet();
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef, SubscriptionTypeEnum type) {
        Set<String> subscriptionIds = getSubscriptionIds(stopPointRef, stopToSubscription);
        Set<Subscription> subscriptions = getSubscriptions(subscriptionIds, type);
        logger.trace("Found {} unique subscriptions containing '{}' in to/from stops",stopPointRef, subscriptions.size());
        return subscriptions;
    }

    public Set<Subscription> getSubscriptionsForLineRefAndNoStops(String lineRef, SubscriptionTypeEnum type) {
        Set<String> subscriptionIds = getSubscriptionIds(lineRef, lineNoStopsToSubscription);
        Set<Subscription> subscriptions = getSubscriptions(subscriptionIds, type);
        logger.trace("Found {} unique subscriptions containing '{}' in lineRefs", subscriptions.size(), lineRef);
        return subscriptions;
    }

    public Set<Subscription> getSubscriptionsForCodespaceAndNoStops(String codespace, SubscriptionTypeEnum type) {
        Set<String> subscriptionIds = getSubscriptionIds(codespace, codespaceNoStopsToSubscription);
        Set<Subscription> subscriptions = getSubscriptions(subscriptionIds, type);
        logger.trace("Found {} unique subscriptions containing '{}' in codespaces", subscriptions.size(), codespace);
        return subscriptions;
    }

    private HashSet<String> getSubscriptionIds(String key, HashMap<String, Set<String>> set) {
        for (int i = 0; i< CONCURRENTMODIFICATION_ATTEMPTS; i++) {
            try {
                return new HashSet<>(set.getOrDefault(key, Collections.emptySet()));
            } catch (ConcurrentModificationException e) {
                //will attempt several times
                logger.debug("Got an ConcurrentModificationException while getting all subscriptions");
            }
        }
        logger.error("Could not get all subscriptionIds due to ConcurrentModificationExceptions - attempted {} times", CONCURRENTMODIFICATION_ATTEMPTS);
        return new HashSet<>();
    }

    private Set<Subscription> getSubscriptions(Set<String> subscriptionIds, SubscriptionTypeEnum type) {
        HashSet<Subscription> subscriptions = new HashSet<>(subscriptionIds.size());
        for (Iterator<String> idIterator = subscriptionIds.iterator(); idIterator.hasNext(); ) {
            String id = idIterator.next();
            Subscription subscription = idToSubscription.get(id);
            if (subscription == null) {
                //this happens quite often (after a subscription is removed on an other pod until the periodic populateSubscriptionCacheFromDatastore)
                idIterator.remove();
            } else if (subscription.getType() == SubscriptionTypeEnum.ALL || subscription.getType() == type){
                subscriptions.add(subscription);
            }
        }
        return subscriptions;
    }

    public Subscription addSubscription(Subscription subscription) {
        Key key = datastore.allocateId(subscriptionkeyFactory.newKey());
        Entity task = convertEntity(subscription, key);
        //No need for a transaction when adding
        datastore.put(task);
        subscription = convertSubscription(task);
        addOrUpdateSubscriptionInLocalStorage(subscription);
        logger.info("Added subscription with id {}", subscription.getId());
        publish(SUBSCRIPTION_ACTION_ADDED, subscription.getId());
        return subscription;
    }

    public void removeSubscription(String subscriptionId) {
        datastore.delete(subscriptionkeyFactory.newKey(Long.parseLong(subscriptionId)));
        removeSubscriptionFromLocalStorage(subscriptionId);
        publish(SUBSCRIPTION_ACTION_REMOVED, subscriptionId);
    }

    public boolean updateSubscription(Subscription subscription) {
        Key key = subscriptionkeyFactory.newKey(Long.parseLong(subscription.getId()));
        Entity task = convertEntity(subscription, key);
        Transaction transaction = datastore.newTransaction();
        try {
            transaction.update(task);
            transaction.commit();
        } catch (Exception e) {
            logger.warn("Could not update subscription", e);
            transaction.rollback();
            return false;
        }
        addOrUpdateSubscriptionInLocalStorage(idToSubscription, stopToSubscription, lineNoStopsToSubscription, codespaceNoStopsToSubscription, subscription);
        publish(SUBSCRIPTION_ACTION_UPDATED, subscription.getId());
        return true;
    }

    public long getNumberOfSubscriptions() {
        return idToSubscription.size();
    }

    public Subscription getSubscriptionByName(String name) {
        if (StringUtils.isNotBlank(name)) {
            //TODO: won't scale to well, but sufficient for now
            for (Subscription subscription : idToSubscription.values()) {
                if (name.equals(subscription.getName())) {
                    return subscription;
                }
            }
        }
        return null;
    }


    private Entity convertEntity(Subscription s, Key key) {
        Entity.Builder builder = Entity.newBuilder(key)
                .set("created", Timestamp.now())
                .set("name", StringValue.newBuilder(s.getName()).setExcludeFromIndexes(true).build())
                .set("pushAddress", StringValue.newBuilder(s.getPushAddress()).setExcludeFromIndexes(true).build())
                .set("failedPushCounter", LongValue.newBuilder(s.getFailedPushCounter()).setExcludeFromIndexes(true).build())
                .set("pushAllData", BooleanValue.of(s.isPushAllData()))
                .set("siriSubscriptionModel", BooleanValue.of(s.isUseSiriSubscriptionModel()));
        if (s.getHeartbeatInterval() != null) {
            builder.set("heartbeatInterval", StringValue.of(s.getHeartbeatInterval().toString()));
        }
        if (s.getInitialTerminationTime() != null) {
            Date date = Date.from(s.getInitialTerminationTime().toInstant());
            builder.set("initialTerminationTime", Timestamp.of(date));
        }
        if (s.getFirstErrorSeen() != null) {
            Date date = Date.from(s.getFirstErrorSeen().toInstant());
            builder.set("firstErrorSeen", Timestamp.of(date));
        }
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
        if (entity.contains("siriSubscriptionModel")) {
            subscription.setUseSiriSubscriptionModel(entity.getBoolean("siriSubscriptionModel"));
        }
        if (entity.contains("pushAllData")) {
            subscription.setPushAllData(entity.getBoolean("pushAllData"));
        }
        if (entity.contains("heartbeatInterval")) {
            String heartbeatInterval = entity.getString("heartbeatInterval");
            subscription.setHeartbeatInterval(toDuration(heartbeatInterval));
        }
        if (entity.contains("initialTerminationTime")) {
            Timestamp initialTerminationTime = entity.getTimestamp("initialTerminationTime");
            subscription.setInitialTerminationTime(ZonedDateTime.ofInstant(initialTerminationTime.toSqlTimestamp().toInstant(), ZoneId.systemDefault()));
        }
        if (entity.contains("firstErrorSeen")) {
            Timestamp firstErrorSeen = entity.getTimestamp("firstErrorSeen");
            subscription.setFirstErrorSeen(ZonedDateTime.ofInstant(firstErrorSeen.toSqlTimestamp().toInstant(), ZoneId.systemDefault()));
        }
        return subscription;
    }

    private Duration toDuration(String heartbeatInterval) {
        try {
            DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
            return datatypeFactory.newDuration(heartbeatInterval);
        } catch (Exception e) {
            logger.error("Can't convert '{}' to a Duration instance", heartbeatInterval);
            return null;
        }
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

    private void add(Subscription subscription, String key, HashMap<String, Set<String>> map) {
        Set<String> subscriptions = map.getOrDefault(key, new HashSet<>());
        subscriptions.add(subscription.getId());
        map.put(key, subscriptions);
    }

    private void publish(String action, String subscriptionId) {
        String message = action + " " + subscriptionId + " " + serviceId;
        logger.debug("Publish '{}' on subscriptionCacheRenewerTopic", message);
        subscriptionCacheRenewerTopic.publish(message);
    }

    @Override
    public void onMessage(Message<String> message) {

        String messageString = message.getMessageObject();
        logger.debug("Received message: {}", messageString);
        if (lastReloadedTime < message.getPublishTime()) {
            String[] msgParts = messageString.split(" ");
            if (msgParts.length < 2 || msgParts.length > 3) {
                logger.warn("Received message on unexpected format (<ACTION> <SUBSCRIPTION-ID> <SERVICEID (optional)>): {}", messageString);
            } else {
                String action = msgParts[0];
                String subscriptionId = msgParts[1];
                if (msgParts.length == 3) {
                    if (this.serviceId.equals(msgParts[2])) {
                        //Could use message.getPublishingMember(), but it's a little tricky both to test and retrieve...
                        logger.debug("Ignores message as this service was the sender");
                        return;
                    }
                }
                switch (action) {
                    case SUBSCRIPTION_ACTION_ADDED:
                    case SUBSCRIPTION_ACTION_UPDATED:
                        try {
                            Key key = subscriptionkeyFactory.newKey(Long.parseLong(subscriptionId));
                            Entity entity = datastore.get(key);
                            if (entity != null) {
                                Subscription subscription = convertSubscription(entity);
                                addOrUpdateSubscriptionInLocalStorage(subscription);
                            } else {
                                logger.warn("Did not find a subscription in Datastore to add/update based on this message: {}", messageString);
                            }
                        } catch (Exception e) {
                            logger.warn("Could not add or update subscription", e);
                        }
                        break;
                    case SUBSCRIPTION_ACTION_REMOVED:
                        removeSubscriptionFromLocalStorage(subscriptionId);
                        break;
                    default:
                        logger.warn("Received message with unknown action '{}', message: {}", action, messageString);
                }
            }
        } else {
            logger.debug("Ignores reload as received message PublishTime ({}) is older than or equal to the last reload ({})", message.getPublishTime(), lastReloadedTime);
        }
    }
}
