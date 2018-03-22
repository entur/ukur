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

import com.hazelcast.core.IMap;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.subscription.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;

public class DataStorageHazelcastService implements DataStorageService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Map<String, Set<String>> subscriptionsPerStopPoint;
    private final Map<String, Subscription> subscriptions;
    private IMap<String, LiveJourney> currentJourneys;
    private Map<String, Collection<String>> stopPlaceIdToQuayIds;
    private Map<String, String> quayIdToStopPlaceId;

    public DataStorageHazelcastService(Map<String, Set<String>> subscriptionsPerStopPoint,
                                       Map<String, Subscription> subscriptions,
                                       IMap<String, LiveJourney> currentJourneys,
                                       Map<String, Collection<String>> stopPlaceIdToQuayIds,
                                       Map<String, String> quayIdToStopPlaceId) {
        this.subscriptionsPerStopPoint = subscriptionsPerStopPoint;
        this.subscriptions = subscriptions;
        this.currentJourneys = currentJourneys;
        this.stopPlaceIdToQuayIds = stopPlaceIdToQuayIds;
        this.quayIdToStopPlaceId = quayIdToStopPlaceId;
    }


    @Override
    public Collection<Subscription> getSubscriptions() {
        return subscriptions.values();
    }

    @Override
    public long getNumberOfSubscriptions() {
        return subscriptions.size();
    }

    @Override
    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef) {
        Set<String> subscriptionIds = subscriptionsPerStopPoint.get(stopPointRef);
        if (subscriptionIds == null) {
            return Collections.emptySet();
        }
        HashSet<Subscription> result = new HashSet<>(subscriptionIds.size());
        for (String subscriptionId : subscriptionIds) {
            Subscription subscription = subscriptions.get(subscriptionId);
            if (subscription != null) {
                result.add(subscription);
            }
        }
        return result;
    }

    @Override
    public Set<Subscription> getSubscriptionsForLineRefAndNoStops(String lineRef) {
        if (lineRef == null) {
            throw new IllegalArgumentException("Null not allowed");
        }
        HashSet<Subscription> subscriptionsForLineRef = new HashSet<>();
        //Simply iterates all subscriptions since it's the datastore implementation we expect to use in production
        for (Subscription subscription : subscriptions.values()) {
            if (subscription.getLineRefs().contains(lineRef) && subscription.hasNoStops() ) {
                subscriptionsForLineRef.add(subscription);
            }
        }
        return subscriptionsForLineRef;
    }

    @Override
    public Set<Subscription> getSubscriptionsForvehicleRefAndNoStops(String vehicleRef) {
        if (vehicleRef == null) {
            throw new IllegalArgumentException("Null not allowed");
        }
        HashSet<Subscription> subscriptionsForVehicleRef = new HashSet<>();
        //Simply iterates all subscriptions since it's the datastore implementation we expect to use in production
        for (Subscription subscription : subscriptions.values()) {
            if (subscription.getVehicleRefs().contains(vehicleRef) && subscription.hasNoStops()) {
                subscriptionsForVehicleRef.add(subscription);
            }
        }
        return subscriptionsForVehicleRef;
    }

    @Override
    public Subscription addSubscription(Subscription s) {
        String id = UUID.randomUUID().toString();
        while (subscriptions.containsKey(id)) {
            logger.warn("Not really possible: New randomUUID already exists (!) - generates a new one....");
            id = UUID.randomUUID().toString();
        }
        s.setId(id);
        subscriptions.put(id, s);
        HashSet<String> subscribedStops = new HashSet<>();
        subscribedStops.addAll(s.getFromStopPoints());
        subscribedStops.addAll(s.getToStopPoints());
        for (String stoppoint : subscribedStops) {
            stoppoint = stoppoint.trim();// //TODO: consider make usage of these ids case insensitive, maybe using .toUpperCase();
            Set<String> subscriptions = subscriptionsPerStopPoint.get(stoppoint);
            if (subscriptions == null) {
                subscriptions = new HashSet<>();
            }
            subscriptions.add(s.getId());
            subscriptionsPerStopPoint.put(stoppoint, subscriptions);//cause of hazelcast
        }
        return s;
    }

    @Override
    public void removeSubscription(String subscriptionId) {
        Subscription removed = subscriptions.remove(subscriptionId);
        if (removed != null) {
            HashSet<String> subscribedStops = new HashSet<>();
            subscribedStops.addAll(removed.getFromStopPoints());
            subscribedStops.addAll(removed.getToStopPoints());
            logger.debug("Also removes the subscription from these stops: {}", subscribedStops);
            for (String stoppoint : subscribedStops) {
                Set<String> subscriptions = subscriptionsPerStopPoint.get(stoppoint);
                if (subscriptions != null) {
                    subscriptions.remove(removed.getId());
                    subscriptionsPerStopPoint.put(stoppoint, subscriptions); //cause of hazelcast
                }
            }
        }
    }

    @Override
    public void updateSubscription(Subscription subscription) {
        subscriptions.put(subscription.getId(), subscription); //to distribute change to other hazelcast nodes
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

    @Override
    public void removeJourneysOlderThan(ZonedDateTime time) {
        Collection<LiveJourney> values = getCurrentJourneys();
        HashSet<String> toFlush = new HashSet<>();
        for (LiveJourney journey : values) {
            if (time.isAfter(journey.getLastArrivalTime())) {
                toFlush.add(journey.getVehicleRef());
            }
        }
        logger.debug("Will flush {} journeys out of a total of {}", toFlush.size(), values.size());
        for (String flush : toFlush) {
            currentJourneys.delete(flush);
        }
    }

    @Override
    public void updateStopsAndQuaysMap(Map<String, Collection<String>> hashMap) {
        stopPlaceIdToQuayIds.clear();
        stopPlaceIdToQuayIds.putAll(hashMap);
        quayIdToStopPlaceId.clear();
        for (Map.Entry<String, Collection<String>> stopAndQuays : hashMap.entrySet()) {
            for (String quayId : stopAndQuays.getValue()) {
                quayIdToStopPlaceId.put(quayId, stopAndQuays.getKey());
            }
        }
    }

    @Override
    public String mapQuayToStopPlace(String quayId) {
        String stopPlaceid = quayIdToStopPlaceId.get(quayId);
        if (stopPlaceid == null) {
            logger.warn("Did not find quayId '{}' on any stopplace", quayId);
        }
        return stopPlaceid;
    }

    @Override
    public Collection<String> mapStopPlaceToQuays(String stopPlaceId) {
        Collection<String> quayIds = stopPlaceIdToQuayIds.get(stopPlaceId);
        if (quayIds == null) {
            logger.warn("Did not find any stopPlace with stopPlaceId '{}'", stopPlaceId);
        }
        return quayIds;
    }

    @Override
    public long getNumberOfStopPlaces() {
        return stopPlaceIdToQuayIds.size();
    }

}
