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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DataStorageService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Map<String, Set<String>> subscriptionsPerStopPoint;
    private final Map<String, Subscription> subscriptions;
    private final Map<Object, Long> alreadySentCache;
    private IMap<String, LiveJourney> currentJourneys;

    @Autowired
    public DataStorageService(@Qualifier("subscriptionIdsPerStopPoint") Map<String, Set<String>> subscriptionsPerStopPoint,
                              @Qualifier("subscriptions") Map<String, Subscription> subscriptions,
                              @Qualifier("alreadySentCache") Map<Object, Long> alreadySentCache,
                              @Qualifier("currentJourneys") IMap<String, LiveJourney> currentJourneys) {
        this.subscriptionsPerStopPoint = subscriptionsPerStopPoint;
        this.subscriptions = subscriptions;
        this.alreadySentCache = alreadySentCache;
        this.currentJourneys = currentJourneys;
    }

    public int getNumberOfSubscriptions() {
        return subscriptions.size();
    }

    public int getNumberOfUniqueStops() {
        return subscriptionsPerStopPoint.size();
    }

    public Collection<Subscription> getSubscriptions() {
        return subscriptions.values();
    }

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

    public Subscription removeSubscription(String subscriptionId) {
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
        return removed;
    }

    public void updateSubscription(Subscription subscription) {
        subscriptions.put(subscription.getId(), subscription); //to distribute change to other hazelcast nodes
    }

    public Long getAlreadySent(String alreadySentKey) {
        return alreadySentCache.get(alreadySentKey);
    }

    public void putAlreadySent(String alreadySentKey) {
        alreadySentCache.put(alreadySentKey, System.currentTimeMillis());
    }

    public void putCurrentJourney(LiveJourney liveJourney) {
        currentJourneys.set(liveJourney.getVehicleRef(), liveJourney);
    }

    public Collection<LiveJourney> getCurrentJourneys() {
        return currentJourneys.values();
    }

    public void removeCurrentJourney(String vehicleRef) {
        currentJourneys.delete(vehicleRef);
    }
}
