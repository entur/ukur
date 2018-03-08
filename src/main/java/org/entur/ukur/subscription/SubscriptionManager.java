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

package org.entur.ukur.subscription;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import uk.org.siri.siri20.*;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.entur.ukur.subscription.PushAcknowledge.FORGET_ME;
import static org.entur.ukur.subscription.PushAcknowledge.OK;

@Service
public class SubscriptionManager {

    private DataStorageService dataStorageService;
    private SiriMarshaller siriMarshaller;
    private MetricsService metricsService;
    private Map<Object, Long> alreadySentCache;
    private String hostname;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private ExecutorService pushExecutor = Executors.newFixedThreadPool(20);

    @Autowired
    public SubscriptionManager(DataStorageService dataStorageService,
                               SiriMarshaller siriMarshaller,
                               MetricsService metricsService,
                               @Qualifier("alreadySentCache") Map<Object, Long> alreadySentCache) {
        this.dataStorageService = dataStorageService;
        this.siriMarshaller = siriMarshaller;
        this.metricsService = metricsService;
        this.alreadySentCache = alreadySentCache; //TODO: Leaves this in hazelcast for now because of easier eviction setup...
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            logger.info("This nodes hostname is '{}'", hostname);
        } catch (UnknownHostException e) {
            hostname = "random_"+new Random().nextInt(10000); //want to separate message producing nodes from each other, this will work as fallback
            logger.error("Cant resolve hostname - use random name '{}' instead to differentiate nodes", hostname, e);
        }
        logger.info("There are at startup {} subscriptions", dataStorageService.getNumberOfSubscriptions());
    }

    @SuppressWarnings("unused") //Used from camel route
    public Collection<Subscription> listAll() {
        Collection<Subscription> existingSubscriptions = dataStorageService.getSubscriptions();
        Collection<Subscription> result = new ArrayList<>(existingSubscriptions.size());
        HashSet<String> uniqueStops = new HashSet<>();
        for (Subscription subscription : existingSubscriptions) {
            Subscription clone = clone(subscription);
            //TODO: add authorization so we can list these things...
            clone.setId("---hidden---");
            clone.setPushAddress("---hidden---");
            result.add(clone);
            uniqueStops.addAll(clone.getFromStopPoints());
            uniqueStops.addAll(clone.getToStopPoints());
        }
        logger.debug("There are {} subscriptions regarding {} unique stoppoints", result.size(), uniqueStops.size());
        return result;
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef) {
        return dataStorageService.getSubscriptionsForStopPoint(stopPointRef);
    }

    public void notify(HashSet<Subscription> subscriptions, EstimatedVehicleJourney estimatedVehicleJourney) {
        for (Subscription subscription : subscriptions) {
            Set<String> subscribedStops = getAllStops(subscription);
            EstimatedVehicleJourney clone = clone(estimatedVehicleJourney);
            //Removes all other estimated calls than those subscribed upon:
            if (clone.getEstimatedCalls() != null && clone.getEstimatedCalls().getEstimatedCalls() != null) {
                Iterator<EstimatedCall> iterator = clone.getEstimatedCalls().getEstimatedCalls().iterator();
                while (iterator.hasNext()) {
                    EstimatedCall call = iterator.next();
                    String ref = call.getStopPointRef() == null ? "" : call.getStopPointRef().getValue();
                    if (!subscribedStops.contains(ref)) {
                        iterator.remove();
                    }
                }
            }
            //Removes all other recorded calls than those subscribed upon:
            if (clone.getRecordedCalls() != null && clone.getRecordedCalls().getRecordedCalls() != null) {
                Iterator<RecordedCall> iterator = clone.getRecordedCalls().getRecordedCalls().iterator();
                while (iterator.hasNext()) {
                    RecordedCall call = iterator.next();
                    String ref = call.getStopPointRef() == null ? "" : call.getStopPointRef().getValue();
                    if (!subscribedStops.contains(ref)) {
                        iterator.remove();
                    }
                }
            }
            clone.setIsCompleteStopSequence(null); //since we have tampered with the calls!
            pushMessage(subscription, clone);
        }
    }

    public void notify(HashSet<Subscription> subscriptions, PtSituationElement ptSituationElement) {
        for (Subscription subscription : subscriptions) {
            Set<String> subscribedStops = getAllStops(subscription);
            PtSituationElement clone = clone(ptSituationElement);
            AffectsScopeStructure affects = clone.getAffects();
            if (affects != null) {
                //make sure elements not covered by the norwegian profile are empty:
                affects.setAreaOfInterest(null);
                affects.setExtensions(null);
                affects.setOperators(null);
                affects.setPlaces(null);
                affects.setRoads(null);
                affects.setStopPoints(null);
                affects.setVehicles(null);
                //TODO: networks are part of the profile, but ignored for now
                affects.setNetworks(null);

                //Removes affected StopPoints not subscribed upon
                AffectsScopeStructure.StopPoints affectsStopPoints = affects.getStopPoints();
                if (affectsStopPoints != null && affectsStopPoints.getAffectedStopPoints() != null) {
                    Iterator<AffectedStopPointStructure> iterator = affectsStopPoints.getAffectedStopPoints().iterator();
                    while (iterator.hasNext()) {
                        AffectedStopPointStructure stop = iterator.next();
                        String ref = stop.getStopPointRef() == null ? "" : stop.getStopPointRef().getValue();
                        if (!subscribedStops.contains(ref)) {
                            iterator.remove();
                        }
                    }
                }
                //Removes affected StopPlaces not subscribed upon
                AffectsScopeStructure.StopPlaces stopPlaces = affects.getStopPlaces();
                if (stopPlaces != null && stopPlaces.getAffectedStopPlaces() != null) {
                    Iterator<AffectedStopPlaceStructure> iterator = stopPlaces.getAffectedStopPlaces().iterator();
                    while (iterator.hasNext()) {
                        AffectedStopPlaceStructure stop = iterator.next();
                        String ref = stop.getStopPlaceRef() == null ? "" : stop.getStopPlaceRef().getValue();
                        if (!subscribedStops.contains(ref)) {
                            iterator.remove();
                        }
                    }
                }
                //Removes affected VehicleJourneys (and unsubscribed stops) without any stops subscribed upon
                AffectsScopeStructure.VehicleJourneys vehicleJourneys = affects.getVehicleJourneys();
                if (vehicleJourneys != null && vehicleJourneys.getAffectedVehicleJourneies() != null) {
                    Iterator<AffectedVehicleJourneyStructure> iterator = vehicleJourneys.getAffectedVehicleJourneies().iterator();
                    while(iterator.hasNext()) {
                        AffectedVehicleJourneyStructure journeyStructure = iterator.next();
                        boolean removeJourney = true;
                        if (journeyStructure.getRoutes() != null) {
                            List<AffectedRouteStructure> routes = journeyStructure.getRoutes();
                            Iterator<AffectedRouteStructure> routeStructureIterator = routes.iterator();
                            while(routeStructureIterator.hasNext()) {
                                AffectedRouteStructure routeStructure = routeStructureIterator.next();
                                AffectedRouteStructure.StopPoints stopPoints = routeStructure.getStopPoints();
                                if (stopPoints != null && stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints() != null) {
                                    List<Serializable> affectedStopPointsAndLinkProjectionToNextStopPoints = stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints();
                                    Iterator<Serializable> stops = affectedStopPointsAndLinkProjectionToNextStopPoints.iterator();
                                    while (stops.hasNext()) {
                                        Serializable stop = stops.next();
                                        if (stop instanceof AffectedStopPointStructure) {
                                            AffectedStopPointStructure affectedStopPoint = (AffectedStopPointStructure) stop;
                                            String ref = affectedStopPoint.getStopPointRef() == null ? "" : affectedStopPoint.getStopPointRef().getValue();
                                            if (!subscribedStops.contains(ref)) {
                                                stops.remove();
                                            }
                                        }
                                    }
                                    if (affectedStopPointsAndLinkProjectionToNextStopPoints.isEmpty()) {
                                        routeStructureIterator.remove();
                                    }
                                }
                            }
                            removeJourney = routes.isEmpty();
                        }
                        if (removeJourney) {
                            iterator.remove();
                        }
                    }
                }
            }

            pushMessage(subscription, clone);
        }
    }


    @SuppressWarnings({"unused", "UnusedReturnValue"}) //Used from Camel REST api
    public Subscription add(Subscription s) {
        if (s == null) {
            throw new IllegalArgumentException("No subscription given");
        }
        if (StringUtils.isBlank(s.getPushAddress())) {
            throw new IllegalArgumentException("PushAddress is required");
        }
        s.normalizeAndRemoveIgnoredStops();
        if (s.getFromStopPoints() == null || s.getFromStopPoints().isEmpty()) {
            throw new IllegalArgumentException("At least one valid FROM stop required");
        }
        if (s.getToStopPoints() == null || s.getToStopPoints().isEmpty()) {
            throw new IllegalArgumentException("At least one valid TO stop required");
        }

        Subscription added = dataStorageService.addSubscription(s);
        logger.info("Adds new subscription - assigns id: {}", added.getId());
        logger.debug("There are now {} subscriptions", dataStorageService.getNumberOfSubscriptions());
        return added;
    }

    @SuppressWarnings({"unused", "UnusedReturnValue", "WeakerAccess"}) //Used from Camel REST api
    public void remove(String subscriptionId) {
        logger.info("Removes subscription with id {}", subscriptionId);
        dataStorageService.removeSubscription(subscriptionId);
    }

    private Set<String> getAllStops(Subscription subscription) {
        Set<String> fromStopPoints = subscription.getFromStopPoints();
        Set<String> toStopPoints = subscription.getToStopPoints();
        HashSet<String> result = new HashSet<>();
        if (fromStopPoints != null) {
            result.addAll(fromStopPoints);
        }
        if (toStopPoints != null) {
            result.addAll(toStopPoints);
        }
        return result;
    }

    private <T extends Serializable> T clone(T toClone) {
        return SerializationUtils.clone(toClone);
    }

    private void pushMessage(Subscription subscription, Object siriElement) {

        String alreadySentKey = calculateUniqueKey(subscription, siriElement);
        Long ifPresent = alreadySentCache.get(alreadySentKey);

        if (ifPresent != null) {
            long diffInSecs = (System.currentTimeMillis() - ifPresent) / 1000;
            logger.debug("skips message since it has already been pushed to the same subscription (id={}) {} seconds ago", subscription.getId(), diffInSecs);
            return;
        }

        alreadySentCache.put(alreadySentKey, System.currentTimeMillis());

        logger.debug("PUSH ({}) {} to subscription name: {}, pushAddress: {}",
                hostname, siriElement.getClass(), subscription.getName(), subscription.getPushAddress());
        if (StringUtils.startsWithIgnoreCase(subscription.getPushAddress(), "http://") ) {
            pushToHttp(subscription, siriElement);
        } else {
            logger.warn("No push address for subscription with id='{}'");
        }
    }

    private String calculateUniqueKey(Subscription subscription, Object siriElement) {
        String content;
        try {
            //Can't use the cxf generated objects directly, has to convert it to something we can compare
            content = siriMarshaller.marshall(siriElement);
        } catch (Exception e) {
            logger.warn("Could not marshall {}", e);
            content = UUID.randomUUID().toString();
        }
        return subscription.getId()+content.hashCode();
    }

    private void pushToHttp(Subscription subscription, Object siriElement) {

        pushExecutor.execute(() -> {
            Timer pushToHttp = metricsService.getTimer(MetricsService.TIMER_PUSH);
            Timer.Context context = pushToHttp.time();
            try {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_XML);
                headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
                RestTemplate restTemplate = new RestTemplate();
                String pushAddress = subscription.getPushAddress();
                if (siriElement instanceof EstimatedVehicleJourney) {
                    pushAddress += "/et";
                } else if (siriElement instanceof PtSituationElement) {
                    pushAddress += "/sx";
                }
                URI uri = URI.create(pushAddress);
                ResponseEntity<String> response = null;
                try {
                    HttpEntity entity = new HttpEntity<>(siriElement, headers);
                    response = restTemplate.postForEntity(uri, entity, String.class);
                    logger.debug("Receive {} on push to {} for subscription with id {}",
                            response, uri, subscription.getId());
                } catch (Exception e) {
                    logger.warn("Could not push to {} for subscription with id {}",
                            uri, subscription.getId(), e);
                }
                boolean ok = response != null && response.getStatusCode() == HttpStatus.OK;
                String responseBody = ok && response.getBody() != null ? response.getBody().trim() : null;
                if (ok && FORGET_ME.name().equals(responseBody)) {
                    logger.info("Receive {} on push to {} and removes subscription with id {}", FORGET_ME, uri, subscription.getId());
                    remove(subscription.getId());
                } else if (ok && OK.name().equals(responseBody)) {
                    subscription.resetFailedPushCounter();
                    dataStorageService.updateSubscription(subscription);
                } else {
                    logger.info("Unexpected response on push '{}' - increase failed push counter for subscription wih id {}",
                            response, subscription.getId());
                    long failedPushCounter = subscription.increaseFailedPushCounter();
                    if (failedPushCounter > 3) {
                        logger.info("Removes subscription with id {} after {} failed push attempts",
                                subscription.getId(), failedPushCounter);
                        remove(subscription.getId());
                    } else {
                        dataStorageService.updateSubscription(subscription);
                    }
                }
            } finally {
                context.stop();
            }
        });
    }
}
