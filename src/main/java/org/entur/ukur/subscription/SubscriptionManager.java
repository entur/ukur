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
import jakarta.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.camelroute.InvalidSubscriptionIdException;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.PrometheusMetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.xml.SiriMarshaller;
import org.entur.ukur.xml.SiriObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import uk.org.siri.siri21.AffectedLineStructure;
import uk.org.siri.siri21.AffectedRouteStructure;
import uk.org.siri.siri21.AffectedStopPlaceStructure;
import uk.org.siri.siri21.AffectedStopPointStructure;
import uk.org.siri.siri21.AffectedVehicleJourneyStructure;
import uk.org.siri.siri21.AffectsScopeStructure;
import uk.org.siri.siri21.EstimatedCall;
import uk.org.siri.siri21.EstimatedTimetableDeliveryStructure;
import uk.org.siri.siri21.EstimatedVehicleJourney;
import uk.org.siri.siri21.EstimatedVersionFrameStructure;
import uk.org.siri.siri21.HeartbeatNotificationStructure;
import uk.org.siri.siri21.PtSituationElement;
import uk.org.siri.siri21.RecordedCall;
import uk.org.siri.siri21.RequestorRef;
import uk.org.siri.siri21.ServiceDelivery;
import uk.org.siri.siri21.Siri;
import uk.org.siri.siri21.SituationExchangeDeliveryStructure;
import uk.org.siri.siri21.SubscriptionQualifierStructure;
import uk.org.siri.siri21.SubscriptionTerminatedNotificationStructure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.entur.ukur.service.MetricsService.GAUGE_PUSH_QUEUE;
import static org.entur.ukur.subscription.SiriXMLSubscriptionHandler.SIRI_VERSION;
import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;

@Service
public class SubscriptionManager {

    @Autowired
    PrometheusMetricsService prometheusMetricsService;

    private DataStorageService dataStorageService;
    private SiriMarshaller siriMarshaller;
    private MetricsService metricsService;
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;
    private String hostname;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private ThreadPoolExecutor pushExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);
    private Map<String, Long> subscriptionNextHeartbeat;
    private Map<MessageIdentifierKey, String> lastMessageChecksum;
    private ZonedDateTime nextTerminatedCheck = null;

    HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    @Autowired
    public SubscriptionManager(DataStorageService dataStorageService,
                               SiriMarshaller siriMarshaller,
                               MetricsService metricsService,
                               @Qualifier("heartbeats") Map<String, Long> subscriptionNextHeartbeat,
                               @Qualifier("lastMessageChecksum") Map<MessageIdentifierKey, String> lastMessageChecksum,
                               QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {
        this.dataStorageService = dataStorageService;
        this.siriMarshaller = siriMarshaller;
        this.metricsService = metricsService;
        this.subscriptionNextHeartbeat = subscriptionNextHeartbeat;
        this.lastMessageChecksum = lastMessageChecksum;
        this.quayAndStopPlaceMappingService = quayAndStopPlaceMappingService;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            logger.info("This nodes hostname is '{}'", hostname);
        } catch (UnknownHostException e) {
            hostname = "random_"+new Random().nextInt(10000); //want to separate message producing nodes from each other easy in the logs, this will work as fallback
            logger.error("Cant resolve hostname - use random name '{}' instead to differentiate nodes", hostname, e);
        }
        metricsService.registerGauge(GAUGE_PUSH_QUEUE, this::getActivePushQueueSize);
        logger.info("There are at startup {} subscriptions", dataStorageService.getNumberOfSubscriptions());
    }

    private int getActivePushQueueSize() {
        return pushExecutor.getQueue().size();
    }

    public int getActivePushThreads() {
        return pushExecutor.getActiveCount();
    }

    @SuppressWarnings("unused") //Used from camel route
    public Collection<Subscription> listAll() {
        Collection<Subscription> existingSubscriptions = dataStorageService.getSubscriptions();
        logger.debug("There are {} subscriptions", existingSubscriptions.size());
        return Collections.unmodifiableCollection(existingSubscriptions);
    }
    @SuppressWarnings("unused") //Used from camel route
    public Collection<Subscription> findDuplicates() {
        Collection<Subscription> existingSubscriptions = dataStorageService.getSubscriptions();
        logger.debug("There are {} subscriptions", existingSubscriptions.size());
        HashMap<String, Subscription> tmpDuplicates = new HashMap<>();
        HashMap<String, Subscription> urlDuplicates = new HashMap<>();
        for (Subscription existingSubscription : existingSubscriptions) {
            Subscription existing = tmpDuplicates.put(existingSubscription.getPushAddress(), existingSubscription);
            if (existing != null) {
                urlDuplicates.put(existing.getPushAddress(), existing);
            }
        }

        return Collections.unmodifiableCollection(urlDuplicates.values());
    }
    @SuppressWarnings("unused") //Used from camel route
    public Collection<Subscription> deleteDuplicates() {
        Collection<Subscription> existingSubscriptions = findDuplicates();
        logger.warn("Deleting {} duplicate subscriptions.   ", existingSubscriptions.size());
        for (Subscription duplicate : existingSubscriptions) {
            this.remove(duplicate.getId());
        }
        return Collections.unmodifiableCollection(existingSubscriptions);
    }

    @SuppressWarnings("unused") //Used from camel route
    public void reloadSubscriptionCache() {
        logger.info("Reloads subscription cache");
        dataStorageService.populateSubscriptionCacheFromDatastore();
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef, SubscriptionTypeEnum type) {
        HashSet<Subscription> subscriptions = new HashSet<>();
        if (stopPointRef.startsWith("NSR:Quay:")) {
            String stopPlace = quayAndStopPlaceMappingService.mapQuayToStopPlace(stopPointRef);
            if (StringUtils.isNotBlank(stopPlace)) {
                Set<Subscription> subscriptionsForStopPlace = dataStorageService.getSubscriptionsForStopPoint(stopPlace, type);
                logger.trace("Found {} subscriptions for stopPlace {} which quay {} is part of", subscriptionsForStopPlace.size(), stopPlace, stopPointRef);
                subscriptions.addAll(subscriptionsForStopPlace);
            }
        }
        Set<Subscription> subscriptionsForStopPoint = dataStorageService.getSubscriptionsForStopPoint(stopPointRef, type);
        logger.trace("Found {} subscriptions for {}", subscriptionsForStopPoint.size(), stopPointRef);
        subscriptions.addAll(subscriptionsForStopPoint);
        return subscriptions;
    }

    public Set<Subscription> getSubscriptionsForLineRef(String lineRef, SubscriptionTypeEnum type) {
        return dataStorageService.getSubscriptionsForLineRefAndNoStops(lineRef, type);
    }

    public Set<Subscription> getSubscriptionsForCodespace(String codespace, SubscriptionTypeEnum type) {
        return dataStorageService.getSubscriptionsForCodespaceAndNoStops(codespace, type);
    }

    public void notifySubscriptionsOnStops(HashSet<Subscription> subscriptions, EstimatedVehicleJourney estimatedVehicleJourney, ZonedDateTime timestamp) {
        for (Subscription subscription : subscriptions) {
            Set<String> subscribedStops = getAllStops(subscription);
            EstimatedVehicleJourney clone = clone(estimatedVehicleJourney);
            //Removes all other estimated calls than those subscribed upon:
            if (clone.getEstimatedCalls() != null && clone.getEstimatedCalls().getEstimatedCalls() != null) {
                Iterator<EstimatedCall> iterator = clone.getEstimatedCalls().getEstimatedCalls().iterator();
                while (iterator.hasNext()) {
                    EstimatedCall call = iterator.next();
                    String ref = getStringValue(call.getStopPointRef());
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
                    String ref = getStringValue(call.getStopPointRef());
                    if (!subscribedStops.contains(ref)) {
                        iterator.remove();
                    }
                }
            }
            clone.setIsCompleteStopSequence(false); //since we have tampered with the calls!
            pushMessage(subscription, clone, timestamp, "ET_STOPS", clone.getDataSource());
        }
    }

    public void notifySubscriptionsWithFullMessage(HashSet<Subscription> subscriptions, EstimatedVehicleJourney estimatedVehicleJourney, ZonedDateTime timestamp) {
        for (Subscription subscription : subscriptions) {
            pushMessage(subscription, estimatedVehicleJourney, timestamp, "ET_FULL", estimatedVehicleJourney.getDataSource());
        }
    }

    public void notifySubscriptions(HashSet<Subscription> subscriptions, PtSituationElement ptSituationElement, ZonedDateTime timestamp) {
        for (Subscription subscription : subscriptions) {
            Set<String> subscribedStops = getAllStops(subscription);
            PtSituationElement clone = clone(ptSituationElement);
            AffectsScopeStructure affects = clone.getAffects();
            if (affects != null) {
                //clears elements not covered by the norwegian profile to reduce size on push-message:
                affects.setAreaOfInterest(null);
                affects.setExtensions(null);
                affects.setOperators(null);
                affects.setPlaces(null);
                affects.setStopPoints(null);
                affects.setRoads(null);
                affects.setVehicles(null);
                //removes part not subscribed upon (to reduce size on push-message):
                if (subscription.getCodespaces().isEmpty() || !subscription.hasNoStops() || !subscription.getLineRefs().isEmpty()) {
                    removeUnsubscribedNetworks(subscription, affects.getNetworks());
                    if (subscription.hasNoStops()) {
                        affects.setStopPlaces(null);
                    } else {
                        removeUnsubscribedStopPlaces(subscribedStops, affects.getStopPlaces());
                    }
                    removeUnsubscribedJourneys(subscription, subscribedStops, affects.getVehicleJourneys());
                }
            }

            if (withAffects(clone)) {
                pushMessage(subscription, clone, timestamp, "SX", clone.getParticipantRef() != null ? clone.getParticipantRef().getValue() : null);
            } else {
                BigInteger version = SiriObjectHelper.getBigIntegerValue(clone.getVersion());
                String situationNumber = getStringValue(clone.getSituationNumber());
                logger.info("do not push PtSituationElement with situationnumber {} and version {} to subscription with id {} as all affects are removed", situationNumber, version, subscription.getId());
            }
        }
    }

    Subscription getSubscriptionByName(String name) {
        return dataStorageService.getSubscriptionByName(name);
    }

    private void removeUnsubscribedJourneys(Subscription subscription, Set<String> subscribedStops, AffectsScopeStructure.VehicleJourneys vehicleJourneys) {
        //Removes affected VehicleJourneys (and unsubscribed stops) without any stops, lines or vehicles subscribed upon
        if (vehicleJourneys != null && vehicleJourneys.getAffectedVehicleJourneies() != null) {
            Iterator<AffectedVehicleJourneyStructure> iterator = vehicleJourneys.getAffectedVehicleJourneies().iterator();
            while (iterator.hasNext()) {
                AffectedVehicleJourneyStructure journeyStructure = iterator.next();
                boolean removeJourney = true;
                if (isSubscribed(subscription, journeyStructure) && journeyStructure.getRoutes() != null) {
                    List<AffectedRouteStructure> routes = journeyStructure.getRoutes();
                    if (!subscription.hasNoStops()) {
                        Iterator<AffectedRouteStructure> routeStructureIterator = routes.iterator();
                        while (routeStructureIterator.hasNext()) {
                            AffectedRouteStructure routeStructure = routeStructureIterator.next();
                            AffectedRouteStructure.StopPoints stopPoints = routeStructure.getStopPoints();
                            if (stopPoints != null && stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints() != null) {
                                List<Serializable> affectedStopPointsAndLinkProjectionToNextStopPoints = stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints();
                                Iterator<Serializable> stops = affectedStopPointsAndLinkProjectionToNextStopPoints.iterator();
                                while (stops.hasNext()) {
                                    Serializable stop = stops.next();
                                    if (stop instanceof AffectedStopPointStructure) {
                                        AffectedStopPointStructure affectedStopPoint = (AffectedStopPointStructure) stop;
                                        String ref = getStringValue(affectedStopPoint.getStopPointRef());
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
                    }
                    removeJourney = routes.isEmpty();
                }
                if (removeJourney) {
                    iterator.remove();
                }
            }
        }
    }

    private void removeUnsubscribedStopPlaces(Set<String> subscribedStops, AffectsScopeStructure.StopPlaces stopPlaces) {
        if (stopPlaces != null && stopPlaces.getAffectedStopPlaces() != null) {
            Iterator<AffectedStopPlaceStructure> iterator = stopPlaces.getAffectedStopPlaces().iterator();
            while (iterator.hasNext()) {
                AffectedStopPlaceStructure stop = iterator.next();
                String ref = getStringValue(stop.getStopPlaceRef());
                if (!subscribedStops.contains(ref)) {
                    iterator.remove();
                }
            }
        }
    }

    private void removeUnsubscribedNetworks(Subscription subscription, AffectsScopeStructure.Networks networks) {
        if (networks != null && networks.getAffectedNetworks() != null) {
            Iterator<AffectsScopeStructure.Networks.AffectedNetwork> networkIterator = networks.getAffectedNetworks().iterator();
            while (networkIterator.hasNext()) {
                AffectsScopeStructure.Networks.AffectedNetwork affectedNetwork = networkIterator.next();
                boolean keep = false;
                List<AffectedLineStructure> affectedLines = affectedNetwork.getAffectedLines();
                if (affectedLines != null) {
                    Iterator<AffectedLineStructure> lineIterator = affectedLines.iterator();
                    while (lineIterator.hasNext()) {
                        AffectedLineStructure line = lineIterator.next();
                        if (subscription.getLineRefs().contains(getStringValue(line.getLineRef()))) {
                            keep = true;
                        } else {
                            lineIterator.remove();
                        }
                    }
                }
                if (!keep) {
                    networkIterator.remove();
                }
            }
        }
    }

    private boolean withAffects(PtSituationElement element) {
        AffectsScopeStructure affects = element.getAffects();
        if (affects == null) {
            return false;
        }
        List<AffectedStopPlaceStructure> stopPlaces = affects.getStopPlaces() == null ? null : affects.getStopPlaces().getAffectedStopPlaces();
        List<AffectedVehicleJourneyStructure> vehicleJourneys = affects.getVehicleJourneys() == null ? null : affects.getVehicleJourneys().getAffectedVehicleJourneies();
        List<AffectsScopeStructure.Networks.AffectedNetwork> networks = affects.getNetworks() == null ? null : affects.getNetworks().getAffectedNetworks();
        boolean hasStopPlace = stopPlaces != null && !stopPlaces.isEmpty();
        boolean hasVehicleJourney = vehicleJourneys != null && !vehicleJourneys.isEmpty();
        boolean hasNetworks = networks != null && !networks.isEmpty();
        return hasStopPlace || hasVehicleJourney || hasNetworks;
    }

    private boolean isSubscribed(Subscription subscription, AffectedVehicleJourneyStructure journey) {
        if (!subscription.getLineRefs().isEmpty() && journey.getLineRef() != null) {
            String lineref = journey.getLineRef().getValue();
            return !StringUtils.isNotBlank(lineref) || subscription.getLineRefs().contains(lineref);
        }
        return true;
    }

    @SuppressWarnings({"unused", "UnusedReturnValue"}) //Used from Camel REST api
    public Subscription addOrUpdate(Subscription s) {
        return addOrUpdate(s, false);
    }

    @SuppressWarnings({"unused"}) //Used from Camel quartz trigger route
    public void handleHeartbeatAndTermination() {
        handleHeartbeatAndTermination(ZonedDateTime.now());
    }

    void handleHeartbeatAndTermination(ZonedDateTime now) {

        Date dateNow = Date.from(now.toInstant());
        long epochNow = dateNow.getTime();

        Collection<Subscription> subscriptions = new HashSet<>(dataStorageService.getSubscriptions());

        //check for terminated subscriptions only every 3 hour (node local timestamp as there is no harm if we check more frequent)
        if (nextTerminatedCheck == null || nextTerminatedCheck.isAfter(now)) {
            nextTerminatedCheck = now.plusHours(3);
            for (Iterator<Subscription> iterator = subscriptions.iterator(); iterator.hasNext(); ) {
                Subscription subscription = iterator.next();
                if (subscription.getInitialTerminationTime() != null && now.isAfter(subscription.getInitialTerminationTime())) {
                    logger.info("Removes subscription with InitialTerminationTime in the past - subscription id={}, name={}", subscription.getId(), subscription.getName());
                    if (prometheusMetricsService != null) {
                        prometheusMetricsService.registerRemovedSubscription(subscription.getPushHost(), 1);
                    }
                    pushNotification(subscription, NotificationTypeEnum.subscriptionTerminated);
                    remove(subscription.getId());
                    iterator.remove(); //to prevent from also sending heartbeat
                }
            }
        }

        for (Subscription subscription : subscriptions) {
            Duration heartbeatInterval = subscription.getHeartbeatInterval();
            if (heartbeatInterval != null) {
                Long nextHeartbeat = subscriptionNextHeartbeat.get(subscription.getId());
                if (nextHeartbeat == null || nextHeartbeat < epochNow) {
                    long epochNextNotification = heartbeatInterval.toMillis() + epochNow;
                    subscriptionNextHeartbeat.put(subscription.getId(), epochNextNotification);
                    if (nextHeartbeat != null) {
                        pushNotification(subscription, NotificationTypeEnum.heartbeat);
                    } //else we assume subscription is just created - and don't notify until next time
                }
            }
        }
    }

    Subscription addOrUpdate(Subscription subscription, boolean siriXML) {
        if (subscription == null) {
            throw new IllegalArgumentException("No subscription given");
        }
        if (StringUtils.isBlank(subscription.getPushAddress())) {
            throw new IllegalArgumentException("PushAddress is required");
        }
        subscription.normalizeAndRemoveIgnoredStops();
        removeInvalidStopPointsFromSubscription(subscription);

            boolean noToStops = subscription.getToStopPoints().isEmpty();
            boolean noFromStops = subscription.getFromStopPoints().isEmpty();
            boolean noCodespaces = subscription.getCodespaces().isEmpty();
            boolean noLineRefs = subscription.getLineRefs().isEmpty();
            if (noToStops && noFromStops && noCodespaces && noLineRefs) {
                throw new IllegalArgumentException("No criterias given, must have at least one lineRef, one codespace or a valid fromStop and a valid toStop." +
                        " Please check NSR database for valid stops");
            }
            if ((noFromStops && !noToStops) || (noToStops && !noFromStops)) {
                throw new IllegalArgumentException("Must have both TO and FROM valid stops");
            }

        if (!siriXML && subscription.isSiriXMLBasedSubscription()) {
            throw new IllegalArgumentException("Illegal name (can't start with 'SIRI-XML')");
        }
        if (StringUtils.isNotBlank(subscription.getId())) {
            logger.info("Attempts to updates subscription with id {}", subscription.getId());
            if ( updateSubscription(subscription)) {
                logger.info("Updated subscription with id {} successfully", subscription.getId());
            } else {
                throw new InvalidSubscriptionIdException("Could not update subscription");
            }
            return subscription;
        } else {
            Subscription added = addSubscription(subscription);
            logger.info("Added new subscription - assigns id: {}", added.getId());
            if (prometheusMetricsService != null) {
                prometheusMetricsService.registerAddedSubscription(added.getPushHost(), 1);
            }
            return added;
        }
    }

    private void removeInvalidStopPointsFromSubscription(Subscription subscription) {
        Set<String> fromStopPoints = new HashSet<>(subscription.getFromStopPoints());
        Set<String> toStopPoints = new HashSet<>(subscription.getToStopPoints());

        for (String fromStopPoint : fromStopPoints) {
            if (!quayAndStopPlaceMappingService.isValidStopPlace(fromStopPoint) &&
                    !quayAndStopPlaceMappingService.isValidQuayId(fromStopPoint)) {
                subscription.removeFromStopPoint(fromStopPoint);
            }
        }
        for (String toStopPoint : toStopPoints) {
            if (!quayAndStopPlaceMappingService.isValidStopPlace(toStopPoint) &&
                    !quayAndStopPlaceMappingService.isValidQuayId(toStopPoint)) {
                subscription.removeToStopPoint(toStopPoint);
            }
        }
    }

    @SuppressWarnings({"unused", "UnusedReturnValue", "WeakerAccess"}) //Used from Camel REST api
    public void remove(String subscriptionId) {
        logger.info("Removes subscription with id {}", subscriptionId);
        removeSubscription(subscriptionId);
        subscriptionNextHeartbeat.remove(subscriptionId);

        List<MessageIdentifierKey> messageKeysForSubscription = lastMessageChecksum.keySet().stream().filter(key -> key.subscriptionId.equals(subscriptionId)).collect(Collectors.toList());
        for (MessageIdentifierKey keyToRemove : messageKeysForSubscription) {
            lastMessageChecksum.remove(keyToRemove);
        }
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
        HashSet<String> mappedQuays = new HashSet<>();
        for (String stopPoint : result) {
            if (stopPoint.startsWith("NSR:StopPlace:")) {
                mappedQuays.addAll(quayAndStopPlaceMappingService.mapStopPlaceToQuays(stopPoint));
            }
        }
        result.addAll(mappedQuays);
        return result;
    }

    private <T extends Serializable> T clone(T toClone) {
        return SerializationUtils.clone(toClone);
    }

    private void pushMessage(Subscription subscription, Object siriElement, ZonedDateTime timestamp, String typeOfMessage, String codespace) {
//        logger.info("PUSH {} to id={}", siriElement.getClass().getSimpleName(), subscription.getId());
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerDataToSubscriber(subscription.getPushHost(), typeOfMessage, codespace, 1);
        }
        pushToHttp(subscription, siriElement, timestamp);
    }

    private void pushNotification(Subscription subscription, NotificationTypeEnum type) {
        pushExecutor.execute(() -> {
            try {
                Siri siri = new Siri();
                siri.setVersion(SIRI_VERSION);
                switch (type) {
                    case heartbeat:
                        HeartbeatNotificationStructure heartbeatNotification = new HeartbeatNotificationStructure();
                        siri.setHeartbeatNotification(heartbeatNotification);
                        heartbeatNotification.setRequestTimestamp(ZonedDateTime.now());
                        RequestorRef producerRef = new RequestorRef();
                        producerRef.setValue(subscription.getName());
                        heartbeatNotification.setProducerRef(producerRef);
                        break;
                    case subscriptionTerminated:
                        SubscriptionTerminatedNotificationStructure subscriptionTerminatedNotification = new SubscriptionTerminatedNotificationStructure();
                        subscriptionTerminatedNotification.setResponseTimestamp(ZonedDateTime.now());
                        RequestorRef requestorRef = new RequestorRef();
                        requestorRef.setValue(subscription.getSiriRequestor());
                        subscriptionTerminatedNotification.getSubscriberRevesAndSubscriptionRevesAndSubscriptionFilterReves().add(requestorRef);
                        SubscriptionQualifierStructure subscriptionQualifierStructure = new SubscriptionQualifierStructure();
                        subscriptionQualifierStructure.setValue(subscription.getSiriClientGeneratedId());
                        subscriptionTerminatedNotification.getSubscriberRevesAndSubscriptionRevesAndSubscriptionFilterReves().add(subscriptionQualifierStructure);
                        siri.setSubscriptionTerminatedNotification(subscriptionTerminatedNotification);
                        break;
                    default:
                        logger.error("Called without proper type specified...");
                        return;
                }
                HttpStatus responseStatus = post(subscription, subscription.getPushAddress(), siri);
                logger.info("POST {} to {}: {}", type, subscription.getId(), responseStatus);

                handleResponse(responseStatus, subscription, subscription.getPushAddress());

            } catch (Exception e) {
                logger.error("Got exception while pushing message", e);
            }
        });
    }

    private void pushToHttp(Subscription subscription, Object siriElement, ZonedDateTime timestamp) {
        pushExecutor.execute(() -> {
            try {
                String pushAddress = subscription.getPushAddress();

                String messageIdentifier = null;

                final Object pushMessage;
                if (subscription.isUseSiriSubscriptionModel()) {
                    Siri siri = new Siri();
                    siri.setVersion(SIRI_VERSION);
                    siri.setServiceDelivery(new ServiceDelivery());
                    siri.getServiceDelivery().setResponseTimestamp(timestamp);
                    RequestorRef producer = new RequestorRef();
                    if (siriElement instanceof EstimatedVehicleJourney) {
                        producer.setValue(((EstimatedVehicleJourney) siriElement).getDataSource());
                        EstimatedTimetableDeliveryStructure estimatedTimetableDeliveryStructure = new EstimatedTimetableDeliveryStructure();
                        EstimatedVersionFrameStructure estimatedVersionFrameStructure = new EstimatedVersionFrameStructure();
                        estimatedVersionFrameStructure.getEstimatedVehicleJourneies().add((EstimatedVehicleJourney) siriElement);
                        estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrames().add(estimatedVersionFrameStructure);
                        siri.getServiceDelivery().getEstimatedTimetableDeliveries().add(estimatedTimetableDeliveryStructure);
                    } else if (siriElement instanceof PtSituationElement) {
                        producer.setValue(((PtSituationElement) siriElement).getParticipantRef().getValue());
                        SituationExchangeDeliveryStructure situationExchangeDeliveryStructure = new SituationExchangeDeliveryStructure();
                        SituationExchangeDeliveryStructure.Situations situations = new SituationExchangeDeliveryStructure.Situations();
                        situationExchangeDeliveryStructure.setSituations(situations);
                        situations.getPtSituationElements().add((PtSituationElement) siriElement);
                        siri.getServiceDelivery().getSituationExchangeDeliveries().add(situationExchangeDeliveryStructure);
                    }
                    siri.getServiceDelivery().setProducerRef(producer);
                    pushMessage = siri;
                } else {
                    pushMessage = siriElement;
                    if (siriElement instanceof EstimatedVehicleJourney) {
                        pushAddress += "/et";
                    } else if (siriElement instanceof PtSituationElement) {
                        pushAddress += "/sx";
                    }
                }

                if (siriElement instanceof EstimatedVehicleJourney) {
                    if (((EstimatedVehicleJourney) siriElement).getDatedVehicleJourneyRef() != null) {
                        messageIdentifier = ((EstimatedVehicleJourney) siriElement).getDatedVehicleJourneyRef().getValue();
                    }
                } else if (siriElement instanceof PtSituationElement) {
                    if (((PtSituationElement) siriElement).getSituationNumber() != null) {
                        messageIdentifier = ((PtSituationElement) siriElement).getSituationNumber().getValue();
                    }
                }

                if (hasMessageBeenUpdated(subscription, messageIdentifier, pushMessage)) {

                    HttpStatus responseStatus = post(subscription, pushAddress, pushMessage);
                    handleResponse(responseStatus, subscription, pushAddress);
                } else {
                    logger.info("Ignoring push to {} since data for key [{}] has not changed since last push-attempt.", subscription, messageIdentifier);
                }
            } catch (Exception e) {
                logger.error("Got exception while pushing message", e);
            }
        });
    }

    /*
     * @return true if the message sent is NOT equal to last sent message.
     */
    private boolean hasMessageBeenUpdated(Subscription subscription, String messageIdentifier, Object pushMessage) {
        boolean isEqualToLastMessage = false;
        try {
            String subscriptionId = subscription.getId();

            MessageIdentifierKey msgId = new MessageIdentifierKey(subscriptionId, messageIdentifier);

            String messageChecksum = getChecksum((Serializable) pushMessage);

            if (lastMessageChecksum.containsKey(msgId)) {
                isEqualToLastMessage = messageChecksum.equals(lastMessageChecksum.get(msgId));
            }

            lastMessageChecksum.put(msgId, messageChecksum);

        } catch (Exception e) {
            //Ignore - will be flagged as updated message
        }
        return !isEqualToLastMessage;
    }

    private static String getChecksum(Serializable object) throws IOException, NoSuchAlgorithmException {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] thedigest = md.digest(baos.toByteArray());
            return DatatypeConverter.printHexBinary(thedigest);
        } finally {
            oos.close();
            baos.close();
        }
    }

    private void handleResponse(HttpStatus responseStatus, Subscription subscription, String pushAddress) {
        if (HttpStatus.RESET_CONTENT.equals(responseStatus)) {
            logger.info("Receive {} on push to {} and removes subscription with id {}", HttpStatus.RESET_CONTENT, pushAddress, subscription.getId());
            remove(subscription.getId());
        } else if (HttpStatus.OK.equals(responseStatus)) {
            if (subscription.getFailedPushCounter() > 0) {
                subscription.resetFailedPushCounter();
                updateSubscription(subscription);
            }
        } else {

            logger.info("Unexpected response code on push '{}' - increase failed push counter from {} for subscription wih id {}",
                    responseStatus,
                    subscription.getFailedPushCounter(),
                    subscription.getId()
            );

            // Last request failed - remove checksum for last send-attempt to retry
            lastMessageChecksum.remove(subscription.getId());

            if (subscription.shouldRemove()) {
                logger.info("Removing subscription with id {} after {} failed push attempts where first error was seen {}", subscription.getId(), subscription.getFailedPushCounter(), subscription.getFirstErrorSeen() );
                remove(subscription.getId());
            } else {
                logger.info("Updating subscription {} with failed push counter {}", subscription.getId(), subscription.getFailedPushCounter());
                updateSubscription(subscription);
            }
        }
    }

    private boolean updateSubscription(Subscription subscription) {
        Timer.Context context = metricsService.getTimer(MetricsService.SUBSCRIPTION_UPDATE).time();
        try {
            return dataStorageService.updateSubscription(subscription);
        } finally {
            context.stop();
        }
    }

    private Subscription addSubscription(Subscription subscription) {
        Timer.Context context = metricsService.getTimer(MetricsService.SUBSCRIPTION_ADD).time();
        try {
            return dataStorageService.addSubscription(subscription);
        } finally {
            context.stop();
        }
    }

    private void removeSubscription(String subscriptionId) {
        Timer.Context context = metricsService.getTimer(MetricsService.SUBSCRIPTION_DELETE).time();
        try {
            dataStorageService.removeSubscription(subscriptionId);
        } finally {
            context.stop();
        }
    }


    private HttpStatus post(Subscription subscription, String pushAddress, Object pushMessage) {
        Timer pushToHttp = metricsService.getTimer(MetricsService.TIMER_PUSH);
        Timer.Context context = pushToHttp.time();
        try {
            String payload = siriMarshaller.marshall(pushMessage);

            HttpRequest post = HttpRequest.newBuilder()
                    .uri(URI.create(pushAddress))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload.getBytes(StandardCharsets.UTF_8)))
                    .header("Content-Type", "application/xml")
                    .build();

            HttpResponse<Void> response = httpClient.send(post, HttpResponse.BodyHandlers.discarding());
            int responseCode = response.statusCode();

            logger.debug("Receive {} on push to {} for subscription {}", responseCode, pushAddress, subscription);
            return HttpStatus.valueOf(responseCode);
        } catch (ConnectException e) {
            logger.warn("Connection timed out for push to {} for subscription with id {}", subscription.getPushAddress(), subscription.getId(), e);
            return HttpStatus.REQUEST_TIMEOUT;
        } catch (Exception e) {
            logger.warn("Could not push to {} for subscription with id {}", subscription.getPushAddress(), subscription.getId(), e);
            return HttpStatus.BAD_REQUEST;
        } finally {
            context.stop();
        }
    }

    private enum NotificationTypeEnum {
        heartbeat,
        subscriptionTerminated
    }
}

