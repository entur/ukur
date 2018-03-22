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

package org.entur.ukur.camelroute;

import com.codahale.metrics.Timer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.routedata.Call;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.routedata.LiveRouteManager;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.ifopt.siri20.StopPlaceRef;
import uk.org.siri.siri20.*;

import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class NsbSXSubscriptionProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private SubscriptionManager subscriptionManager;
    private SiriMarshaller siriMarshaller;
    private LiveRouteManager liveRouteManager;
    private FileStorageService fileStorageService;
    private MetricsService metricsService;
    @Value("${ukur.camel.sx.store.files:false}")
    private boolean storeMessagesToFile = false;

    @Autowired
    public NsbSXSubscriptionProcessor(SubscriptionManager subscriptionManager,
                                      SiriMarshaller siriMarshaller,
                                      LiveRouteManager liveRouteManager,
                                      FileStorageService fileStorageService,
                                      MetricsService metricsService) {
        this.subscriptionManager = subscriptionManager;
        this.siriMarshaller = siriMarshaller;
        this.liveRouteManager = liveRouteManager;
        this.fileStorageService = fileStorageService;
        this.metricsService = metricsService;
        logger.debug("Initializes...");
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.debug("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
        Timer timer = metricsService.getTimer(MetricsService.TIMER_SX_UNMARSHALL);
        Timer.Context time = timer.time();
        PtSituationElement ptSituationElement;
        try {
            ptSituationElement = siriMarshaller.unmarshall(xml, PtSituationElement.class);
        } finally {
            time.stop();
        }
        if (ptSituationElement == null) {
            throw new IllegalArgumentException("No PtSituationElement element...");
        }
        metricsService.registerReceivedMessage(PtSituationElement.class);
        if (processPtSituationElement(ptSituationElement)) {
            if (storeMessagesToFile) {
                fileStorageService.writeToFile(ptSituationElement);
            }
        }
    }

    private boolean processPtSituationElement(PtSituationElement ptSituationElement) {
        RequestorRef participantRef = ptSituationElement.getParticipantRef();
        boolean isNSB = participantRef != null && "NSB".equalsIgnoreCase(participantRef.getValue());
        if (!isNSB) {
            logger.trace("Skips estimatedVehicleJourney (not NSB)");
            return false;
        }
        AffectsScopeStructure affects = ptSituationElement.getAffects();
        if (affects == null) {
            logger.info("Got PtSituationElement without any effects - nothing to notify");
            return false;
        }
        com.codahale.metrics.Timer timer = metricsService.getTimer(MetricsService.TIMER_SX_PROCESS);
        Timer.Context time = timer.time();
        try {
            HashSet<String> stopsToNotify = findAffectedStopPointRefs(affects);
            AffectsScopeStructure.VehicleJourneys vehicleJourneys = affects.getVehicleJourneys();
            HashSet<Subscription> subscriptionsToNotify = new HashSet<>();
            if (vehicleJourneys != null) {
                subscriptionsToNotify.addAll(findAffectedSubscriptions(vehicleJourneys));
            }
            logger.debug("Processes NSB PtSituationElement ({}) - with {} affected stops", ptSituationElement.getSituationNumber().getValue(), stopsToNotify.size());
            for (String ref : stopsToNotify) {
                subscriptionsToNotify.addAll(subscriptionManager.getSubscriptionsForStopPoint(ref));
            }
            logger.debug("There are {} subscriptions to notify", subscriptionsToNotify.size());
            if (!subscriptionsToNotify.isEmpty()) {
                subscriptionManager.notifySubscriptions(subscriptionsToNotify, ptSituationElement);
            }
        } finally {
            time.stop();
        }
        return true;
    }

    /**
     *  Gå gjennom Affects|StopPoints og matche StopPointRef mot subscriptions
     *  Gå gjennom Affects|StopPlacess og matche StopPlaceRef mot subscriptions <-- Litt usikker på denne, men tar med for nå
	 *  Gå gjennom Affects|VehicleJourneys|AffectedVehicleJourney|Route|StopPoints|AffectedStopPoint og
     */
    protected HashSet<String> findAffectedStopPointRefs(AffectsScopeStructure affects) {
        HashSet<String> stopsToNotify = new HashSet<>();

        AffectsScopeStructure.StopPoints affectsStopPoints = affects.getStopPoints();
        if (affectsStopPoints != null) {
            List<AffectedStopPointStructure> affectedStopPoints = affectsStopPoints.getAffectedStopPoints();
            for (AffectedStopPointStructure affectedStopPoint : affectedStopPoints) {
                StopPointRef stopPointRef = affectedStopPoint.getStopPointRef();
                addStop(stopsToNotify, stopPointRef != null ? stopPointRef.getValue() : null);
            }
        }

        AffectsScopeStructure.StopPlaces stopPlaces = affects.getStopPlaces();
        if (stopPlaces != null) {
            List<AffectedStopPlaceStructure> affectedStopPlaces = stopPlaces.getAffectedStopPlaces();
            for (AffectedStopPlaceStructure affectedStopPlace : affectedStopPlaces) {
                StopPlaceRef stopPlaceRef = affectedStopPlace.getStopPlaceRef();
                addStop(stopsToNotify, stopPlaceRef != null ? stopPlaceRef.getValue() : null);
            }
        }

        return stopsToNotify;
    }

    protected HashSet<Subscription> findAffectedSubscriptions(AffectsScopeStructure.VehicleJourneys vehicleJourneys) {
        HashMap<String, LiveJourney> journeys = null;
        HashSet<Subscription> subscriptions = new HashSet<>();
        List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = vehicleJourneys.getAffectedVehicleJourneies();
        for (AffectedVehicleJourneyStructure affectedVehicleJourney : affectedVehicleJourneies) {
            List<AffectedRouteStructure> routes = affectedVehicleJourney.getRoutes();
            for (AffectedRouteStructure route : routes) {
                AffectedRouteStructure.StopPoints stopPoints = route.getStopPoints();
                if (stopPoints == null) continue;
                List<Serializable> affectedStopPointsAndLinkProjectionToNextStopPoints = stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints();
                List<String> orderedListOfStops = new ArrayList<>();
                for (Serializable affectedStopPointsAndLinkProjectionToNextStopPoint : affectedStopPointsAndLinkProjectionToNextStopPoints) {
                    if (affectedStopPointsAndLinkProjectionToNextStopPoint instanceof AffectedStopPointStructure) {
                        AffectedStopPointStructure affectedStopPoint = (AffectedStopPointStructure) affectedStopPointsAndLinkProjectionToNextStopPoint;
                        StopPointRef stopPointRef = affectedStopPoint.getStopPointRef();
                        addStop(orderedListOfStops, stopPointRef != null ? stopPointRef.getValue() : null);
                    }
                }
                boolean hasCompleteRoute = !Boolean.TRUE.equals(stopPoints.isAffectedOnly());
                String lineRef = affectedVehicleJourney.getLineRef() == null ? null : affectedVehicleJourney.getLineRef().getValue();
                String vehicleJourneyRef = null;

                List<VehicleJourneyRef> vehicleJourneyReves = affectedVehicleJourney.getVehicleJourneyReves();
                if (vehicleJourneyReves == null || vehicleJourneyReves.isEmpty()) {
                    logger.trace("No vehicleJourneyRef in AffectedVehicleJourneyStructure");
                } else if (vehicleJourneyReves.size() > 1) {
                    logger.warn("More than one ({}) vehicleJourneyRef in AffectedVehicleJourneyStructure - 'norsk siri profil' only allows one", vehicleJourneyReves.size());
                } else {
                    vehicleJourneyRef = vehicleJourneyReves.get(0).getValue();
                    if (!hasCompleteRoute || lineRef == null) {
                        if (journeys == null) {
                            journeys = getJourneys();
                        }
                        LiveJourney liveJourney = journeys.get(vehicleJourneyRef);
                        if (liveJourney == null) {
                            logger.debug("Has no route data for journey with vehicleJourneyRef: {}", vehicleJourneyRef);
                        } else {
                            if (!hasCompleteRoute) {
                                orderedListOfStops = liveJourney.getCalls().stream()
                                        .map(Call::getStopPointRef)
                                        .collect(Collectors.toList());
                                hasCompleteRoute = true;
                            }
                            if (lineRef == null) {
                                lineRef = liveJourney.getLineRef();
                                if (StringUtils.isNotBlank(lineRef)) {
                                    //update the original message with lineref so we later can pick out the relevant part when generating the push message
                                    LineRef ref = new LineRef();
                                    ref.setValue(lineRef);
                                    affectedVehicleJourney.setLineRef(ref);
                                }
                            }
                        }
                    }
                }

                for (String stop : orderedListOfStops) {
                    Set<Subscription> subscriptionsForStopPoint = subscriptionManager.getSubscriptionsForStopPoint(stop);
                    for (Subscription subscription : subscriptionsForStopPoint) {
                        //TODO: The vehicleJourneyRef usage below is NSB specific (in SX messages from NSB vehicleJourneyRef references vehicleRef in ET messages from BaneNOR)
                        if (subscribedOrEmpty(lineRef, subscription.getLineRefs()) && subscribedOrEmpty(vehicleJourneyRef, subscription.getVehicleRefs())){
                            if (!hasCompleteRoute) {
                                subscriptions.add(subscription);
                                logger.trace("Has only affected stops and don't find route in LiveRouteService, adds all subscriptions on these stops - regardless of direction");
                            } else {
                                if (affected(subscription, orderedListOfStops)) {
                                    subscriptions.add(subscription);
                                }
                            }
                        }
                    }
                }
                //TODO: The vehicleJourneyRef usage below is NSB specific (in SX messages from NSB vehicleJourneyRef references vehicleRef in ET messages from BaneNOR)
                subscriptions.addAll(findSubscriptionsOnLineOrVehicle(lineRef, vehicleJourneyRef));
            }
        }
        return subscriptions;
    }

    private HashSet<Subscription> findSubscriptionsOnLineOrVehicle(String lineRef, String vehicleRef) {
        HashSet<Subscription> subscriptions = new HashSet<>();
        if (StringUtils.isNotBlank(lineRef)) {
            Set<Subscription> subscriptionsForLineRef = subscriptionManager.getSubscriptionsForLineRef(lineRef);
            subscriptionsForLineRef.removeIf(s->!subscribedOrEmpty(vehicleRef, s.getVehicleRefs()));
            logger.trace("Adds {} subscriptions regarding lineRef={}", subscriptionsForLineRef.size(), lineRef);
            subscriptions.addAll(subscriptionsForLineRef);
        }
        if (StringUtils.isNotBlank(vehicleRef)) {
            Set<Subscription> subscriptionsForvehicleRef = subscriptionManager.getSubscriptionsForvehicleRef(vehicleRef);
            subscriptionsForvehicleRef.removeIf(s->!subscribedOrEmpty(lineRef, s.getLineRefs()));
            logger.trace("Adds {} subscriptions regarding vehicleRef={}", subscriptionsForvehicleRef.size(), vehicleRef);
            subscriptions.addAll(subscriptionsForvehicleRef);
        }
        return subscriptions;
    }

    private boolean subscribedOrEmpty(String value, Set<String> values) {
        return values.isEmpty() || StringUtils.isBlank(value) || values.contains(value);
    }


    private HashMap<String, LiveJourney> getJourneys() {
        HashMap<String, LiveJourney> journeys;//only get journeys once per PtSituationElement since access can be slow (hazelcast)
        journeys = new HashMap<>();
        for (LiveJourney liveJourney : liveRouteManager.getJourneys()) {
            //Since SX messages from NSB specify Vehicle
            journeys.put(liveJourney.getVehicleRef(), liveJourney);
        }
        return journeys;
    }

    protected boolean affected(Subscription subscription, List<String> orderedListOfStops) {
        int from = findIndexOfOne(subscription.getFromStopPoints(), orderedListOfStops);
        int to = findIndexOfOne(subscription.getToStopPoints(), orderedListOfStops);
        return from > -1 && to > -1 && from < to;
    }

    private int findIndexOfOne(Set<String> stops, List<String> orderedListOfStops) {
        for (int i = 0; i < orderedListOfStops.size(); i++) {
             if (stops.contains(orderedListOfStops.get(i))) {
                 return i;
             }
        }
        return -1;
    }

    private void addStop(Collection<String> stopsToNotify, String ref) {
        if (ref != null && StringUtils.startsWithIgnoreCase(ref, "NSR:")) {
            stopsToNotify.add(ref);
        }
    }

}

