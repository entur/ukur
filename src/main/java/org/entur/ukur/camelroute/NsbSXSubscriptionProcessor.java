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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.camelroute.status.SubscriptionStatus;
import org.entur.ukur.routedata.Call;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.routedata.LiveRouteManager;
import org.entur.ukur.service.FileStorageService;
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
    private SubscriptionStatus status = new SubscriptionStatus();
    @Value("${ukur.camel.sx.store.files:false}")
    private boolean storeMessagesToFile = false;

    @Autowired
    public NsbSXSubscriptionProcessor(SubscriptionManager subscriptionManager,
                                      SiriMarshaller siriMarshaller,
                                      LiveRouteManager liveRouteManager,
                                      FileStorageService fileStorageService) {
        this.subscriptionManager = subscriptionManager;
        this.siriMarshaller = siriMarshaller;
        this.liveRouteManager = liveRouteManager;
        this.fileStorageService = fileStorageService;
        logger.debug("Initializes...");
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.trace("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
        PtSituationElement ptSituationElement = siriMarshaller.unmarhall(xml, PtSituationElement.class);
        if (ptSituationElement == null) {
            throw new IllegalArgumentException("No PtSituationElement element...");
        }
        status.processed(PtSituationElement.class);
        if (processPtSituationElement(ptSituationElement)) {
            status.handled(PtSituationElement.class);
            if (storeMessagesToFile) {
                fileStorageService.writeToFile(ptSituationElement);
            }
        }
    }

    @SuppressWarnings("unused") //Used from camel camelroute
    public SubscriptionStatus getStatus() {
        return status;
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
            subscriptionManager.notify(subscriptionsToNotify, ptSituationElement);
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
                if (!hasCompleteRoute) {
                    List<VehicleJourneyRef> vehicleJourneyReves = affectedVehicleJourney.getVehicleJourneyReves();
                    if (vehicleJourneyReves == null || vehicleJourneyReves.isEmpty()) {
                        logger.trace("No vehicleJourneyRef in AffectedVehicleJourneyStructure");
                    } else if (vehicleJourneyReves.size() > 1) {
                        logger.warn("More than one ({}) vehicleJourneyRef in AffectedVehicleJourneyStructure - 'norsk siri profil' only allows one", vehicleJourneyReves.size());
                    } else {
                        String vehicleJourneyRef = vehicleJourneyReves.get(0).getValue();
                        if (journeys == null) {
                            //only get journeys once per PtSituationElement since access can be slow (hazelcast)
                            journeys = new HashMap<>();
                            for (LiveJourney liveJourney : liveRouteManager.getJourneys()) {
                                journeys.put(liveJourney.getVehicleRef(), liveJourney);
                            }
                        }
                        LiveJourney liveJourney = journeys.get(vehicleJourneyRef);
                        if (liveJourney == null) {
                            logger.debug("Has no camelroute data for journey with vehicleJourneyRef: {}", vehicleJourneyRef);
                        } else {
                            orderedListOfStops = liveJourney.getCalls().stream()
                                    .map(Call::getStopPointRef)
                                    .collect(Collectors.toList());
                            hasCompleteRoute = true;
                        }
                    }
                }

                for (String stop : orderedListOfStops) {
                    Set<Subscription> subscriptionsForStopPoint = subscriptionManager.getSubscriptionsForStopPoint(stop);
                    if (!hasCompleteRoute) {
                        subscriptions.addAll(subscriptionsForStopPoint);
                        logger.trace("Has only affected stops and don't find camelroute in LiveRouteService, adds all subscriptions on these stops - regardless of direction");
                    } else {
                        for (Subscription subscription : subscriptionsForStopPoint) {
                            if (affected(subscription, orderedListOfStops)) {
                                subscriptions.add(subscription);
                            }
                        }
                    }
                }
            }
        }
        return subscriptions;
    }

    private boolean affected(Subscription subscription, List<String> orderedListOfStops) {
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

