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

package org.entur.ukur.route;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.org.ifopt.siri20.StopPlaceRef;
import uk.org.siri.siri20.*;

import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

@Service
public class NsbSXSubscriptionProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private SubscriptionManager subscriptionManager;
    private SiriMarshaller siriMarshaller;
    private SubscriptionStatus status = new SubscriptionStatus();

    @Autowired
    public NsbSXSubscriptionProcessor(SubscriptionManager subscriptionManager, SiriMarshaller siriMarshaller) {
        this.subscriptionManager = subscriptionManager;
        this.siriMarshaller = siriMarshaller;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.trace("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
        PtSituationElement ptSituationElement = siriMarshaller.unmarhall(xml, PtSituationElement.class);
        if (ptSituationElement == null) {
            throw new IllegalArgumentException("No PtSituationElement element...");
        }
        processPtSituationElement(ptSituationElement);
        status.processed(PtSituationElement.class);
    }

    @SuppressWarnings("unused") //Used from camel route
    public SubscriptionStatus getStatus() {
        return status;
    }

    private void processPtSituationElement(PtSituationElement ptSituationElement) {
        RequestorRef participantRef = ptSituationElement.getParticipantRef();
        boolean isNSB = participantRef != null && "NSB".equalsIgnoreCase(participantRef.getValue());
        if (!isNSB) {
            logger.trace("Skips estimatedVehicleJourney (not NSB)");
            return;
        }
        status.handled(PtSituationElement.class);
        AffectsScopeStructure affects = ptSituationElement.getAffects();
        if (affects == null) {
            logger.info("Got PtSituationElement without any effects - nothing to notify");
            return;
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

    }

    /**
     *  Gå gjennom Affects|StopPoints og matche StopPointRef mot subscriptions
     *  Gå gjennom Affects|StopPlacess og matche StopPlaceRef mot subscriptions <-- Litt usikker på denne, men tar med for nå
	 *  Gå gjennom Affects|VehicleJourneys|AffectedVehicleJourney|Route|StopPoints|AffectedStopPoint og
     */
    private HashSet<String> findAffectedStopPointRefs(AffectsScopeStructure affects) {
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
        HashSet<Subscription> subscriptions = new HashSet<>();
        List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = vehicleJourneys.getAffectedVehicleJourneies();
        for (AffectedVehicleJourneyStructure affectedVehicleJourney : affectedVehicleJourneies) {
            //TODO: if we get route-data from an other source, we can look up a route based on LineRef/VehicleJourneyRef
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
                for (String stop : orderedListOfStops) {
                    Set<Subscription> subscriptionsForStopPoint = subscriptionManager.getSubscriptionsForStopPoint(stop);
                    if (Boolean.TRUE.equals(stopPoints.isAffectedOnly())) {
                        subscriptions.addAll(subscriptionsForStopPoint);
                        logger.trace("Only affected stops in route, adds all subscriptions on these stops - regardless of direction");
                    } else {
                        //TODO: Assumes that the stops are complete and in correct order...
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

