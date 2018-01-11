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

import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.lang3.StringUtils;
import org.rutebanken.siri20.util.SiriXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.ifopt.siri20.StopPlaceRef;
import uk.org.siri.siri20.*;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

class NsbSXSubscriptionProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private SubscriptionManager subscriptionManager;

    public NsbSXSubscriptionProcessor(SubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.debug("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
        Siri siri = SiriXml.parseXml(xml);
        if (siri == null || siri.getServiceDelivery() == null) {
            throw new IllegalArgumentException("No ServiceDelivery element...");
        }
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        exchange.getIn().setHeader(TestRoute.MORE_DATA_HEADER, serviceDelivery.isMoreData());
        List<SituationExchangeDeliveryStructure> situationExchangeDeliveries = serviceDelivery.getSituationExchangeDeliveries();
        for (SituationExchangeDeliveryStructure situationExchangeDelivery : situationExchangeDeliveries) {
            SituationExchangeDeliveryStructure.Situations situations = situationExchangeDelivery.getSituations();
            List<PtSituationElement> ptSituationElements = situations.getPtSituationElements();
            for (PtSituationElement ptSituationElement : ptSituationElements) {
                processPtSituationElement(ptSituationElement);
            }
        }
    }

    private void processPtSituationElement(PtSituationElement ptSituationElement) {
        RequestorRef participantRef = ptSituationElement.getParticipantRef();
        boolean isNSB = participantRef != null && "NSB".equalsIgnoreCase(participantRef.getValue());
        if (!isNSB) {
//            logger.debug("Skips estimatedVehicleJourney (not NSB)");
            return;
        }
        HashSet<String> stopsToNotify = findAffectedStopPointRefs(ptSituationElement);
        HashSet<Subscription> subscriptionsToNotify = new HashSet<>();
        for (String ref : stopsToNotify) {
            subscriptionsToNotify.addAll(subscriptionManager.getSubscriptionsForStopPoint(ref));
        }
        if (!subscriptionsToNotify.isEmpty()) {
            subscriptionManager.notify(subscriptionsToNotify, ptSituationElement);
        }

    }

    /**
     *  Gå gjennom Affects|StopPoints og matche StopPointRef mot subscriptions
     *  Gå gjennom Affects|StopPlacess og matche StopPlaceRef mot subscriptions <-- Litt usikker på denne, men tar med for nå
	 *  Gå gjennom Affects|VehicleJourneys|AffectedVehicleJourney|Route|StopPoints|AffectedStopPoint og
     */
    private HashSet<String> findAffectedStopPointRefs(PtSituationElement ptSituationElement) {
        HashSet<String> stopsToNotify = new HashSet<>();
        AffectsScopeStructure affects = ptSituationElement.getAffects();
        if (affects == null) return stopsToNotify;

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
        AffectsScopeStructure.VehicleJourneys vehicleJourneys = affects.getVehicleJourneys();
        if (vehicleJourneys != null) {
            List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = vehicleJourneys.getAffectedVehicleJourneies();
            for (AffectedVehicleJourneyStructure affectedVehicleJourney : affectedVehicleJourneies) {
                List<AffectedRouteStructure> routes = affectedVehicleJourney.getRoutes();
                for (AffectedRouteStructure route : routes) {
                    AffectedRouteStructure.StopPoints stopPoints = route.getStopPoints();
                    if (stopPoints == null) continue;
                    //TODO: Mulig vi kan sjekke retning på ruta om vi kan stole på rekkefølgen til stoppoint'sa
                    //TODO: PtSituationElement med SituationNumber som begynner med "status-" gjelder en (/flere?) konkret avgang, og meldingen må kobles til denne for å gi mening:
                    //TODO: eksempel "Vennligst ta neste eller andre tog."
                    List<Serializable> affectedStopPointsAndLinkProjectionToNextStopPoints = stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints();
                    for (Serializable affectedStopPointsAndLinkProjectionToNextStopPoint : affectedStopPointsAndLinkProjectionToNextStopPoints) {
                        if (affectedStopPointsAndLinkProjectionToNextStopPoint instanceof AffectedStopPointStructure) {
                            AffectedStopPointStructure affectedStopPoint = (AffectedStopPointStructure) affectedStopPointsAndLinkProjectionToNextStopPoint;
                            StopPointRef stopPointRef = affectedStopPoint.getStopPointRef();
                            addStop(stopsToNotify, stopPointRef != null ? stopPointRef.getValue() : null);
                        }
                    }
                }
            }
        }
        return stopsToNotify;
    }

    private void addStop(HashSet<String> stopsToNotify, String ref) {
        if (ref != null && StringUtils.startsWithIgnoreCase(ref, "NSR:")) {
            stopsToNotify.add(ref);
        }
    }

}

