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
import uk.org.siri.siri20.AffectedLineStructure;
import uk.org.siri.siri20.AffectedRouteStructure;
import uk.org.siri.siri20.AffectedStopPlaceStructure;
import uk.org.siri.siri20.AffectedStopPointStructure;
import uk.org.siri.siri20.AffectedVehicleJourneyStructure;
import uk.org.siri.siri20.AffectsScopeStructure;
import uk.org.siri.siri20.HalfOpenTimestampOutputRangeStructure;
import uk.org.siri.siri20.PtSituationElement;
import uk.org.siri.siri20.ServiceDelivery;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.StopPointRef;

import java.io.InputStream;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.entur.ukur.subscription.SubscriptionTypeEnum.SX;
import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;

@Service
public class SXSubscriptionProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private SubscriptionManager subscriptionManager;
    private SiriMarshaller siriMarshaller;
    private FileStorageService fileStorageService;
    private MetricsService metricsService;
    @Value("${ukur.camel.sx.store.files:false}")
    private boolean storeMessagesToFile = false;

    @Autowired
    public SXSubscriptionProcessor(SubscriptionManager subscriptionManager,
                                   SiriMarshaller siriMarshaller,
                                   FileStorageService fileStorageService,
                                   MetricsService metricsService) {
        this.subscriptionManager = subscriptionManager;
        this.siriMarshaller = siriMarshaller;
        this.fileStorageService = fileStorageService;
        this.metricsService = metricsService;
        logger.debug("Initializes...");
    }

    @Override
    public void process(Exchange exchange) {
        try {
            InputStream xml = exchange.getIn().getBody(InputStream.class);
            logger.debug("Received XML with size {} bytes", xml.available());
            Timer timer = metricsService.getTimer(MetricsService.TIMER_SX_UNMARSHALL);
            Timer.Context time = timer.time();
            PtSituationElement ptSituationElement;
            ZonedDateTime timestamp;
            try {
                Siri siri = siriMarshaller.unmarshall(xml, Siri.class);
                ServiceDelivery serviceDelivery = siri.getServiceDelivery();
                timestamp = serviceDelivery.getResponseTimestamp();
                ptSituationElement = serviceDelivery.getSituationExchangeDeliveries().get(0).getSituations().getPtSituationElements().get(0);
            } finally {
                time.stop();
            }
            if (ptSituationElement == null) {
                throw new IllegalArgumentException("No PtSituationElement element...");
            }
            metricsService.registerReceivedMessage(PtSituationElement.class);
            if (processPtSituationElement(ptSituationElement, timestamp)) {
                if (storeMessagesToFile) {
                    fileStorageService.writeToFile(ptSituationElement);
                }
            }
            metricsService.registerMessageDelay(MetricsService.HISTOGRAM_PROCESSED_DELAY, timestamp);
        } catch (Exception e) {
            //We always want to acknowlede so things don't end up on DLQ
            logger.error("Caught error during processing of exchange with expected PtSituationElement", e);
        }
    }

    private boolean processPtSituationElement(PtSituationElement ptSituationElement, ZonedDateTime timestamp) {
        AffectsScopeStructure affects = ptSituationElement.getAffects();
        if (affects == null) {
            logger.debug("Got PtSituationElement without any effects - nothing to notify");
            return false;
        }

        if (notValidNext24Hours(ptSituationElement.getValidityPeriods())) {
            logger.debug("Skips message that is not valid the next 24 hours (will be received again later) - situationNumber={}", getStringValue(ptSituationElement.getSituationNumber()));
            return false;
        }

        com.codahale.metrics.Timer timer = metricsService.getTimer(MetricsService.TIMER_SX_PROCESS);
        Timer.Context time = timer.time();
        try {
            AffectsScopeStructure.VehicleJourneys vehicleJourneys = affects.getVehicleJourneys();
            HashSet<Subscription> subscriptionsToNotify = new HashSet<>();
            int numberAffectedVehicleJourneys = 0;
            if (vehicleJourneys != null && vehicleJourneys.getAffectedVehicleJourneies() != null) {
                List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = vehicleJourneys.getAffectedVehicleJourneies();
                numberAffectedVehicleJourneys = affectedVehicleJourneies.size();
                HashSet<Subscription> affectedVehicleJourneySubscriptions = findAffectedSubscriptions(affectedVehicleJourneies);
                subscriptionsToNotify.addAll(affectedVehicleJourneySubscriptions);
            }
            HashSet<String> affectedLineRefs = findAffectedLineRefs(affects.getNetworks());
            HashSet<String> affectedStopPlaceRefs = findAffectedStopPlaceRefs(affects.getStopPlaces());
            logger.debug("Processes PtSituationElement ({}) - with {} StopPlaces, {} VehicleJourneys and {} LineRefs (networks)",
                    getStringValue(ptSituationElement.getSituationNumber()), affectedStopPlaceRefs.size(), numberAffectedVehicleJourneys, affectedLineRefs.size());
            for (String ref : affectedLineRefs) {
                subscriptionsToNotify.addAll(subscriptionManager.getSubscriptionsForLineRef(ref, SX));
            }
            for (String ref : affectedStopPlaceRefs) {
                subscriptionsToNotify.addAll(subscriptionManager.getSubscriptionsForStopPoint(ref, SX));
            }

            String codespace = getStringValue(ptSituationElement.getParticipantRef());
            if (codespace != null) {
                int before = subscriptionsToNotify.size();
                subscriptionsToNotify.removeIf(s -> !s.getCodespaces().isEmpty() && !s.getCodespaces().contains(codespace));
                if (logger.isDebugEnabled()) {
                    int after = subscriptionsToNotify.size();
                    if (after != before) {
                        logger.debug("Removed {} subscriptions that has limited codespace to something other than {}", (before-after), codespace);
                    }
                }
                Set<Subscription> subscriptionsForCodespace = subscriptionManager.getSubscriptionsForCodespace(codespace, SX);
                if (!subscriptionsForCodespace.isEmpty()) {
                    logger.debug("There are {} subscriptions on codespace (ParticipantRef) {}", subscriptionsForCodespace.size(), codespace);
                    subscriptionsToNotify.addAll(subscriptionsForCodespace);
                }
            } else {
                int before = subscriptionsToNotify.size();
                subscriptionsToNotify.removeIf(s -> !s.getCodespaces().isEmpty());
                int after = subscriptionsToNotify.size();
                logger.debug("No codespace (ParticipantRef) on ptSituationElement, removes {} subscriptions that specify codespaces", before-after);
            }
            logger.debug("There are {} subscriptions to notify", subscriptionsToNotify.size());
            if (!subscriptionsToNotify.isEmpty()) {
                subscriptionManager.notifySubscriptions(subscriptionsToNotify, ptSituationElement, timestamp);
            }
        } finally {
            long nanos = time.stop();
            logger.debug("Done processing PtSituationElement after {} ms", nanos/1000000);
        }
        return true;
    }

    private HashSet<String> findAffectedLineRefs(AffectsScopeStructure.Networks networks) {
        HashSet<String> affectedLineRefs = new HashSet<>();
        if (networks != null) {
            List<AffectsScopeStructure.Networks.AffectedNetwork> affectedNetworks = networks.getAffectedNetworks();
            for (AffectsScopeStructure.Networks.AffectedNetwork network : affectedNetworks) {
                List<AffectedLineStructure> affectedLines = network.getAffectedLines();
                for (AffectedLineStructure affectedLine : affectedLines) {
                    String lineRef = getStringValue(affectedLine.getLineRef());
                    if (StringUtils.isNotBlank(lineRef)) {
                        affectedLineRefs.add(lineRef);
                        //TODO: ROR-298: It is legal to specify route as done for AffectedVehicleJourney: we should respect stopconditons the same (reusable) way
                    }
                }
            }
        }
        return affectedLineRefs;
    }

    private boolean notValidNext24Hours(List<HalfOpenTimestampOutputRangeStructure> validityPeriods) {
        if (validityPeriods == null || validityPeriods.isEmpty()) {
            logger.trace("Has no validity period to check validity");
            return false;
        }

        ZonedDateTime earliest = null;
        for (HalfOpenTimestampOutputRangeStructure validityPeriod : validityPeriods) {
            ZonedDateTime startTime = validityPeriod.getStartTime();
            if (earliest == null || startTime.isBefore(earliest)) {
                earliest = startTime;
            }
        }
        return earliest.isAfter(ZonedDateTime.now().plusDays(1));
    }

    /**
     * Gå gjennom Affects|StopPoints og matche StopPointRef mot subscriptions
     * Gå gjennom Affects|StopPlacess og matche StopPlaceRef mot subscriptions <-- Litt usikker på denne, men tar med for nå
     * Gå gjennom Affects|VehicleJourneys|AffectedVehicleJourney|Route|StopPoints|AffectedStopPoint og
     */
    HashSet<String> findAffectedStopPlaceRefs(AffectsScopeStructure.StopPlaces stopPlaces) {
        HashSet<String> stopsToNotify = new HashSet<>();
        if (stopPlaces != null) {
            List<AffectedStopPlaceStructure> affectedStopPlaces = stopPlaces.getAffectedStopPlaces();
            for (AffectedStopPlaceStructure affectedStopPlace : affectedStopPlaces) {
                StopPlaceRef stopPlaceRef = affectedStopPlace.getStopPlaceRef();
                addStop(stopsToNotify, getStringValue(stopPlaceRef));
            }
        }
        return stopsToNotify;
    }

    HashSet<Subscription> findAffectedSubscriptions(List<AffectedVehicleJourneyStructure> affectedVehicleJourneies) {
        HashSet<Subscription> subscriptions = new HashSet<>();
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
                        addStop(orderedListOfStops, getStringValue(stopPointRef)); //TODO: ROR-298: legg til affectedStopPoint.getStopConditions() så de kan tas høyde for!
                        /*
                        StopCondition(s) angir hvilke passasjerer meldingen gjelder for (kan f.eks. brukes for å beskrive relevans for av- og påstigning):
                        - exceptionalStop (gjelder passasjerer som skal bytte)
                        - destination (gjelder ved ankomst eller for avstigende passasjerer)
                        - notStopping (gjelder ved passering, stopper ikke)
                        - requestStop (gjelder når det er stopp kun ved forespørsel)
                        - startPoint (gjelder ved avgang eller for påstigende passasjerer)
                        - stop (default - normal stoppestedsoperasjon, dvs gjelder både ankomst og avgang / avstigning og påstigning / bytte)
                         */
                    }
                }
                boolean hasCompleteRoute = !Boolean.TRUE.equals(stopPoints.isAffectedOnly());
                String lineRef = getStringValue(affectedVehicleJourney.getLineRef());
                //TODO: ideally the SX message should have all data we need to find affected subscription, but it doesn't.. Can look up against route-data (OTP does that to match SX messages with

                for (String stop : orderedListOfStops) {
                    Set<Subscription> subscriptionsForStopPoint = subscriptionManager.getSubscriptionsForStopPoint(stop, SX);
                    for (Subscription subscription : subscriptionsForStopPoint) {
                        Set<String> subscribedLines = subscription.getLineRefs();
                        if (subscribedLines.isEmpty() || (StringUtils.isNotBlank(lineRef) && subscribedLines.contains(lineRef)) ) {
                            if (!hasCompleteRoute) {
                                subscriptions.add(subscription);
                                //TODO: Hvis subscription på stopp og kun ett av dem funnet: ikke legge til for å unngå unødvendige meldinger - men vurder stopcondition også!
                                //TODO: ROR-298: Sjekk stopconditions!
                                logger.trace("Has only affected stops and don't find route in LiveRouteService, adds all subscriptions on these stops - regardless of direction");
                            } else {
                                if (affected(subscription, orderedListOfStops)) {
                                    subscriptions.add(subscription);
                                }
                            }
                        }
                    }
                }
                subscriptions.addAll(findSubscriptionsOnLineOrVehicle(lineRef));
            }
        }

        return subscriptions;
    }

    private HashSet<Subscription> findSubscriptionsOnLineOrVehicle(String lineRef) {
        HashSet<Subscription> subscriptions = new HashSet<>();
        if (StringUtils.isNotBlank(lineRef)) {
            Set<Subscription> subscriptionsForLineRef = subscriptionManager.getSubscriptionsForLineRef(lineRef, SX);
            logger.trace("Adds {} subscriptions regarding lineRef={}", subscriptionsForLineRef.size(), lineRef);
            subscriptions.addAll(subscriptionsForLineRef);
        }
        return subscriptions;
    }

    private boolean affected(Subscription subscription, List<String> orderedListOfStops) {
        //TODO: ROR-298: Sjekke stopconditions!
        int from = findIndexOfOne(subscription.getFromStopPoints(), orderedListOfStops);
        int to = findIndexOfOne(subscription.getToStopPoints(), orderedListOfStops);
        boolean affected = from > -1 && to > -1 && from < to;
        if (affected) {
            logger.trace("Affected subscription '{}' from {} to {}", subscription.getName(), orderedListOfStops.get(from), orderedListOfStops.get(to));
        }
        return affected;
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

