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
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.DeviationType;
import org.entur.ukur.subscription.StopDetails;
import org.entur.ukur.subscription.StopDetailsAndSubscriptions;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.ArrivalBoardingActivityEnumeration;
import uk.org.siri.siri20.CallStatusEnumeration;
import uk.org.siri.siri20.DepartureBoardingActivityEnumeration;
import uk.org.siri.siri20.EstimatedCall;
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.RecordedCall;
import uk.org.siri.siri20.ServiceDelivery;
import uk.org.siri.siri20.ServiceFeatureRef;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.StopAssignmentStructure;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.entur.ukur.subscription.SubscriptionTypeEnum.ET;
import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;

@Service
public class ETSubscriptionProcessor implements org.apache.camel.Processor {
    private static final int DIRECTION_FROM = 1;
    private static final int DIRECTION_TO = 2;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private SubscriptionManager subscriptionManager;
    private MetricsService metricsService;
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;

    private SiriMarshaller siriMarshaller;
    private FileStorageService fileStorageService;
    @Value("${ukur.camel.et.store.files:false}")
    private boolean storeMessagesToFile = false;
    @Value("${ukur.camel.et.skipCallTimeChecks:false}")
    boolean skipCallTimeChecks = false;

    @Autowired
    public ETSubscriptionProcessor(SubscriptionManager subscriptionManager,
                                   SiriMarshaller siriMarshaller,
                                   FileStorageService fileStorageService,
                                   MetricsService metricsService,
                                   QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {
        this.siriMarshaller = siriMarshaller;
        this.fileStorageService = fileStorageService;
        this.subscriptionManager = subscriptionManager;
        this.metricsService = metricsService;
        this.quayAndStopPlaceMappingService = quayAndStopPlaceMappingService;
        logger.debug("Initializes...");
    }

    /**
     * Expects inputstream with XML with EstimatedVehicleJourney as root element.
     */
    @Override
    public void process(Exchange exchange) {
        try {
            InputStream xml = exchange.getIn().getBody(InputStream.class);
            logger.debug("Received XML with size {} bytes", xml.available());
            Timer timer = metricsService.getTimer(MetricsService.TIMER_ET_UNMARSHALL);
            Timer.Context time = timer.time();
            EstimatedVehicleJourney estimatedVehicleJourney;
            ZonedDateTime timestamp;
            try {
                Siri siri = siriMarshaller.unmarshall(xml, Siri.class);
                ServiceDelivery serviceDelivery = siri.getServiceDelivery();
                timestamp = serviceDelivery.getResponseTimestamp();
                estimatedVehicleJourney = serviceDelivery.getEstimatedTimetableDeliveries().get(0).getEstimatedJourneyVersionFrames().get(0).getEstimatedVehicleJourneies().get(0);
            } finally {
                time.stop();
            }
            if (estimatedVehicleJourney == null) {
                throw new IllegalArgumentException("No EstimatedVehicleJourney element...");
            }
            metricsService.registerReceivedMessage(EstimatedVehicleJourney.class);

            if (processEstimatedVehicleJourney(estimatedVehicleJourney, timestamp)) {
                if (storeMessagesToFile) {
                    fileStorageService.writeToFile(estimatedVehicleJourney);
                }
            }
            metricsService.registerMessageDelay(MetricsService.HISTOGRAM_PROCESSED_DELAY, timestamp);
        } catch (Exception e) {
            //We always want to acknowlede so things don't end up on DLQ
            logger.error("Caught error during processing of exchange with expected EstimatedVehicleJourney", e);
        }
    }

    boolean processEstimatedVehicleJourney(EstimatedVehicleJourney estimatedVehicleJourney, ZonedDateTime timestamp) {
        if (shouldIgnoreJourney(estimatedVehicleJourney)) {
            logger.debug("Ignores EstimatedVehicleJourney with LineRef {}", getStringValue(estimatedVehicleJourney.getLineRef()));
            metricsService.getMeter(MetricsService.METER_ET_IGNORED).mark();
            return false;
        }
        Timer timer = metricsService.getTimer(MetricsService.TIMER_ET_PROCESS);
        Timer.Context time = timer.time();
        try {
            List<StopDetails> deviations = getDeviations(estimatedVehicleJourney);
            if (deviations.isEmpty()) {
                logger.trace("Processes EstimatedVehicleJourney (LineRef={}, DatedVehicleJourneyRef={}) - no deviations", getStringValue(estimatedVehicleJourney.getLineRef()), getStringValue(estimatedVehicleJourney.getDatedVehicleJourneyRef()));
                metricsService.getMeter(MetricsService.METER_ET_WITHOUT_DEVIATIONS).mark();
            } else {
                logger.debug("Processes EstimatedVehicleJourney (LineRef={}, DatedVehicleJourneyRef={}) - with {} deviations", getStringValue(estimatedVehicleJourney.getLineRef()), getStringValue(estimatedVehicleJourney.getDatedVehicleJourneyRef()), deviations.size());
                metricsService.getMeter(MetricsService.METER_ET_WITH_DEVIATIONS).mark();
            }
            List<StopDetailsAndSubscriptions> affectedSubscriptions = findAffectedSubscriptions(deviations, estimatedVehicleJourney);
            String lineRef = getStringValue(estimatedVehicleJourney.getLineRef());
            String codespace = estimatedVehicleJourney.getDataSource();
            HashSet<Subscription> subscriptionsToNoNotify = new HashSet<>();
            for (StopDetailsAndSubscriptions stopDetailsAndSubscriptions : affectedSubscriptions) {
                StopDetails stopDetails = stopDetailsAndSubscriptions.getStopDetails();
                Duration delayDuration = stopDetails.getDelayDuration();

                HashSet<Subscription> subscriptions = stopDetailsAndSubscriptions.getSubscriptions();
                subscriptions.removeIf(s -> notIncluded(lineRef, s.getLineRefs()));
                subscriptions.removeIf(s -> notIncluded(codespace, s.getCodespaces()));
                if (delayDuration != null) {
                    subscriptions.removeIf(s -> delayedLessThan(delayDuration, s.getMinimumDelay()));
                }
                subscriptions.removeIf(s -> deviationTypeFilter(s.getDeviationType(),stopDetails.getDeviationTypes()));
                logger.debug(" - For stopPlace {} there are {} affected subscriptions ", stopDetails.getStopPointRef(), subscriptions.size());
                subscriptionsToNoNotify.addAll(subscriptions); //accumulates subscriptions as these are normally found twice (from and to)
            }
            subscriptionManager.notifySubscriptionsOnStops(subscriptionsToNoNotify, estimatedVehicleJourney, timestamp);
            HashSet<Subscription> subscriptionsOnLineRefOrCodespace = findSubscriptionsOnLineRefOrCodespace(lineRef, codespace);
            if (!subscriptionsOnLineRefOrCodespace.isEmpty()) {
                if (deviations.isEmpty()) {
                    //only send to subscriptions with isPushAllData=true
                    subscriptionsOnLineRefOrCodespace.removeIf(subscription -> !subscription.isPushAllData());
                }
                logger.debug(" - There are {} affected subscriptions on lineref={} or codespace={}", subscriptionsOnLineRefOrCodespace.size(), lineRef, codespace);
                subscriptionManager.notifySubscriptionsWithFullMessage(subscriptionsOnLineRefOrCodespace, estimatedVehicleJourney, timestamp);
            }
        } finally {
            long nanos = time.stop();
            logger.debug("Done processing EstimatedVehicleJourney after {} ms", nanos / 1000000);
        }
        return true;
    }

    private boolean deviationTypeFilter(DeviationType subscriptionDeviationType, Set<DeviationType> stopDetailsDeviationTypes) {
        return !subscriptionDeviationType.equals(DeviationType.ALL) && !stopDetailsDeviationTypes.contains(subscriptionDeviationType);

    }

    private boolean delayedLessThan(Duration delayDuration, Duration minimumDelay) {
        if (minimumDelay == null) {
            return false;
        } else {
            var minimumDelayTimeInMillis = minimumDelay.toMillis();
            var delayedArrivalInMillis = delayDuration.toMillis();
            return delayedArrivalInMillis <= minimumDelayTimeInMillis;
        }
    }

    private boolean shouldIgnoreJourney(EstimatedVehicleJourney estimatedVehicleJourney) {
        List<ServiceFeatureRef> serviceFeatureReves = estimatedVehicleJourney.getServiceFeatureReves();
        for (ServiceFeatureRef serviceFeature : serviceFeatureReves) {
            if (StringUtils.equalsIgnoreCase("freightTrain", getStringValue(serviceFeature))) {
                logger.trace("shouldIgnoreJourney returns true because the estimatedVehicleJourney regards a freightTrain");
                return true;
            }
        }
        return false;
    }

    private boolean notIncluded(String value, Set<String> values) {
        return !values.isEmpty() && StringUtils.isNotBlank(value) && !values.contains(value);
    }

    private HashSet<Subscription> findSubscriptionsOnLineRefOrCodespace(String lineRef, String codespace) {
        HashSet<Subscription> subscriptions = new HashSet<>();
        if (StringUtils.isNotBlank(lineRef)) {
            Set<Subscription> lineRefSubscriptions = subscriptionManager.getSubscriptionsForLineRef(lineRef, ET);
            lineRefSubscriptions.removeIf(s -> notIncluded(codespace, s.getCodespaces()));
            subscriptions.addAll(lineRefSubscriptions);
        }
        if (StringUtils.isNotBlank(codespace)) {
            Set<Subscription> codespaceSubscriptions = subscriptionManager.getSubscriptionsForCodespace(codespace, ET);
            codespaceSubscriptions.removeIf(s -> notIncluded(lineRef, s.getLineRefs()));
            subscriptions.addAll(codespaceSubscriptions);
        }
        return subscriptions;
    }


    private List<StopDetailsAndSubscriptions> findAffectedSubscriptions(List<StopDetails> deviations, EstimatedVehicleJourney estimatedVehicleJourney) {
        HashMap<String, StopData> stops = getStopData(estimatedVehicleJourney);
        ArrayList<StopDetailsAndSubscriptions> affectedSubscriptions = new ArrayList<>();
        for (StopDetails deviation : deviations) {
            HashSet<Subscription> subscriptions = new HashSet<>();
            String stopPoint = deviation.getStopPointRef();
            //TODO: we should possibly use stops from Destination/Arrival StopAssignment? Then a quay-only subscription could receive deviations on track-change as well (from and to the susbcribed quay)
            if (StringUtils.startsWithIgnoreCase(stopPoint, "NSR:")) {
                //Bryr oss kun om stopPointRef på "nasjonalt format"
                Set<Subscription> subs = subscriptionManager.getSubscriptionsForStopPoint(stopPoint, ET);
                for (Subscription sub : subs) {
                    if (validDirection(sub, stops)) {
                        if (!deviation.getDeviationTypes().isEmpty() || subscribedStopDelayed(sub, stopPoint, deviation)) {
                            subscriptions.add(sub);
                        }
                    }
                }
            }
            if (!subscriptions.isEmpty()) {
                affectedSubscriptions.add(new StopDetailsAndSubscriptions(deviation, subscriptions));
            }
        }

        //Get subscriptions with isPushAllData=true for all stops without deviations:
        Set<String> stopRefs = stops.keySet();
        stopRefs.removeAll(deviations.stream().map(StopDetails::getStopPointRef).collect(Collectors.toSet()));
        for (String stopRef : stopRefs) {
            HashSet<Subscription> subscriptions = new HashSet<>();
            Set<Subscription> subs = subscriptionManager.getSubscriptionsForStopPoint(stopRef, ET);
            for (Subscription sub : subs) {
                if (sub.isPushAllData() && validDirection(sub, stops)) {
                    subscriptions.add(sub);
                }
            }
            if (!subscriptions.isEmpty()) {
                affectedSubscriptions.add(new StopDetailsAndSubscriptions(new StopDetails(stopRef), subscriptions));
            }
        }

        return affectedSubscriptions;
    }

    private boolean subscribedStopDelayed(Subscription sub, String stopPoint, StopDetails deviation) {
        if ((sub.getFromStopPoints().contains(stopPoint) && deviation.isDelayedDeparture()) ||
                (sub.getToStopPoints().contains(stopPoint) && deviation.isDelayedArrival())) {
            return true;
        }

        if (stopPoint.startsWith("NSR:Quay:")) {
            String stopPlaceId = quayAndStopPlaceMappingService.mapQuayToStopPlace(stopPoint);
            if (stopPlaceId != null) {
                return (sub.getFromStopPoints().contains(stopPlaceId) && deviation.isDelayedDeparture()) ||
                        (sub.getToStopPoints().contains(stopPlaceId) && deviation.isDelayedArrival());
            }
        }

        return false;
    }

    boolean validDirection(Subscription subscription, HashMap<String, StopData> stops) {
        ZonedDateTime fromTime = findOne(stops, subscription.getFromStopPoints(), DIRECTION_FROM);
        ZonedDateTime toTime = findOne(stops, subscription.getToStopPoints(), DIRECTION_TO);
        return fromTime != null && toTime != null && fromTime.isBefore(toTime);
    }

    HashMap<String, StopData> getStopData(EstimatedVehicleJourney journey) {
        HashMap<String, StopData> stops = new HashMap<>();
        if (journey.getRecordedCalls() != null && journey.getRecordedCalls().getRecordedCalls() != null) {
            for (RecordedCall call : journey.getRecordedCalls().getRecordedCalls()) {
                if (call.getStopPointRef() != null) {

                    /*
                     * Cannot rely only on departure-time - subscription TO last stop must also be allowed.
                     * Using aimedArrival when aimedDeparture is not set.
                     */
                    ZonedDateTime time;
                    if (call.getAimedDepartureTime() != null) {
                        time = call.getAimedDepartureTime();
                    } else {
                        time = call.getAimedArrivalTime();
                    }

                    StopData data = new StopData(time);
                    String stopPointRef = call.getStopPointRef().getValue();
                    stops.put(stopPointRef, data);
                }
            }
        }
        if (journey.getEstimatedCalls() != null && journey.getEstimatedCalls().getEstimatedCalls() != null) {
            for (EstimatedCall call : journey.getEstimatedCalls().getEstimatedCalls()) {
                if (call.getStopPointRef() != null) {

                    /*
                     * Cannot rely only on departure-time - subscription TO last stop must also be allowed.
                     * Using aimedArrival when aimedDeparture is not set.
                     */
                    ZonedDateTime time;
                    if (call.getAimedDepartureTime() != null) {
                        time = call.getAimedDepartureTime();
                    } else {
                        time = call.getAimedArrivalTime();
                    }

                    StopData data = new StopData(time,
                            call.getArrivalBoardingActivity(), call.getArrivalStatus(),
                            call.getDepartureBoardingActivity(), call.getDepartureStatus());
                    String stopPointRef = call.getStopPointRef().getValue();
                    stops.put(stopPointRef, data);
                }
            }
        }
        HashMap<String, StopData> mappedStops = new HashMap<>();
        for (Map.Entry<String, StopData> entry : stops.entrySet()) {
            String stopPointId = entry.getKey();
            if (stopPointId.startsWith("NSR:Quay:")) {
                String stopPlaceId = quayAndStopPlaceMappingService.mapQuayToStopPlace(stopPointId);
                if (stopPlaceId != null) {
                    mappedStops.put(stopPlaceId, entry.getValue());
                }
            }
        }
        stops.putAll(mappedStops);
        return stops;
    }

    private List<StopDetails> getDeviations(EstimatedVehicleJourney estimatedVehicleJourney) {
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = estimatedVehicleJourney.getEstimatedCalls();
        boolean cancelledJourney = Boolean.TRUE.equals(estimatedVehicleJourney.isCancellation());
        List<StopDetails> deviations = new ArrayList<>();
        if (estimatedCalls != null && estimatedCalls.getEstimatedCalls() != null) {
            for (EstimatedCall call : estimatedCalls.getEstimatedCalls()) {
                if (futureEstimatedCall(call)) {
                    if (cancelledJourney || Boolean.TRUE.equals(call.isCancellation())) {
                        deviations.add(StopDetails.cancelled(getStringValue(call.getStopPointRef())));
                    } else if (isTrackChange(call)) {
                        deviations.add(StopDetails.trackChange(getStringValue(call.getStopPointRef())));
                    } else {
                        boolean delayedDeparture = call.getDepartureStatus() == CallStatusEnumeration.DELAYED || isDelayed(call.getAimedDepartureTime(), call.getExpectedDepartureTime());
                        boolean delayedArrival = call.getArrivalStatus() == CallStatusEnumeration.DELAYED || isDelayed(call.getAimedArrivalTime(), call.getExpectedArrivalTime());
                        final Duration delayedArrivalDuration = getDelayedArrivalDuration(call.getAimedArrivalTime(), call.getExpectedArrivalTime());
                        if (delayedArrival || delayedDeparture) {
                            deviations.add(StopDetails.delayed(getStringValue(call.getStopPointRef()), delayedDeparture, delayedArrival, delayedArrivalDuration));
                        }
                    }
                }
            }
        }
        return deviations;
    }

    private boolean isTrackChange(EstimatedCall call) {
        StopAssignmentStructure stopAssignment = call.getArrivalStopAssignment();
        if (stopAssignment == null) {
            //According to the profile, only one of arrival- or departureStopAssigment can be set - our logic most often populate arrival
            stopAssignment = call.getDepartureStopAssignment();
        }
        if (stopAssignment != null && stopAssignment.getAimedQuayRef() != null && stopAssignment.getExpectedQuayRef() != null) {
            return !StringUtils.equals(stopAssignment.getAimedQuayRef().getValue(), stopAssignment.getExpectedQuayRef().getValue());
        }
        return false;
    }

    private boolean futureEstimatedCall(EstimatedCall call) {
        if (skipCallTimeChecks) {
            return true;
        }
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime expected = call.getExpectedDepartureTime();
        ZonedDateTime aimed = call.getAimedDepartureTime();
        if (expected == null && aimed == null) {
            //no departure on last stop
            expected = call.getExpectedArrivalTime();
            aimed = call.getAimedArrivalTime();
        }

        if (expected != null) {
            return now.isBefore(expected);
        } else {
            if (aimed != null) {
                return now.isBefore(aimed);
            } else {
                return false;
            }
        }
    }

    private Duration getDelayedArrivalDuration(ZonedDateTime aimed, ZonedDateTime expected) {
        if (aimed != null && expected != null) {
            final var isAfter = expected.isAfter(aimed);
            if (isAfter) {
                return Duration.between(aimed, expected);
            }
            return null;
        }
        return null;
    }

    private boolean isDelayed(ZonedDateTime aimed, ZonedDateTime expected) {
        return false;
    }

    private ZonedDateTime findOne(HashMap<String, StopData> stops, Set<String> fromStopPoints, int direction) {
        for (String fromStopPoint : fromStopPoints) {
            StopData stopData = stops.get(fromStopPoint);
            if (stopData != null) {
                switch (direction) {
                    case DIRECTION_FROM:
                        CallStatusEnumeration departureStatus = stopData.getDepartureStatus();
                        if (departureStatus != null && departureStatus != CallStatusEnumeration.CANCELLED) {

                            //Departure has NOT been cancelled - check departure-activity

                            DepartureBoardingActivityEnumeration depActivity = stopData.getDepartureBoardingActivity();
                            if (depActivity != null && depActivity != DepartureBoardingActivityEnumeration.BOARDING) {
                                logger.debug("skips FROM StopPoint {} as it is not boarding for departure", fromStopPoint);
                                return null;
                            }
                        }
                        break;
                    case DIRECTION_TO:
                        CallStatusEnumeration arrivalStatus = stopData.getArrivalStatus();
                        if (arrivalStatus != null && arrivalStatus != CallStatusEnumeration.CANCELLED) {

                            //Arrival has NOT been cancelled - check arrival-activity

                            ArrivalBoardingActivityEnumeration arrActivity = stopData.getArrivalBoardingActivity();
                            if (arrActivity != null && arrActivity != ArrivalBoardingActivityEnumeration.ALIGHTING) {
                                logger.debug("skips TO StopPoint {} as it is not alighting at arrival", fromStopPoint);
                                return null;
                            }
                        }
                        break;
                }
                return stopData.getAimedDepartureTime();
            }
        }
        return null;
    }

    class StopData {
        private final ZonedDateTime aimedDepartureTime;
        private final ArrivalBoardingActivityEnumeration arrivalBoardingActivity;
        private final CallStatusEnumeration arrivalStatus;
        private final DepartureBoardingActivityEnumeration departureBoardingActivity;
        private final CallStatusEnumeration departureStatus;

        StopData(ZonedDateTime aimedDepartureTime) {
            this(aimedDepartureTime, null, null, null, null);
        }

        StopData(ZonedDateTime aimedDepartureTime,
                 ArrivalBoardingActivityEnumeration arrivalBoardingActivity,
                 CallStatusEnumeration arrivalStatus,
                 DepartureBoardingActivityEnumeration departureBoardingActivity,
                 CallStatusEnumeration departureStatus) {
            this.aimedDepartureTime = aimedDepartureTime;
            this.arrivalBoardingActivity = arrivalBoardingActivity;
            this.arrivalStatus = arrivalStatus;
            this.departureBoardingActivity = departureBoardingActivity;
            this.departureStatus = departureStatus;
        }

        ZonedDateTime getAimedDepartureTime() {
            return aimedDepartureTime;
        }

        ArrivalBoardingActivityEnumeration getArrivalBoardingActivity() {
            return arrivalBoardingActivity;
        }

        public CallStatusEnumeration getArrivalStatus() {
            return arrivalStatus;
        }

        DepartureBoardingActivityEnumeration getDepartureBoardingActivity() {
            return departureBoardingActivity;
        }

        public CallStatusEnumeration getDepartureStatus() {
            return departureStatus;
        }
    }

}
