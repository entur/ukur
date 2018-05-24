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
import org.entur.ukur.subscription.DeviatingStop;
import org.entur.ukur.subscription.DeviatingStopAndSubscriptions;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.*;

import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;

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
            logger.debug("Reveived XML with size {} bytes", xml.available());
            Timer timer = metricsService.getTimer(MetricsService.TIMER_ET_UNMARSHALL);
            Timer.Context time = timer.time();
            EstimatedVehicleJourney estimatedVehicleJourney;
            try {
                estimatedVehicleJourney = siriMarshaller.unmarshall(xml, EstimatedVehicleJourney.class);
            } finally {
                time.stop();
            }
            if (estimatedVehicleJourney == null) {
                throw new IllegalArgumentException("No EstimatedVehicleJourney element...");
            }
            metricsService.registerReceivedMessage(EstimatedVehicleJourney.class);

            if (processEstimatedVehicleJourney(estimatedVehicleJourney)) {
                if (storeMessagesToFile) {
                    fileStorageService.writeToFile(estimatedVehicleJourney);
                }
            }
        } catch (Exception e) {
            //We always want to acknowlede so things don't end up on DLQ
            logger.error("Caught error during processing of exchange with expected EstimatedVehicleJourney", e);
        }
    }

    boolean processEstimatedVehicleJourney(EstimatedVehicleJourney estimatedVehicleJourney) {
        if (shouldIgnoreJourney(estimatedVehicleJourney)) {
            logger.debug("Ignores EstimatedVehicleJourney with LineRef {}", getStringValue(estimatedVehicleJourney.getLineRef()));
            metricsService.getMeter(MetricsService.METER_ET_IGNORED).mark();
            return false;
        }
        Timer timer = metricsService.getTimer(MetricsService.TIMER_ET_PROCESS);
        Timer.Context time = timer.time();
        try {
            List<DeviatingStop> deviations = getEstimatedDelaysAndCancellations(estimatedVehicleJourney);
            if (deviations.isEmpty()) {
                logger.trace("Processes EstimatedVehicleJourney (LineRef={}, DatedVehicleJourneyRef={}) - no estimated delays or cancellations", getStringValue(estimatedVehicleJourney.getLineRef()), getStringValue(estimatedVehicleJourney.getDatedVehicleJourneyRef()));
                metricsService.getMeter(MetricsService.METER_ET_WITHOUT_DEVIATIONS).mark();
            } else {
                logger.debug("Processes EstimatedVehicleJourney (LineRef={}, DatedVehicleJourneyRef={}) - with {} estimated delays", getStringValue(estimatedVehicleJourney.getLineRef()), getStringValue(estimatedVehicleJourney.getDatedVehicleJourneyRef()), deviations.size());
                metricsService.getMeter(MetricsService.METER_ET_WITH_DEVIATIONS).mark();
                List<DeviatingStopAndSubscriptions> affectedSubscriptions = findAffectedSubscriptions(deviations, estimatedVehicleJourney);
                String lineRef = getStringValue(estimatedVehicleJourney.getLineRef());
                String codespace = estimatedVehicleJourney.getDataSource();
                HashSet<Subscription> subscriptionsToNoNotify = new HashSet<>();
                for (DeviatingStopAndSubscriptions deviatingStopAndSubscriptions : affectedSubscriptions) {
                    HashSet<Subscription> subscriptions = deviatingStopAndSubscriptions.getSubscriptions();
                    subscriptions.removeIf(s -> notIncluded(lineRef, s.getLineRefs()));
                    subscriptions.removeIf(s -> notIncluded(codespace, s.getCodespaces()));
                    DeviatingStop stop = deviatingStopAndSubscriptions.getDeviatingStop();
                    logger.debug(" - For delayed/cancelled departure from stopPlace {} there are {} affected subscriptions ", stop.getStopPointRef(), subscriptions.size());
                    subscriptionsToNoNotify.addAll(subscriptions); //accumulates subscriptions as these are normally found twice (from and to)
                }
                subscriptionManager.notifySubscriptionsOnStops(subscriptionsToNoNotify, estimatedVehicleJourney);
                HashSet<Subscription> subscriptionsOnLineRefOrCodespace = findSubscriptionsOnLineRefOrCodespace(lineRef, codespace);
                if (!subscriptionsOnLineRefOrCodespace.isEmpty()) {
                    logger.debug(" - There are {} affected subscriptions on lineref={} or codespace={}", subscriptionsOnLineRefOrCodespace.size(), lineRef, codespace);
                    subscriptionManager.notifySubscriptionsWithFullMessage(subscriptionsOnLineRefOrCodespace, estimatedVehicleJourney);
                }
            }
        } finally {
            long nanos = time.stop();
            logger.debug("Done processing EstimatedVehicleJourney after {} ms", nanos/1000000);
        }
        return true;
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


    private List<DeviatingStopAndSubscriptions> findAffectedSubscriptions(List<DeviatingStop> deviations, EstimatedVehicleJourney estimatedVehicleJourney) {
        HashMap<String, StopData> stops = getStopData(estimatedVehicleJourney);
        ArrayList<DeviatingStopAndSubscriptions> affectedSubscriptions = new ArrayList<>();
        for (DeviatingStop deviation : deviations) {
            HashSet<Subscription> subscriptions = new HashSet<>();
            String stopPoint = deviation.getStopPointRef();
            if (StringUtils.startsWithIgnoreCase(stopPoint, "NSR:")) {
                //Bryr oss kun om stopPointRef på "nasjonalt format"
                Set<Subscription> subs = subscriptionManager.getSubscriptionsForStopPoint(stopPoint, ET);
                for (Subscription sub : subs) {
                    if (validDirection(sub, stops)) {
                        if ( deviation.isCancelled() || subscripbedStopDelayed(sub, stopPoint, deviation) ) {
                            subscriptions.add(sub);
                        }
                    }
                }
            }
            if (!subscriptions.isEmpty()) {
                affectedSubscriptions.add(new DeviatingStopAndSubscriptions(deviation, subscriptions));
            }
        }
        return affectedSubscriptions;
    }

    private boolean subscripbedStopDelayed(Subscription sub, String stopPoint, DeviatingStop deviation) {
        if ( (sub.getFromStopPoints().contains(stopPoint) && deviation.isDelayedDeparture()) ||
                (sub.getToStopPoints().contains(stopPoint) && deviation.isDelayedArrival()) ) {
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
                    StopData data = new StopData(call.getAimedDepartureTime());
                    String stopPointRef = call.getStopPointRef().getValue();
                    stops.put(stopPointRef, data);
                }
            }
        }
        if (journey.getEstimatedCalls() != null && journey.getEstimatedCalls().getEstimatedCalls() != null) {
            for (EstimatedCall call : journey.getEstimatedCalls().getEstimatedCalls()) {
                if (call.getStopPointRef() != null) {
                    StopData data = new StopData(call.getAimedDepartureTime(),
                            call.getArrivalBoardingActivity(), call.getDepartureBoardingActivity());
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

    private List<DeviatingStop> getEstimatedDelaysAndCancellations(EstimatedVehicleJourney estimatedVehicleJourney) {
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = estimatedVehicleJourney.getEstimatedCalls();
        boolean cancelledJourney = Boolean.TRUE.equals(estimatedVehicleJourney.isCancellation());
        List<DeviatingStop> deviations = new ArrayList<>();
        for (EstimatedCall call : estimatedCalls.getEstimatedCalls()) {
            if (futureEstimatedCall(call)) {
                if (cancelledJourney || Boolean.TRUE.equals(call.isCancellation())) {
                    deviations.add(DeviatingStop.cancelled(getStringValue(call.getStopPointRef())));
                } else {
                    boolean delayedDeparture = call.getDepartureStatus() == CallStatusEnumeration.DELAYED || isDelayed(call.getAimedDepartureTime(), call.getExpectedDepartureTime());
                    boolean delayedArrival = call.getArrivalStatus() == CallStatusEnumeration.DELAYED || isDelayed(call.getAimedArrivalTime(), call.getExpectedArrivalTime());
                    if (delayedArrival || delayedDeparture) {
                        deviations.add(DeviatingStop.delayed(getStringValue(call.getStopPointRef()), delayedDeparture, delayedArrival));
                    }
                }
            }
        }
        return deviations;
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

    private boolean isDelayed(ZonedDateTime aimed, ZonedDateTime expected) {
        if (aimed != null && expected != null) {
            return expected.isAfter(aimed);
        }
        return false;
    }

    //TODO: case of stop ids given are relevant... That's not nessecary!
    private ZonedDateTime findOne(HashMap<String, StopData> stops, Set<String> fromStopPoints, int direction) {
        for (String fromStopPoint : fromStopPoints) {
            StopData stopData = stops.get(fromStopPoint);
            if (stopData != null) {
                switch (direction) {
                    case DIRECTION_FROM:
                        DepartureBoardingActivityEnumeration depActivity = stopData.getDepartureBoardingActivity();
                        if (depActivity != null && depActivity != DepartureBoardingActivityEnumeration.BOARDING) {
                            logger.debug("skips FROM StopPoint as it is not boarding for departure");
                            return null;
                        }
                        break;
                    case DIRECTION_TO:
                        ArrivalBoardingActivityEnumeration arrActivity = stopData.getArrivalBoardingActivity();
                        if (arrActivity != null && arrActivity != ArrivalBoardingActivityEnumeration.ALIGHTING) {
                            logger.debug("skips TO StopPoint as it is not alighting at arrival");
                            return null;
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
        private final DepartureBoardingActivityEnumeration departureBoardingActivity;

        StopData(ZonedDateTime aimedDepartureTime) {
            this(aimedDepartureTime, null, null);
        }

        StopData(ZonedDateTime aimedDepartureTime,
                        ArrivalBoardingActivityEnumeration arrivalBoardingActivity,
                        DepartureBoardingActivityEnumeration departureBoardingActivity) {
            this.aimedDepartureTime = aimedDepartureTime;
            this.arrivalBoardingActivity = arrivalBoardingActivity;
            this.departureBoardingActivity = departureBoardingActivity;
        }

        ZonedDateTime getAimedDepartureTime() {
            return aimedDepartureTime;
        }

        ArrivalBoardingActivityEnumeration getArrivalBoardingActivity() {
            return arrivalBoardingActivity;
        }

        DepartureBoardingActivityEnumeration getDepartureBoardingActivity() {
            return departureBoardingActivity;
        }
    }

}
