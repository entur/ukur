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
import org.entur.ukur.routedata.LiveRouteManager;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.EstimatedCallAndSubscriptions;
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

@Service
public class NsbETSubscriptionProcessor implements org.apache.camel.Processor {
    private static final int DIRECTION_FROM = 1;
    private static final int DIRECTION_TO = 2;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private SubscriptionManager subscriptionManager;
    private MetricsService metricsService;
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;

    private SiriMarshaller siriMarshaller;
    private LiveRouteManager liveRouteManager;
    private FileStorageService fileStorageService;
    @Value("${ukur.camel.et.store.files:false}")
    private boolean storeMessagesToFile = false;

    @Autowired
    public NsbETSubscriptionProcessor(SubscriptionManager subscriptionManager,
                                      SiriMarshaller siriMarshaller,
                                      LiveRouteManager liveRouteManager,
                                      FileStorageService fileStorageService,
                                      MetricsService metricsService,
                                      QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {
        this.siriMarshaller = siriMarshaller;
        this.liveRouteManager = liveRouteManager;
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
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.debug("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
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
    }

    protected boolean processEstimatedVehicleJourney(EstimatedVehicleJourney estimatedVehicleJourney) {
        OperatorRefStructure operatorRef = estimatedVehicleJourney.getOperatorRef();
        boolean isNSB = operatorRef != null && "NSB".equalsIgnoreCase(operatorRef.getValue());
        if (!isNSB) {
            logger.trace("Skips estimatedVehicleJourney (not NSB)");
            return false;
        }
        Timer timer = metricsService.getTimer(MetricsService.TIMER_ET_PROCESS);
        Timer.Context time = timer.time();
        try {
            liveRouteManager.updateJourney(estimatedVehicleJourney);
            List<EstimatedCall> deviations = getEstimatedDelaysAndCancellations(estimatedVehicleJourney.getEstimatedCalls());
            if (deviations.isEmpty()) {
                logger.trace("Processes NSB estimatedVehicleJourney ({}) - no estimated delays or cancellations", estimatedVehicleJourney.getDatedVehicleJourneyRef().getValue());
            } else {
                logger.debug("Processes NSB estimatedVehicleJourney ({}) - with {} estimated delays", estimatedVehicleJourney.getDatedVehicleJourneyRef().getValue(), deviations.size());
                List<EstimatedCallAndSubscriptions> affectedSubscriptions = findAffectedSubscriptions(deviations, estimatedVehicleJourney);
                String lineRef = estimatedVehicleJourney.getLineRef() == null ? null : estimatedVehicleJourney.getLineRef().getValue();
                String vehicleRef = estimatedVehicleJourney.getVehicleRef() == null ? null : estimatedVehicleJourney.getVehicleRef().getValue();
                for (EstimatedCallAndSubscriptions estimatedCallAndSubscriptions : affectedSubscriptions) {
                    HashSet<Subscription> subscriptions = estimatedCallAndSubscriptions.getSubscriptions();
                    subscriptions.removeIf(s -> notIncluded(lineRef, s.getLineRefs()) || notIncluded(vehicleRef, s.getVehicleRefs()));
                    EstimatedCall estimatedCall = estimatedCallAndSubscriptions.getEstimatedCall();
                    logger.debug(" - For delayed departure from stopPlace {} there are {} affected subscriptions ", estimatedCall.getStopPointRef().getValue(), subscriptions.size());
                    subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
                }
                HashSet<Subscription> subscriptionsOnLineOrVehicleRef = findSubscriptionsOnLineOrVehicleRef(lineRef, vehicleRef);
                if (!subscriptionsOnLineOrVehicleRef.isEmpty()) {
                    subscriptionManager.notifySubscriptionsWithFullMessage(subscriptionsOnLineOrVehicleRef, estimatedVehicleJourney);
                }
            }
        } finally {
            time.stop();
        }
        return true;
    }

    private boolean notIncluded(String value, Set<String> values) {
        return !values.isEmpty() && StringUtils.isNotBlank(value) && !values.contains(value);
    }

    private HashSet<Subscription> findSubscriptionsOnLineOrVehicleRef(String lineRef, String vehicleRef) {
        HashSet<Subscription> subscriptions = new HashSet<>();
        if (StringUtils.isNotBlank(lineRef)) {
            Set<Subscription> lineRefSubscriptions = subscriptionManager.getSubscriptionsForLineRef(lineRef);
            if (StringUtils.isNotBlank(vehicleRef)) {
                lineRefSubscriptions.removeIf(s->!s.getVehicleRefs().contains(vehicleRef));
            }
            subscriptions.addAll(lineRefSubscriptions);
        }
        if (StringUtils.isNotBlank(vehicleRef)) {
            Set<Subscription> vehicleRefSubscriptions = subscriptionManager.getSubscriptionsForvehicleRef(vehicleRef);
            if (StringUtils.isNotBlank(lineRef)) {
                vehicleRefSubscriptions.removeIf(s->!s.getVehicleRefs().contains(lineRef));
            }
            subscriptions.addAll(vehicleRefSubscriptions);
        }
        return subscriptions;
    }


    private List<EstimatedCallAndSubscriptions> findAffectedSubscriptions(List<EstimatedCall> estimatedDelays, EstimatedVehicleJourney estimatedVehicleJourney) {
        HashMap<String, StopData> stops = getStopData(estimatedVehicleJourney);
        ArrayList<EstimatedCallAndSubscriptions> affectedSubscriptions = new ArrayList<>();
        for (EstimatedCall estimatedDelay : estimatedDelays) {
            HashSet<Subscription> subscriptions = new HashSet<>();
            StopPointRef stopPointRef = estimatedDelay.getStopPointRef();
            if (stopPointRef != null && StringUtils.startsWithIgnoreCase(stopPointRef.getValue(), "NSR:")) {
                //Bryr oss kun om stopPointRef på "nasjonalt format"
                Set<Subscription> subs = subscriptionManager.getSubscriptionsForStopPoint(stopPointRef.getValue());
                for (Subscription sub : subs) {
                    if (validDirection(sub, stops)) {
                        subscriptions.add(sub);
                    }
                    /*
                    TODO: For nå holder vi gyldighet på subscriptions utenfor...
                    Men hva vil egentlig trigge et "treff"?
                    - en avgang som normalt skulle gått(fra-StopPointRef)/ankommet(til-StopPointRef) innenfor tidsrommet
                    - en avgang som nå (forsinket) går(fra-StopPointRef)/ankommer(til-StopPointRef) innenfor tidsrommet
                     */
                }
                //TODO: Må skille mellom to og from stops ved å se på arrival og departure status
            }
            if (!subscriptions.isEmpty()) {
                affectedSubscriptions.add(new EstimatedCallAndSubscriptions(estimatedDelay, subscriptions));
            }
        }
        return affectedSubscriptions;
    }

    protected boolean validDirection(Subscription subscription, HashMap<String, StopData> stops) {
        ZonedDateTime fromTime = findOne(stops, subscription.getFromStopPoints(), DIRECTION_FROM);
        ZonedDateTime toTime = findOne(stops, subscription.getToStopPoints(), DIRECTION_TO);
        //TODO: Tror kanskje denne logikken vil ha problemer med feks tbane-ringen...
        return fromTime != null && toTime != null && fromTime.isBefore(toTime);
    }

    protected HashMap<String, StopData> getStopData(EstimatedVehicleJourney journey) {
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

    private List<EstimatedCall> getEstimatedDelaysAndCancellations(EstimatedVehicleJourney.EstimatedCalls estimatedCalls) {
        List<EstimatedCall> delayed = new ArrayList<>();
        for (EstimatedCall estimatedCall : estimatedCalls.getEstimatedCalls()) {
            if (Boolean.TRUE.equals(estimatedCall.isCancellation())) {
                delayed.add(estimatedCall);
            } else if (estimatedCall.getDepartureStatus() == CallStatusEnumeration.DELAYED) {
                // Noen ganger har vi kun AimedDepartureTime (og ikke ExpectedDepartureTime) da vet vi ikke hvor stor forsinkelse...
                // Dette pga måten data blir samlet inn på (Anshar) - det vil komme en ny melding senere med ExpectedDepartureTime også
                if (estimatedCall.getExpectedDepartureTime() != null && estimatedCall.getAimedDepartureTime() != null) {
                    delayed.add(estimatedCall);
                } else {
                    logger.debug("Have a delayed EstimatedCall, but can't calculate delay - ignores it");
                }
            }
            //TODO: Må også se på  arrivalstatus så vi kan skille mellom to og from stops skikkelig
        }
        return delayed;
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
