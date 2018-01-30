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
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.subscription.EstimatedCallAndSubscriptions;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
    private SubscriptionStatus status = new SubscriptionStatus();
    private SiriMarshaller siriMarshaller;

    @Autowired
    public NsbETSubscriptionProcessor(SubscriptionManager subscriptionManager, SiriMarshaller siriMarshaller) {
        this.siriMarshaller = siriMarshaller;
        logger.debug("Initializes...");
        this.subscriptionManager = subscriptionManager;
    }

    /**
     * Expects inputstream with XML with EstimatedVehicleJourney as root element.
     */
    @Override
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.trace("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
        EstimatedVehicleJourney estimatedVehicleJourney = siriMarshaller.unmarhall(xml, EstimatedVehicleJourney.class);
        if (estimatedVehicleJourney == null) {
            throw new IllegalArgumentException("No EstimatedVehicleJourney element...");
        }
        status.processed(EstimatedVehicleJourney.class);
        processEstimatedVehicleJourney(estimatedVehicleJourney);
    }

    @SuppressWarnings("unused") //Used from camel route
    public SubscriptionStatus getStatus() {
        return status;
    }

    private void processEstimatedVehicleJourney(EstimatedVehicleJourney estimatedVehicleJourney) {
        OperatorRefStructure operatorRef = estimatedVehicleJourney.getOperatorRef();
        boolean isNSB = operatorRef != null && "NSB".equalsIgnoreCase(operatorRef.getValue());
        if (!isNSB) {
            logger.trace("Skips estimatedVehicleJourney (not NSB)");
            return;
        }
        status.handled(EstimatedVehicleJourney.class);
        List<EstimatedCall> estimatedDelays = getEstimatedDelaysAndCancellations(estimatedVehicleJourney.getEstimatedCalls());
        logger.debug("Processes NSB estimatedVehicleJourney ({}) - with {} estimated delays", estimatedVehicleJourney.getDatedVehicleJourneyRef().getValue(), estimatedDelays.size());
        List<EstimatedCallAndSubscriptions> affectedSubscriptions = findAffectedSubscriptions(estimatedDelays, estimatedVehicleJourney);
        for (EstimatedCallAndSubscriptions estimatedCallAndSubscriptions : affectedSubscriptions) {
            EstimatedCall estimatedCall = estimatedCallAndSubscriptions.getEstimatedCall();
            HashSet<Subscription> subscriptions = estimatedCallAndSubscriptions.getSubscriptions();
            logger.debug(" - For delayed departure from stopPlace {} there are {} affected subscriptions ", estimatedCall.getStopPointRef().getValue(), subscriptions.size());
            subscriptionManager.notify(subscriptions, estimatedCall, estimatedVehicleJourney);
        }
    }

    private List<EstimatedCallAndSubscriptions> findAffectedSubscriptions(List<EstimatedCall> estimatedDelays, EstimatedVehicleJourney estimatedVehicleJourney) {
        ArrayList<EstimatedCallAndSubscriptions> affectedSubscriptions = new ArrayList<>();
        for (EstimatedCall estimatedDelay : estimatedDelays) {
            HashSet<Subscription> subscriptions = new HashSet<>();
            StopPointRef stopPointRef = estimatedDelay.getStopPointRef();
            if (stopPointRef != null && StringUtils.startsWithIgnoreCase(stopPointRef.getValue(), "NSR:")) {
                //Bryr oss kun om stopPointRef på "nasjonalt format"
                Set<Subscription> subs = subscriptionManager.getSubscriptionsForStopPoint(stopPointRef.getValue());
                for (Subscription sub : subs) {
                    if (validDirection(sub, estimatedVehicleJourney)) {
                        subscriptions.add(sub);
                    }
                    //TODO: Vi bør sjekke hvor stor forsinkelse og ha en nedre grense - mulig hver subscription skal ha det? Ved cancellation er ikke det relevant.
                    /*
                    TODO: For nå holder vi gyldighet på subscriptions utenfor...
                    Men hva vil egentlig trigge et "treff"?
                    - en avgang som normalt skulle gått(fra-StopPointRef)/ankommet(til-StopPointRef) innenfor tidsrommet
                    - en avgang som nå (forsinket) går(fra-StopPointRef)/ankommer(til-StopPointRef) innenfor tidsrommet
                     */
                }
            }
            if (!subscriptions.isEmpty()) {
                affectedSubscriptions.add(new EstimatedCallAndSubscriptions(estimatedDelay, subscriptions));
            }
        }
        return affectedSubscriptions;
    }

    protected boolean validDirection(Subscription subscription, EstimatedVehicleJourney journey) {
        HashMap<String, StopData> stops = new HashMap<>();
        if (journey.getRecordedCalls() != null && journey.getRecordedCalls().getRecordedCalls() != null) {
            for (RecordedCall call : journey.getRecordedCalls().getRecordedCalls()) {
                if (call.getStopPointRef() != null) {
                    stops.put(call.getStopPointRef().getValue(), new StopData(call.getAimedDepartureTime()));
                }
            }
        }
        if (journey.getEstimatedCalls() != null && journey.getEstimatedCalls().getEstimatedCalls() != null) {
            for (EstimatedCall call : journey.getEstimatedCalls().getEstimatedCalls()) {
                if (call.getStopPointRef() != null) {
                    stops.put(call.getStopPointRef().getValue(), new StopData(call.getAimedDepartureTime(),
                            call.getArrivalBoardingActivity(), call.getDepartureBoardingActivity()));
                }
            }
        }
        ZonedDateTime fromTime = findOne(stops, subscription.getFromStopPoints(), DIRECTION_FROM);
        ZonedDateTime toTime = findOne(stops, subscription.getToStopPoints(), DIRECTION_TO);
        //TODO: Tror kanskje denne logikken vil ha problemer med feks tbane-ringen...
        return fromTime != null && toTime != null && fromTime.isBefore(toTime);
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

    private class StopData {
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
