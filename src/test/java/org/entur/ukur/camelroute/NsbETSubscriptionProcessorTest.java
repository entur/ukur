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

import org.entur.ukur.routedata.LiveRouteManager;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class NsbETSubscriptionProcessorTest {


    @Test
    public void validDirection() throws JAXBException {

        EstimatedVehicleJourney.RecordedCalls recordedCalls = new EstimatedVehicleJourney.RecordedCalls();
        addRecordedCall(recordedCalls, "R1", ZonedDateTime.now().minus(2, ChronoUnit.HOURS));
        addRecordedCall(recordedCalls, "R2", ZonedDateTime.now().minus(1, ChronoUnit.HOURS));
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addEstimatedCall(estimatedCalls, "E1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        addEstimatedCall(estimatedCalls, "E2", ZonedDateTime.now().plus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setRecordedCalls(recordedCalls);
        journey.setEstimatedCalls(estimatedCalls);

        NsbETSubscriptionProcessor processor = new NsbETSubscriptionProcessor(mock(SubscriptionManager.class),
                new SiriMarshaller(), mock(LiveRouteManager.class), mock(FileStorageService.class), mock(MetricsService.class));

        //No errors if no hits...
        assertFalse(processor.validDirection(new Subscription(), journey));

        //Only to in journey
        assertFalse(processor.validDirection(createSubscription("X", "E2"), journey));

        //Only from in journey
        assertFalse(processor.validDirection(createSubscription("E2", "X"), journey));

        //To and from in correct order in estimated calls
        assertTrue(processor.validDirection(createSubscription("E1", "E2"), journey));

        //To and from in opposite order in estimated calls
        assertFalse(processor.validDirection(createSubscription("E2", "E1"), journey));

        //correct order: to in estimated calls, from in recorded calls
        assertTrue(processor.validDirection(createSubscription("R1", "E2"), journey));

        //opposite order: to in estimated calls, from in recorded calls
        assertFalse(processor.validDirection(createSubscription("E1", "R1"), journey));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void processEstimatedVehicleJourney() throws JAXBException {

        EstimatedVehicleJourney.RecordedCalls recordedCalls = new EstimatedVehicleJourney.RecordedCalls();
        addRecordedCall(recordedCalls, "R1", ZonedDateTime.now().minus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addDelayedEstimatedCall(estimatedCalls, "E1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        addDelayedEstimatedCall(estimatedCalls, "E2", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setRecordedCalls(recordedCalls);
        journey.setEstimatedCalls(estimatedCalls);
        VehicleRef vehicleRef = new VehicleRef();
        vehicleRef.setValue("1234");
        journey.setVehicleRef(vehicleRef);
        LineRef lineRef = new LineRef();
        lineRef.setValue("NSB:Line:1");
        journey.setLineRef(lineRef);
        OperatorRefStructure operatorRef = new OperatorRefStructure();
        operatorRef.setValue("NSB");
        journey.setOperatorRef(operatorRef);
        journey.setDatedVehicleJourneyRef(new DatedVehicleJourneyRef());

        Set<Subscription> subscriptionsForStopPoint = new HashSet<>();
        //Expects these to be found:
        Subscription s_R1_E1 = createSubscription(subscriptionsForStopPoint, "R1", "E1", null, null);
        Subscription s_R1_E1_v = createSubscription(subscriptionsForStopPoint, "R1", "E1", "1234", null);
        Subscription s_R1_E1_v_l = createSubscription(subscriptionsForStopPoint, "R1", "E1", "1234", "NSB:Line:1");
        Subscription s_R1_E1_l = createSubscription(subscriptionsForStopPoint, "R1", "E1", null, "NSB:Line:1");
        Subscription s_l_v = createSubscription(subscriptionsForStopPoint, null, null, "1234", "NSB:Line:1");
        //These should not be found:
        Subscription s_l = createSubscription(subscriptionsForStopPoint, null, null, null, "NSB:Line:1");
        Subscription s_v = createSubscription(subscriptionsForStopPoint, null, null, "1234", null);
        createSubscription(subscriptionsForStopPoint, "E1", "R1", null, null);
        createSubscription(subscriptionsForStopPoint, "R1", "E1", "4444", "NSB:Line:2");
        Subscription s_l_vx = createSubscription(subscriptionsForStopPoint, null, null, "4444", "NSB:Line:1");
        Subscription s_lx_v = createSubscription(subscriptionsForStopPoint, null, null, "1234", "NSB:Line:2");
        createSubscription(subscriptionsForStopPoint, "x1", "E1", "1234", "NSB:Line:1");
        createSubscription(subscriptionsForStopPoint, "R1", "x1", "1234", "NSB:Line:1");

        SubscriptionManager subscriptionManagerMock = mock(SubscriptionManager.class); //must be somewhat carefull so we don't spend to much time testing the mock...
        when((subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:StopPlace:E1"))).thenReturn(subscriptionsForStopPoint);
        when((subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:StopPlace:R1"))).thenReturn(subscriptionsForStopPoint);
        when((subscriptionManagerMock.getSubscriptionsForLineRef("NSB:Line:1"))).thenReturn(new HashSet<>(Arrays.asList(s_l, s_l_v, s_l_vx)));
        when((subscriptionManagerMock.getSubscriptionsForvehicleRef("1234"))).thenReturn(new HashSet<>(Arrays.asList(s_l_v, s_v, s_lx_v)));

        NsbETSubscriptionProcessor processor = new NsbETSubscriptionProcessor(subscriptionManagerMock,
                new SiriMarshaller(), mock(LiveRouteManager.class), mock(FileStorageService.class), new MetricsService(null, 0));

        ArgumentCaptor<HashSet> subscriptionsOnStopsCaptor= ArgumentCaptor.forClass(HashSet.class);
        ArgumentCaptor<HashSet> subscriptionsOnLineOrVehicleJourneyCaptor= ArgumentCaptor.forClass(HashSet.class);
        assertTrue(processor.processEstimatedVehicleJourney(journey));
        verify(subscriptionManagerMock).notifySubscriptionsOnStops(subscriptionsOnStopsCaptor.capture(), eq(journey));
        verify(subscriptionManagerMock).notifySubscriptionsWithFullMessage(subscriptionsOnLineOrVehicleJourneyCaptor.capture(), eq(journey));
        HashSet<Subscription> notifiedSubscriptionsOnStops = subscriptionsOnStopsCaptor.getValue();
        assertEquals(4, notifiedSubscriptionsOnStops.size());
        assertTrue(notifiedSubscriptionsOnStops.contains(s_R1_E1));
        assertTrue(notifiedSubscriptionsOnStops.contains(s_R1_E1_v));
        assertTrue(notifiedSubscriptionsOnStops.contains(s_R1_E1_v_l));
        assertTrue(notifiedSubscriptionsOnStops.contains(s_R1_E1_l));
        HashSet<Subscription> notifiedSubscriptionsWithFullMessage = subscriptionsOnLineOrVehicleJourneyCaptor.getValue();
        assertEquals(1, notifiedSubscriptionsWithFullMessage.size());
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(s_l_v));
    }

    private int subscriptionCounter = 0;

    private Subscription createSubscription(Set<Subscription> subscriptions, String from, String to, String vehicleJourney, String line) {
        Subscription subscription = new Subscription();
        subscription.setId(Integer.toString(subscriptionCounter++));
        if (from != null) {
            subscription.addFromStopPoint("NSR:StopPlace:"+from);
        }
        if (to != null) {
            subscription.addToStopPoint("NSR:StopPlace:"+to);
        }
        if (vehicleJourney != null) {
            subscription.addVehicleRef(vehicleJourney);
        }
        if (line!= null) {
            subscription.addLineRef(line);
        }
        if (subscriptions != null) {
            subscriptions.add(subscription);
        }
        return subscription;
    }

    private Subscription createSubscription(String from, String to) {
        return createSubscription(null, from, to, null, null);
    }

    private void addDelayedEstimatedCall(EstimatedVehicleJourney.EstimatedCalls estimatedCalls, String stopPointRef, ZonedDateTime time) {
        EstimatedCall estimatedCall = new EstimatedCall();
        StopPointRef ref = new StopPointRef();
        ref.setValue("NSR:StopPlace:"+stopPointRef);
        estimatedCall.setStopPointRef(ref);
        estimatedCall.setAimedDepartureTime(time);
        estimatedCall.setExpectedDepartureTime(time.plusMinutes(5));
        estimatedCall.setDepartureStatus(CallStatusEnumeration.DELAYED);
        estimatedCall.setAimedArrivalTime(time);
        estimatedCall.setExpectedArrivalTime(time.plusMinutes(5));
        estimatedCall.setArrivalStatus(CallStatusEnumeration.DELAYED);
        estimatedCalls.getEstimatedCalls().add(estimatedCall);
    }

    private void addEstimatedCall(EstimatedVehicleJourney.EstimatedCalls estimatedCalls, String stopPointRef, ZonedDateTime departureTime) {
        EstimatedCall estimatedCall = new EstimatedCall();
        StopPointRef ref = new StopPointRef();
        ref.setValue("NSR:StopPlace:"+stopPointRef);
        estimatedCall.setStopPointRef(ref);
        estimatedCall.setAimedDepartureTime(departureTime);
        estimatedCalls.getEstimatedCalls().add(estimatedCall);
    }

    private void addRecordedCall(EstimatedVehicleJourney.RecordedCalls recordedCalls, String stopPointRef, ZonedDateTime departureTime) {
        RecordedCall recordedCall = new RecordedCall();
        StopPointRef ref = new StopPointRef();
        ref.setValue("NSR:StopPlace:"+stopPointRef);
        recordedCall.setStopPointRef(ref);
        recordedCall.setAimedDepartureTime(departureTime);
        recordedCalls.getRecordedCalls().add(recordedCall);
    }
}