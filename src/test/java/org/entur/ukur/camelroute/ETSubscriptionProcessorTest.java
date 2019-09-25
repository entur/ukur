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

import com.google.common.collect.Sets;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.DeviationType;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.entur.ukur.subscription.SubscriptionTypeEnum.ET;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ETSubscriptionProcessorTest {


    private int subscriptionCounter = 0;

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

        ETSubscriptionProcessor processor = new ETSubscriptionProcessor(mock(SubscriptionManager.class),
                new SiriMarshaller(), mock(FileStorageService.class),
                mock(MetricsService.class), mock(QuayAndStopPlaceMappingService.class));

        HashMap<String, ETSubscriptionProcessor.StopData> stopData = processor.getStopData(journey);
        //No errors if no hits...
        assertFalse(processor.validDirection(new Subscription(), stopData));

        //Only to in journey
        assertFalse(processor.validDirection(createSubscription("X", "E2", true), stopData));

        //Only from in journey
        assertFalse(processor.validDirection(createSubscription("E2", "X", true), stopData));

        //To and from in correct order in estimated calls
        assertTrue(processor.validDirection(createSubscription("E1", "E2", true), stopData));

        //To and from in opposite order in estimated calls
        assertFalse(processor.validDirection(createSubscription("E2", "E1", true), stopData));

        //correct order: to in estimated calls, from in recorded calls
        assertTrue(processor.validDirection(createSubscription("R1", "E2", true), stopData));

        //opposite order: to in estimated calls, from in recorded calls
        assertFalse(processor.validDirection(createSubscription("E1", "R1", true), stopData));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void processEstimatedVehicleJourney() throws JAXBException, DatatypeConfigurationException {
        DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
        final Duration maxDelay_sR1E1 = datatypeFactory.newDuration("PT30M");
        final Duration maxDelay_sR1E1C = datatypeFactory.newDuration("PT4M");

        EstimatedVehicleJourney.RecordedCalls recordedCalls = new EstimatedVehicleJourney.RecordedCalls();
        addRecordedCall(recordedCalls, "R1", ZonedDateTime.now().minus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addDelayedEstimatedCall(estimatedCalls, "E1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        addDelayedEstimatedCall(estimatedCalls, "E2", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setRecordedCalls(recordedCalls);
        journey.setEstimatedCalls(estimatedCalls);
        journey.setDataSource("BNR");
        LineRef lineRef = new LineRef();
        lineRef.setValue("NSB:Line:1");
        journey.setLineRef(lineRef);
        OperatorRefStructure operatorRef = new OperatorRefStructure();
        operatorRef.setValue("NSB");
        journey.setOperatorRef(operatorRef);
        journey.setDatedVehicleJourneyRef(new DatedVehicleJourneyRef());

        Set<Subscription> subscriptionsForStopPoint = new HashSet<>();
        //Expects these to be found:
        Subscription sR1E1 =
                createSubscription("s_R1_E1", subscriptionsForStopPoint, "R1", "E1", null, null, false, maxDelay_sR1E1);
        Subscription sR1E1C = createSubscription("s_R1_E1_c", subscriptionsForStopPoint,
                "R1", "E1", "BNR", null, false, maxDelay_sR1E1C);
        Subscription sR1E1CL = createSubscription("s_R1_E1_c_l", subscriptionsForStopPoint,
                "R1", "E1", "BNR", "NSB:Line:1", false);
        Subscription sR1E1L = createSubscription("s_R1_E1_l", subscriptionsForStopPoint, "R1", "E1",
                null, "NSB:Line:1", false);
        Subscription sLC = createSubscription("s_l_c", subscriptionsForStopPoint, null, null, "BNR",
                "NSB:Line:1", false);
        Subscription sL = createSubscription("s_l", subscriptionsForStopPoint, null, null,
                null, "NSB:Line:1", false);
        Subscription sC = createSubscription("s_c", subscriptionsForStopPoint, null, null,
                "BNR", null, false);
        //These should not be found:
        createSubscription("notfound1", subscriptionsForStopPoint, "E1", "R1", null, null, false);
        createSubscription("notfound2", subscriptionsForStopPoint, "R1", "E1", "XXX",
                "NSB:Line:2", false);
        Subscription sLCx = createSubscription("s_l_cx", subscriptionsForStopPoint, null,
                null, "XXX", "NSB:Line:1", false);
        Subscription sLxC = createSubscription("s_lx_c", subscriptionsForStopPoint, null, null,
                "BNR", "NSB:Line:2", false);
        createSubscription("notfound3", subscriptionsForStopPoint, "x1", "E1",
                "BNR", "NSB:Line:1", false);
        createSubscription("notfound4", subscriptionsForStopPoint, "R1", "x1",
                "BNR", "NSB:Line:1", false);

        SubscriptionManager subscriptionManagerMock =
                mock(SubscriptionManager.class); //must be somewhat carefull so we don't spend to much time testing the mock...
        when(subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:Quay:E1", ET)).thenReturn(subscriptionsForStopPoint);
        when(subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:Quay:R1", ET)).thenReturn(subscriptionsForStopPoint);
        when(subscriptionManagerMock.getSubscriptionsForLineRef("NSB:Line:1", ET)).thenReturn(new HashSet<>(Arrays.asList(sL, sLC, sLCx)));
        when(subscriptionManagerMock.getSubscriptionsForCodespace("BNR", ET)).thenReturn(new HashSet<>(Arrays.asList(sLC, sC, sLxC)));

        QuayAndStopPlaceMappingService mappingMock = mock(QuayAndStopPlaceMappingService.class);
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:E1")).thenReturn("NSR:StopPlace:E1");
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:R1")).thenReturn("NSR:StopPlace:R1");

        ETSubscriptionProcessor processor = new ETSubscriptionProcessor(subscriptionManagerMock,
                new SiriMarshaller(), mock(FileStorageService.class),
                new MetricsService(), mappingMock);

        ArgumentCaptor<HashSet> subscriptionsOnStopsCaptor = ArgumentCaptor.forClass(HashSet.class);
        ArgumentCaptor<HashSet> subscriptionsOnLineOrVehicleJourneyCaptor = ArgumentCaptor.forClass(HashSet.class);
        assertTrue(processor.processEstimatedVehicleJourney(journey, ZonedDateTime.now()));
        verify(subscriptionManagerMock).notifySubscriptionsOnStops(subscriptionsOnStopsCaptor.capture(), eq(journey), any());
        verify(subscriptionManagerMock).notifySubscriptionsWithFullMessage(subscriptionsOnLineOrVehicleJourneyCaptor.capture(), eq(journey), any());
        HashSet<Subscription> notifiedSubscriptionsOnStops = subscriptionsOnStopsCaptor.getValue();
        assertEquals(3, notifiedSubscriptionsOnStops.size());
        assertFalse(notifiedSubscriptionsOnStops.contains(sR1E1));              //Since maxDelay is 30 minutes
        assertTrue(notifiedSubscriptionsOnStops.contains(sR1E1C));             //Since maxDelay is 4 minutes
        assertTrue(notifiedSubscriptionsOnStops.contains(sR1E1CL));
        assertTrue(notifiedSubscriptionsOnStops.contains(sR1E1L));
        HashSet<Subscription> notifiedSubscriptionsWithFullMessage = subscriptionsOnLineOrVehicleJourneyCaptor.getValue();
        assertEquals(3, notifiedSubscriptionsWithFullMessage.size());
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(sLC));
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(sL));
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(sC));
    }

    @Test
    public void findAffectedStopPlaceOnlySubscriptionOnETMessageWithQuays() throws Exception {
        Subscription s1 = createSubscription("s1", "1", "2", false);
        Subscription q1 = createSubscription("q1", "1", "2", true);
        Subscription s2 = createSubscription("s2", "2", "1", false);

        MetricsService metricsService = new MetricsService();
        SiriMarshaller siriMarshaller = new SiriMarshaller();
        DataStorageService dataStorageMock = mock(DataStorageService.class);
        HashSet<Subscription> subscriptionsNotified = new HashSet<>();
        QuayAndStopPlaceMappingService mappingMock = mock(QuayAndStopPlaceMappingService.class);
        SubscriptionManager subscriptionManager =
                new SubscriptionManager(dataStorageMock, siriMarshaller, metricsService, new HashMap<>(), new HashMap<>(), mappingMock) {
                    @Override
                    public void notifySubscriptionsOnStops(HashSet<Subscription> subscriptions,
                                                           EstimatedVehicleJourney estimatedVehicleJourney, ZonedDateTime timestamp) {
                        subscriptionsNotified.addAll(subscriptions);
                    }
                };

        ETSubscriptionProcessor processor = new ETSubscriptionProcessor(subscriptionManager, siriMarshaller,
                mock(FileStorageService.class), metricsService, mappingMock);

        HashSet<Subscription> subscriptions = Sets.newHashSet(s1, s2);
        when(dataStorageMock.getSubscriptionsForStopPoint("NSR:StopPlace:1", ET)).thenReturn(subscriptions);
        when(dataStorageMock.getSubscriptionsForStopPoint("NSR:StopPlace:2", ET)).thenReturn(subscriptions);
        when(dataStorageMock.getSubscriptionsForStopPoint("NSR:Quay:1", ET)).thenReturn(Sets.newHashSet(q1));
        when(dataStorageMock.getSubscriptionsForStopPoint("NSR:Quay:2", ET)).thenReturn(Sets.newHashSet(q1));
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:1")).thenReturn("NSR:StopPlace:1");
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:2")).thenReturn("NSR:StopPlace:2");

        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addDelayedEstimatedCall(estimatedCalls, "1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        addDelayedEstimatedCall(estimatedCalls, "2", ZonedDateTime.now().plus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setEstimatedCalls(estimatedCalls);
        VehicleRef vehicleRef = new VehicleRef();
        vehicleRef.setValue("1111");
        journey.setVehicleRef(vehicleRef);
        LineRef lineRef = new LineRef();
        lineRef.setValue("NSB:Line:1111");
        journey.setLineRef(lineRef);
        OperatorRefStructure operatorRef = new OperatorRefStructure();
        operatorRef.setValue("NSB");
        journey.setOperatorRef(operatorRef);
        journey.setDatedVehicleJourneyRef(new DatedVehicleJourneyRef());

        boolean processed = processor.processEstimatedVehicleJourney(journey, ZonedDateTime.now());
        assertTrue(processed);
        assertEquals(2, subscriptionsNotified.size());
        assertThat(subscriptionsNotified, hasItem(s1));
        assertThat(subscriptionsNotified, hasItem(q1));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void isPushAllDataTests() throws JAXBException {
        EstimatedVehicleJourney.RecordedCalls recordedCalls = new EstimatedVehicleJourney.RecordedCalls();
        addRecordedCall(recordedCalls, "R1", ZonedDateTime.now().minus(1, ChronoUnit.HOURS));
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addEstimatedCall(estimatedCalls, "E1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        addEstimatedCall(estimatedCalls, "E2", ZonedDateTime.now().plus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setRecordedCalls(recordedCalls);
        journey.setEstimatedCalls(estimatedCalls);
        journey.setDataSource("BNR");
        LineRef lineRef = new LineRef();
        lineRef.setValue("NSB:Line:1");
        journey.setLineRef(lineRef);
        OperatorRefStructure operatorRef = new OperatorRefStructure();
        operatorRef.setValue("NSB");
        journey.setOperatorRef(operatorRef);
        journey.setDatedVehicleJourneyRef(new DatedVehicleJourneyRef());

        Set<Subscription> subscriptionsForStopPoint = new HashSet<>();
        //These should not be found:
        createSubscription("s_R1_E1", subscriptionsForStopPoint, "R1", "E1", null, null, false);
        createSubscription("s_R1_E1_c", subscriptionsForStopPoint, "R1", "E1", "BNR", null, false);
        createSubscription("s_R1_E1_c_l", subscriptionsForStopPoint, "R1", "E1", "BNR", "NSB:Line:1", false);
        createSubscription("s_R1_E1_l", subscriptionsForStopPoint, "R1", "E1", null, "NSB:Line:1", false);
        Subscription sLC = createSubscription("s_l_c", subscriptionsForStopPoint, null, null, "BNR", "NSB:Line:1", false);
        Subscription sL = createSubscription("s_l", subscriptionsForStopPoint, null, null, null, "NSB:Line:1", false);
        Subscription sC = createSubscription("s_c", subscriptionsForStopPoint, null, null, "BNR", null, false);
        Subscription sLCx = createSubscription("s_l_cx", subscriptionsForStopPoint, null, null, "XXX", "NSB:Line:1", false);
        Subscription sLxC = createSubscription("s_lx_c", subscriptionsForStopPoint, null, null, "BNR", "NSB:Line:2", false);
        //Expects these to be found:
        Subscription allMessagesR1E1 = createSubscription("allMessages_R1_E1", subscriptionsForStopPoint, "R1", "E1", null, null, false);
        Subscription allMessagesR1E1C = createSubscription("allMessages_R1_E1_c", subscriptionsForStopPoint, "R1", "E1", "BNR", null, false);
        Subscription allMessagesR1E1CL =
                createSubscription("allMessages_R1_E1_c_l", subscriptionsForStopPoint, "R1", "E1", "BNR", "NSB:Line:1", false);
        Subscription allMessagesR1E1L = createSubscription("allMessages_R1_E1_l", subscriptionsForStopPoint, "R1", "E1", null, "NSB:Line:1", false);
        Subscription allMessagesLC = createSubscription("allMessages_l_c", subscriptionsForStopPoint, null, null, "BNR", "NSB:Line:1", false);
        Subscription allMessagesL = createSubscription("allMessages_l", subscriptionsForStopPoint, null, null, null, "NSB:Line:1", false);
        Subscription allMessagesC = createSubscription("allMessages_c", subscriptionsForStopPoint, null, null, "BNR", null, false);
        allMessagesR1E1.setPushAllData(true);
        allMessagesR1E1C.setPushAllData(true);
        allMessagesR1E1CL.setPushAllData(true);
        allMessagesR1E1L.setPushAllData(true);
        allMessagesLC.setPushAllData(true);
        allMessagesL.setPushAllData(true);
        allMessagesC.setPushAllData(true);

        SubscriptionManager subscriptionManagerMock =
                mock(SubscriptionManager.class); //must be somewhat carefull so we don't spend to much time testing the mock...
        when(subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:Quay:E1", ET)).thenReturn(subscriptionsForStopPoint);
        when(subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:Quay:R1", ET)).thenReturn(subscriptionsForStopPoint);
        when(subscriptionManagerMock.getSubscriptionsForLineRef("NSB:Line:1", ET))
                .thenReturn(new HashSet<>(Arrays.asList(sL, sLC, sLCx, allMessagesL, allMessagesLC)));
        when(subscriptionManagerMock.getSubscriptionsForCodespace("BNR", ET))
                .thenReturn(new HashSet<>(Arrays.asList(sLC, sC, sLxC, allMessagesLC, allMessagesC)));

        QuayAndStopPlaceMappingService mappingMock = mock(QuayAndStopPlaceMappingService.class);
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:E1")).thenReturn("NSR:StopPlace:E1");
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:R1")).thenReturn("NSR:StopPlace:R1");

        ETSubscriptionProcessor processor = new ETSubscriptionProcessor(subscriptionManagerMock,
                new SiriMarshaller(), mock(FileStorageService.class),
                new MetricsService(), mappingMock);

        ArgumentCaptor<HashSet> subscriptionsOnStopsCaptor = ArgumentCaptor.forClass(HashSet.class);
        ArgumentCaptor<HashSet> subscriptionsOnLineOrVehicleJourneyCaptor = ArgumentCaptor.forClass(HashSet.class);
        assertTrue(processor.processEstimatedVehicleJourney(journey, ZonedDateTime.now()));
        verify(subscriptionManagerMock).notifySubscriptionsOnStops(subscriptionsOnStopsCaptor.capture(), eq(journey), any());
        verify(subscriptionManagerMock).notifySubscriptionsWithFullMessage(subscriptionsOnLineOrVehicleJourneyCaptor.capture(), eq(journey), any());
        HashSet<Subscription> notifiedSubscriptionsOnStops = subscriptionsOnStopsCaptor.getValue();
        assertEquals(4, notifiedSubscriptionsOnStops.size());
        assertTrue(notifiedSubscriptionsOnStops.contains(allMessagesR1E1));
        assertTrue(notifiedSubscriptionsOnStops.contains(allMessagesR1E1C));
        assertTrue(notifiedSubscriptionsOnStops.contains(allMessagesR1E1CL));
        assertTrue(notifiedSubscriptionsOnStops.contains(allMessagesR1E1L));
        HashSet<Subscription> notifiedSubscriptionsWithFullMessage = subscriptionsOnLineOrVehicleJourneyCaptor.getValue();
        assertEquals(3, notifiedSubscriptionsWithFullMessage.size());
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(allMessagesLC));
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(allMessagesL));
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(allMessagesC));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void trackChange() throws JAXBException {

        EstimatedVehicleJourney.RecordedCalls recordedCalls = new EstimatedVehicleJourney.RecordedCalls();
        addRecordedCall(recordedCalls, "R1", ZonedDateTime.now().minus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addEstimatedCall(estimatedCalls, "E1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS), true);
        addEstimatedCall(estimatedCalls, "E2", ZonedDateTime.now().plus(2, ChronoUnit.HOURS), true);
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setRecordedCalls(recordedCalls);
        journey.setEstimatedCalls(estimatedCalls);
        journey.setDataSource("BNR");
        LineRef lineRef = new LineRef();
        lineRef.setValue("NSB:Line:1");
        journey.setLineRef(lineRef);
        OperatorRefStructure operatorRef = new OperatorRefStructure();
        operatorRef.setValue("NSB");
        journey.setOperatorRef(operatorRef);
        journey.setDatedVehicleJourneyRef(new DatedVehicleJourneyRef());

        Set<Subscription> subscriptionsForStopPoint = new HashSet<>();
        //Expects these to be found:
        Subscription sR1E1 = createSubscription("s_R1_E1", subscriptionsForStopPoint, "R1", "E1", null, null, false);
        Subscription sR1E1C = createSubscription("s_R1_E1_c", subscriptionsForStopPoint, "R1", "E1", "BNR", null, false, null, DeviationType.CANCELED);
        Subscription sR1E1CL = createSubscription("s_R1_E1_c_l", subscriptionsForStopPoint, "R1", "E1", "BNR", "NSB:Line:1", false,null, DeviationType.TRACK_CHANGE);
        Subscription sR1E1L = createSubscription("s_R1_E1_l", subscriptionsForStopPoint, "R1", "E1", null, "NSB:Line:1", false, null, DeviationType.DELAYED);
        Subscription sLC = createSubscription("s_l_c", subscriptionsForStopPoint, null, null, "BNR", "NSB:Line:1", false);
        Subscription sL = createSubscription("s_l", subscriptionsForStopPoint, null, null, null, "NSB:Line:1", false);
        Subscription sC = createSubscription("s_c", subscriptionsForStopPoint, null, null, "BNR", null, false);
        //These should not be found:
        createSubscription("notfound1", subscriptionsForStopPoint, "E1", "R1", null, null, false);
        createSubscription("notfound2", subscriptionsForStopPoint, "R1", "E1", "XXX", "NSB:Line:2", false);
        Subscription sLCx = createSubscription("s_l_cx", subscriptionsForStopPoint, null, null, "XXX", "NSB:Line:1", false);
        Subscription sLxC = createSubscription("s_lx_c", subscriptionsForStopPoint, null, null, "BNR", "NSB:Line:2", false);
        createSubscription("notfound3", subscriptionsForStopPoint, "x1", "E1", "BNR", "NSB:Line:1", false);
        createSubscription("notfound4", subscriptionsForStopPoint, "R1", "x1", "BNR", "NSB:Line:1", false);

        SubscriptionManager subscriptionManagerMock =
                mock(SubscriptionManager.class); //must be somewhat careful so we don't spend to much time testing the mock...
        when(subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:Quay:E1", ET)).thenReturn(subscriptionsForStopPoint);
        when(subscriptionManagerMock.getSubscriptionsForStopPoint("NSR:Quay:R1", ET)).thenReturn(subscriptionsForStopPoint);
        when(subscriptionManagerMock.getSubscriptionsForLineRef("NSB:Line:1", ET)).thenReturn(new HashSet<>(Arrays.asList(sL, sLC, sLCx)));
        when(subscriptionManagerMock.getSubscriptionsForCodespace("BNR", ET)).thenReturn(new HashSet<>(Arrays.asList(sLC, sC, sLxC)));

        QuayAndStopPlaceMappingService mappingMock = mock(QuayAndStopPlaceMappingService.class);
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:E1")).thenReturn("NSR:StopPlace:E1");
        when(mappingMock.mapQuayToStopPlace("NSR:Quay:R1")).thenReturn("NSR:StopPlace:R1");

        ETSubscriptionProcessor processor = new ETSubscriptionProcessor(subscriptionManagerMock,
                new SiriMarshaller(), mock(FileStorageService.class),
                new MetricsService(), mappingMock);

        ArgumentCaptor<HashSet> subscriptionsOnStopsCaptor = ArgumentCaptor.forClass(HashSet.class);
        ArgumentCaptor<HashSet> subscriptionsOnLineOrVehicleJourneyCaptor = ArgumentCaptor.forClass(HashSet.class);
        assertTrue(processor.processEstimatedVehicleJourney(journey, ZonedDateTime.now()));
        verify(subscriptionManagerMock).notifySubscriptionsOnStops(subscriptionsOnStopsCaptor.capture(), eq(journey), any());
        verify(subscriptionManagerMock).notifySubscriptionsWithFullMessage(subscriptionsOnLineOrVehicleJourneyCaptor.capture(), eq(journey), any());
        HashSet<Subscription> notifiedSubscriptionsOnStops = subscriptionsOnStopsCaptor.getValue();
        assertEquals(2, notifiedSubscriptionsOnStops.size());
        assertTrue(notifiedSubscriptionsOnStops.contains(sR1E1));
        assertFalse(notifiedSubscriptionsOnStops.contains(sR1E1C)); // Deviation Type is Canceled
        assertTrue(notifiedSubscriptionsOnStops.contains(sR1E1CL));
        assertFalse(notifiedSubscriptionsOnStops.contains(sR1E1L)); // Deviation Type is Delayed
        HashSet<Subscription> notifiedSubscriptionsWithFullMessage = subscriptionsOnLineOrVehicleJourneyCaptor.getValue();
        assertEquals(3, notifiedSubscriptionsWithFullMessage.size());
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(sLC));
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(sL));
        assertTrue(notifiedSubscriptionsWithFullMessage.contains(sC));
    }

    private Subscription createSubscription(String name,
                                            Set<Subscription> subscriptions,
                                            String from,
                                            String to,
                                            String codespace,
                                            String line,
                                            boolean subscribeToQuay,
                                            Duration maxDelay,
                                            DeviationType deviationType) {

        Subscription subscription = new Subscription();
        subscription.setName(name);
        subscription.setId(Integer.toString(subscriptionCounter++));
        String stopPrefix;
        if (subscribeToQuay) {
            stopPrefix = "NSR:Quay:";
        } else {
            stopPrefix = "NSR:StopPlace:";
        }
        if (from != null) {
            subscription.addFromStopPoint(stopPrefix + from);
        }
        if (to != null) {
            subscription.addToStopPoint(stopPrefix + to);
        }
        if (line != null) {
            subscription.addLineRef(line);
        }
        if (codespace != null) {
            subscription.addCodespace(codespace);
        }
        if (subscriptions != null) {
            subscriptions.add(subscription);
        }
        if (maxDelay != null) {
            subscription.setMaxArrivalDelay(maxDelay);
        }
        subscription.setDeviationType(deviationType);
        return subscription;
    }

    private Subscription createSubscription(String name, Set<Subscription> subscriptions,
                                            String from, String to, String codespace, String line,
                                            boolean subscribeToQuay) {
        return createSubscription(name, subscriptions,from,to,codespace,line,subscribeToQuay,null);
    }
    private Subscription createSubscription(String name, Set<Subscription> subscriptions,
                                            String from, String to, String codespace, String line,
                                            boolean subscribeToQuay, Duration maxDelay) {
        return createSubscription(name, subscriptions,from,to,codespace,line,subscribeToQuay,maxDelay,null);
    }

    private Subscription createSubscription(String name, String from, String to, boolean createQuay) {
        return createSubscription(name, null, from, to, null, null, createQuay);
    }

    private Subscription createSubscription(String from, String to, boolean createQuay) {
        return createSubscription(null, null, from, to, null, null, createQuay);
    }

    private void addDelayedEstimatedCall(EstimatedVehicleJourney.EstimatedCalls estimatedCalls, String stopPointRef, ZonedDateTime time) {
        EstimatedCall estimatedCall = new EstimatedCall();
        StopPointRef ref = new StopPointRef();
        String quay = "NSR:Quay:" + stopPointRef;
        ref.setValue(quay);
        estimatedCall.setStopPointRef(ref);
        estimatedCall.setAimedDepartureTime(time);
        estimatedCall.setExpectedDepartureTime(time.plusMinutes(5));
        estimatedCall.setDepartureStatus(CallStatusEnumeration.DELAYED);
        estimatedCall.setAimedArrivalTime(time);
        estimatedCall.setExpectedArrivalTime(time.plusMinutes(5));
        estimatedCall.setArrivalStatus(CallStatusEnumeration.DELAYED);
        estimatedCall.setArrivalStopAssignment(createStopAssignment(false, quay));
        estimatedCalls.getEstimatedCalls().add(estimatedCall);
    }

    private void addEstimatedCall(EstimatedVehicleJourney.EstimatedCalls estimatedCalls, String stopPointRef, ZonedDateTime time) {
        addEstimatedCall(estimatedCalls, stopPointRef, time, false);
    }

    private void addEstimatedCall(EstimatedVehicleJourney.EstimatedCalls estimatedCalls,
                                  String stopPointRef, ZonedDateTime time, boolean trackChange) {
        EstimatedCall estimatedCall = new EstimatedCall();
        StopPointRef ref = new StopPointRef();
        String quay = "NSR:Quay:" + stopPointRef;
        ref.setValue(quay);
        estimatedCall.setStopPointRef(ref);
        estimatedCall.setAimedDepartureTime(time);
        estimatedCall.setExpectedDepartureTime(time);
        estimatedCall.setDepartureStatus(CallStatusEnumeration.ON_TIME);
        estimatedCall.setAimedArrivalTime(time);
        estimatedCall.setExpectedArrivalTime(time);
        estimatedCall.setArrivalStatus(CallStatusEnumeration.ON_TIME);
        estimatedCall.setArrivalStopAssignment(createStopAssignment(trackChange, quay));
        estimatedCalls.getEstimatedCalls().add(estimatedCall);
    }

    private StopAssignmentStructure createStopAssignment(boolean trackChange, String quay) {
        QuayRefStructure expectedQuay = new QuayRefStructure();
        expectedQuay.setValue(quay);
        QuayRefStructure aimedQuay = new QuayRefStructure();
        aimedQuay.setValue(trackChange ? quay + "aimed" : quay);
        StopAssignmentStructure stopAssignment = new StopAssignmentStructure();
        stopAssignment.setExpectedQuayRef(expectedQuay);
        stopAssignment.setAimedQuayRef(aimedQuay);
        return stopAssignment;
    }

    private void addRecordedCall(EstimatedVehicleJourney.RecordedCalls recordedCalls, String stopPointRef, ZonedDateTime departureTime) {
        RecordedCall recordedCall = new RecordedCall();
        StopPointRef ref = new StopPointRef();
        ref.setValue("NSR:Quay:" + stopPointRef);
        recordedCall.setStopPointRef(ref);
        recordedCall.setAimedDepartureTime(departureTime);
        recordedCalls.getRecordedCalls().add(recordedCall);
    }
}