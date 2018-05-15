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

import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.routedata.LiveRouteManager;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.testsupport.DatastoreTest;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.siri.siri20.*;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SXSubscriptionProcessorTest extends DatastoreTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private SubscriptionManager subscriptionManager;
    private SXSubscriptionProcessor processor;
    private LiveRouteManager liveRouteManagerMock;
    private SiriMarshaller siriMarshaller;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        MetricsService metricsServiceMock = mock(MetricsService.class);
        siriMarshaller = new SiriMarshaller();
        DataStorageService dataStorageService = new DataStorageService(datastore, liveJourneyIMap);
        subscriptionManager = new SubscriptionManager(dataStorageService, siriMarshaller, metricsServiceMock, new HashMap<>(), new QuayAndStopPlaceMappingService(metricsServiceMock));
        liveRouteManagerMock = mock(LiveRouteManager.class);
        processor = new SXSubscriptionProcessor(subscriptionManager, siriMarshaller, liveRouteManagerMock, mock(FileStorageService.class), mock(MetricsService.class));
    }

    @Test
    public void findAffectedSubscriptionOnStopsOnly()  {
        Subscription s1 = createSubscription("s2", "NSR:StopPlace:2", "NSR:StopPlace:3");
        s1.addFromStopPoint("NSR:StopPlace:22");
        s1.addFromStopPoint("NSR:StopPlace:222");
        s1.addToStopPoint("NSR:StopPlace:33");
        s1.addToStopPoint("NSR:StopPlace:333");

        createSubscription("s2", "NSR:StopPlace:3", "NSR:StopPlace:5");
        Subscription s0 = createSubscription("s0", "NSR:StopPlace:0", "NSR:StopPlace:2");

        //Only one in correct order
        HashSet<Subscription> affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(asList("1", "2", "3", "4"), null, false));
        assertEquals(1, affectedSubscriptions.size());
        assertTrue(affectedSubscriptions.contains(s1));

        //None in the opposite order
        affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(asList("4", "3", "2", "1"), null, false));
        assertTrue(affectedSubscriptions.isEmpty());

        //All when we don't know if all stops is present in route
        assertPresent(asList(s1, s0), processor.findAffectedSubscriptions(createVehicleJourneys(Collections.singletonList("2"), null, true)));

        //Only one when we look up the camelroute if not all stops is present in route
        when(liveRouteManagerMock.getJourneys()).thenReturn(Collections.singletonList(createLiveJourney("line#1", "123", asList("1", "2", "3"))));
        assertPresent(Collections.singletonList(s1), processor.findAffectedSubscriptions(createVehicleJourneys(Collections.singletonList("2"), "123", true)));
    }

    @Test
    public void one_affected_unsubscribed_stop_on_journey() throws Exception {
        createSubscription("test", "NSR:StopPlace:2", "NSR:StopPlace:3");

        String SX_with_one_affected_stop_on_journey =
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<PtSituationElement xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\">\n" +
                "  <CreationTime>2018-01-26T11:36:29+01:00</CreationTime>\n" +
                "  <ParticipantRef>NSB</ParticipantRef>\n" +
                "  <SituationNumber>status-168101694</SituationNumber>\n" +
                "  <Version>1</Version>\n" +
                "  <Source>\n" +
                "    <SourceType>web</SourceType>\n" +
                "  </Source>\n" +
                "  <Progress>published</Progress>\n" +
                "  <ValidityPeriod>\n" +
                "    <StartTime>2018-04-11T00:00:00+02:00</StartTime>\n" +
                "    <EndTime>2018-04-12T04:35:00+02:00</EndTime>\n" +
                "  </ValidityPeriod>\n" +
                "  <UndefinedReason/>\n" +
                "  <ReportType>incident</ReportType>\n" +
                "  <Keywords/>\n" +
                "  <Description xml:lang=\"NO\">Toget vil bytte togmateriell på Voss. Du må dessverre bytte tog på denne stasjonen. På strekningen Bergen-Voss gjelder ikke plassreservasjoner.</Description>\n" +
                "  <Description xml:lang=\"EN\">You must change trains at Voss. We apologize for the inconvenience. No seat reservations Bergen-Voss.</Description>\n" +
                "  <Affects>\n" +
                "    <VehicleJourneys>\n" +
                "      <AffectedVehicleJourney>\n" +
                "        <VehicleJourneyRef>64</VehicleJourneyRef>\n" +
                "        <Route>\n" +
                "          <StopPoints>\n" +
                "            <AffectedOnly>true</AffectedOnly>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:440</StopPointRef>\n" +
                "              <StopPointName>Voss</StopPointName>\n" +
                "              <Extensions>\n" +
                "                <BoardingRelevance xmlns:ifopt=\"http://www.ifopt.org.uk/ifopt\" xmlns:datex2=\"http://datex2.eu/schema/2_0RC1/2_0\" xmlns:acsb=\"http://www.ifopt.org.uk/acsb\" arrive=\"true\" depart=\"true\" pass=\"true\" transfer=\"true\"/>\n" +
                "              </Extensions>\n" +
                "            </AffectedStopPoint>\n" +
                "          </StopPoints>\n" +
                "        </Route>\n" +
                "        <OriginAimedDepartureTime>2018-04-11T00:00:00+02:00</OriginAimedDepartureTime>\n" +
                "      </AffectedVehicleJourney>\n" +
                "    </VehicleJourneys>\n" +
                "  </Affects>\n" +
                "</PtSituationElement>\n";
        PtSituationElement ptSituationElement = siriMarshaller.unmarshall(SX_with_one_affected_stop_on_journey, PtSituationElement.class);

        when(liveRouteManagerMock.getJourneys()).thenReturn(Collections.singletonList(createLiveJourney("line#1", "64", asList("1", "2", "440"))));

        assertNotNull(ptSituationElement);
        assertNotNull(ptSituationElement.getAffects());

        HashSet<String> affectedStopPointRefs = processor.findAffectedStopPlaceRefs(ptSituationElement.getAffects().getStopPlaces());
        assertEquals(0, affectedStopPointRefs.size());
        HashSet<Subscription> affectedSubscriptions = processor.findAffectedSubscriptions(ptSituationElement.getAffects().getVehicleJourneys().getAffectedVehicleJourneies());
        assertEquals(0, affectedSubscriptions.size());
        //modify affected journey so we have no route data
        List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = ptSituationElement.getAffects().getVehicleJourneys().getAffectedVehicleJourneies();
        List<VehicleJourneyRef> vehicleJourneyReves = affectedVehicleJourneies.get(0).getVehicleJourneyReves();
        vehicleJourneyReves.clear();
        VehicleJourneyRef journeyRef = new VehicleJourneyRef();
        journeyRef.setValue("NoHit");
        vehicleJourneyReves.add(journeyRef);
        affectedSubscriptions = processor.findAffectedSubscriptions(ptSituationElement.getAffects().getVehicleJourneys().getAffectedVehicleJourneies());
        assertEquals(0, affectedSubscriptions.size());
    }

    @Test
    public void testSubscriptionsWithLineAndStops() {
        //These are supposed to be found
        Subscription s_stops_line = createSubscription("from-to-line", "NSR:StopPlace:10", "NSR:StopPlace:20", "line#1");
        Subscription s_line = createSubscription("line", null, null, "line#1");
        List<Subscription> expectedSubscriptions = asList(s_stops_line, s_line);
        //These are not supposed to be found as they have one or more conditions set that doesn't match:
        createSubscription("NOHIT3-from-to-line", "NSR:StopPlace:40", "NSR:StopPlace:30", "line#1");
        createSubscription("NOHIT4-from-to-line", "NSR:StopPlace:10", "NSR:StopPlace:20", "line#2");
        createSubscription("NOHIT8-line", null, null, "line#2");

        ArrayList<LiveJourney> testRouteData = new ArrayList<>();
        testRouteData.add(createLiveJourney("line#1", "vehicle#1", asList("10", "20", "30", "40")));
        testRouteData.add(createLiveJourney("line#x", "vehicle#x", asList("10", "20", "30", "40")));
        when(liveRouteManagerMock.getJourneys()).thenReturn(testRouteData);

        AffectsScopeStructure.VehicleJourneys vehiclejourney = new AffectsScopeStructure.VehicleJourneys();
        AffectedVehicleJourneyStructure affectedVehicleJourneyStructure = new AffectedVehicleJourneyStructure();
        vehiclejourney.getAffectedVehicleJourneies().add(affectedVehicleJourneyStructure);
        VehicleJourneyRef vehicleJourneyRef = new VehicleJourneyRef();
        vehicleJourneyRef.setValue("vehicle#1");
        affectedVehicleJourneyStructure.getVehicleJourneyReves().add(vehicleJourneyRef);
        AffectedRouteStructure routeStructure = new AffectedRouteStructure();
        affectedVehicleJourneyStructure.getRoutes().add(routeStructure);
        AffectedRouteStructure.StopPoints stopPoints = new AffectedRouteStructure.StopPoints();
        routeStructure.setStopPoints(stopPoints);
        stopPoints.setAffectedOnly(true);
        stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSR:StopPlace:10"));
        stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSR:StopPlace:20"));
        assertPresent(expectedSubscriptions, processor.findAffectedSubscriptions(vehiclejourney.getAffectedVehicleJourneies()));

        stopPoints.setAffectedOnly(false); //<-- Set to false, since it effects how the live route is used
        stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSR:StopPlace:30"));
        stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSR:StopPlace:40"));
        assertPresent(expectedSubscriptions, processor.findAffectedSubscriptions(vehiclejourney.getAffectedVehicleJourneies()));

        AffectsScopeStructure.VehicleJourneys unsubscribedVehiclejourney = new AffectsScopeStructure.VehicleJourneys();
        AffectedVehicleJourneyStructure unsubscribedAffectedVehicleJourneyStructure = new AffectedVehicleJourneyStructure();
        unsubscribedVehiclejourney.getAffectedVehicleJourneies().add(unsubscribedAffectedVehicleJourneyStructure);
        VehicleJourneyRef unsubscribedVehicleJourneyRef = new VehicleJourneyRef();
        unsubscribedVehicleJourneyRef.setValue("unsubscribed");
        unsubscribedAffectedVehicleJourneyStructure.getVehicleJourneyReves().add(unsubscribedVehicleJourneyRef);
        AffectedRouteStructure unsubscribedRouteStructure = new AffectedRouteStructure();
        unsubscribedAffectedVehicleJourneyStructure.getRoutes().add(unsubscribedRouteStructure);
        AffectedRouteStructure.StopPoints unsubscribedStopPoints = new AffectedRouteStructure.StopPoints();
        unsubscribedRouteStructure.setStopPoints(unsubscribedStopPoints);
        unsubscribedStopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSB:StopPlace:111"));
        unsubscribedStopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSB:StopPlace:112"));
        unsubscribedStopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().add(createAffectedStopPointStructure("NSB:StopPlace:113"));
        unsubscribedStopPoints.setAffectedOnly(true);
        assertPresent(Collections.emptyList(), processor.findAffectedSubscriptions(unsubscribedVehiclejourney.getAffectedVehicleJourneies()));
    }

    private void assertPresent(List<Subscription> expectedSubscriptions, Collection<Subscription> actualSubscriptions) {
        logger.debug("Found these subscriptions: {} ", actualSubscriptions.stream().map(Subscription::getName).collect(Collectors.toList()));
        ArrayList<Subscription> missing = new ArrayList<>();
        for (Subscription expectedSubscription : expectedSubscriptions) {
            if (!actualSubscriptions.contains(expectedSubscription)) {
                missing.add(expectedSubscription);
            }
        }
        ArrayList<Subscription> toMany = new ArrayList<>();
        for (Subscription actualSubscription : actualSubscriptions) {
            if (!expectedSubscriptions.contains(actualSubscription)) {
                toMany.add(actualSubscription);
            }
        }
        String errorMessage = "";
        if (expectedSubscriptions.size() != actualSubscriptions.size()) {
            errorMessage = "Expected "+expectedSubscriptions.size()+" subscriptions, but found "+actualSubscriptions.size()+". ";
        }
        if (!missing.isEmpty()) {
            errorMessage += "Missing subscriptions: "+Arrays.toString(missing.stream().map(Subscription::getName).toArray())+". ";
        }
        if (!toMany.isEmpty()) {
            errorMessage += "Unexpected subscriptions: "+Arrays.toString(toMany.stream().map(Subscription::getName).toArray())+". ";
        }
        if (StringUtils.isNotBlank(errorMessage)) {
            fail(errorMessage);
        }
    }

    private AffectedStopPointStructure createAffectedStopPointStructure(String s2) {
        AffectedStopPointStructure stop1 = new AffectedStopPointStructure();
        stop1.setStopPointRef(createStopPointRef(s2));
        return stop1;
    }

    private EstimatedCall createEstimatedCall(String s2) {
        EstimatedCall estimatedCall1 = new EstimatedCall();
        estimatedCall1.setStopPointRef(createStopPointRef(s2));
        return estimatedCall1;
    }

    private StopPointRef createStopPointRef(String value) {
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(value);
        return stopPointRef;
    }

    private Subscription createSubscription(String name, String fromStop, String toStop, String line) {
        Subscription s = new Subscription();
        s.setPushAddress("push");
        if (name != null) s.setName(name);
        if (fromStop != null) s.addFromStopPoint(fromStop);
        if (toStop != null) s.addToStopPoint(toStop);
        if (line != null) s.addLineRef(line);
        return subscriptionManager.addOrUpdate(s);
    }

    private Subscription createSubscription(String name, String fromStop, String toStop) {
        return createSubscription(name, fromStop, toStop, null);
    }

    private LiveJourney createLiveJourney(String line, String vehicleRef, List<String> stops) {
        EstimatedVehicleJourney someJourney = new EstimatedVehicleJourney();
        if (vehicleRef != null) {
            VehicleRef value = new VehicleRef();
            value.setValue(vehicleRef);
            someJourney.setVehicleRef(value);
        }
        if (line != null) {
            LineRef lineRef = new LineRef();
            lineRef.setValue(line);
            someJourney.setLineRef(lineRef);
        }
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        someJourney.setEstimatedCalls(estimatedCalls);
        List<EstimatedCall> calls = estimatedCalls.getEstimatedCalls();
        ZonedDateTime now = ZonedDateTime.now();
        for (String stop : stops) {
            now = now.plusMinutes(10);
            EstimatedCall call = createEstimatedCall("NSR:StopPlace:" + stop);
            call.getStopPointNames();
            call.setAimedArrivalTime(now);
            call.setExpectedArrivalTime(now);
            call.setArrivalStatus(CallStatusEnumeration.ON_TIME);
            call.setAimedDepartureTime(now);
            call.setExpectedDepartureTime(now);
            call.setDepartureStatus(CallStatusEnumeration.ON_TIME);
            calls.add(call);
        }
        return new LiveJourney(someJourney, mock(QuayAndStopPlaceMappingService.class));
    }

    private List<AffectedVehicleJourneyStructure> createVehicleJourneys(List<String> stops, String vehicleJourneyRef, boolean affectedOnly) {
        AffectedVehicleJourneyStructure affectedVehicleJourneyStructure = new AffectedVehicleJourneyStructure();
        if (vehicleJourneyRef != null) {
            List<VehicleJourneyRef> vehicleJourneyReves = affectedVehicleJourneyStructure.getVehicleJourneyReves();
            VehicleJourneyRef ref = new VehicleJourneyRef();
            ref.setValue(vehicleJourneyRef);
            vehicleJourneyReves.add(ref);
        }

        AffectedRouteStructure routeStructure = new AffectedRouteStructure();
        AffectedRouteStructure.StopPoints stopPoints = new AffectedRouteStructure.StopPoints();
        routeStructure.setStopPoints(stopPoints);
        if (affectedOnly) {
            stopPoints.setAffectedOnly(true);
        }
        List<Serializable> affectedStopPointsAndLinkProjectionToNextStopPoints = stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints();
        for (String stop : stops) {
            AffectedStopPointStructure affectedStopPointStructure = createAffectedStopPointStructure("NSR:StopPlace:" + stop);
            affectedStopPointsAndLinkProjectionToNextStopPoints.add(affectedStopPointStructure);
        }

        affectedVehicleJourneyStructure.getRoutes().add(routeStructure);
        return Collections.singletonList(affectedVehicleJourneyStructure);
    }

}