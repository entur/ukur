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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.apache.commons.lang3.StringUtils;
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
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SXSubscriptionProcessorTest extends DatastoreTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private SubscriptionManager subscriptionManager;
    private SXSubscriptionProcessor processor;
    private SiriMarshaller siriMarshaller;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        HazelcastInstance hazelcastInstance = new TestHazelcastInstanceFactory().newHazelcastInstance();
        ITopic<String> subscriptionTopic = hazelcastInstance.getTopic("subscriptions");
        MetricsService metricsServiceMock = mock(MetricsService.class);
        siriMarshaller = new SiriMarshaller();
        DataStorageService dataStorageService = new DataStorageService(datastore, subscriptionTopic);
        HashMap<String, Collection<String>> stopPlacesAndQuays = new HashMap<>();
        stopPlacesAndQuays.put("NSR:StopPlace:0", Sets.newHashSet("NSR:Quay:232"));
        stopPlacesAndQuays.put("NSR:StopPlace:1", Sets.newHashSet("NSR:Quay:232"));
        stopPlacesAndQuays.put("NSR:StopPlace:2", Sets.newHashSet("NSR:Quay:232"));
        stopPlacesAndQuays.put("NSR:StopPlace:3", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:22", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:222", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:3", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:33", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:333", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:5", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:10", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:20", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:30", Sets.newHashSet("NSR:Quay:125"));
        stopPlacesAndQuays.put("NSR:StopPlace:40", Sets.newHashSet("NSR:Quay:125"));
        QuayAndStopPlaceMappingService quayAndStopPlaceMappingService = new QuayAndStopPlaceMappingService(mock(MetricsService.class));
        quayAndStopPlaceMappingService.updateStopsAndQuaysMap(stopPlacesAndQuays);

        subscriptionManager =
                new SubscriptionManager(dataStorageService, siriMarshaller, metricsServiceMock, new HashMap<>(), new HashMap<>(), quayAndStopPlaceMappingService);
        processor = new SXSubscriptionProcessor(subscriptionManager, siriMarshaller, mock(FileStorageService.class), mock(MetricsService.class));
    }

    @Test
    public void findAffectedSubscriptionOnStopsOnly() {
        Subscription s1 = createSubscription("s2", "NSR:StopPlace:2", "NSR:StopPlace:3");
        s1.addFromStopPoint("NSR:StopPlace:22");
        s1.addFromStopPoint("NSR:StopPlace:222");
        s1.addToStopPoint("NSR:StopPlace:33");
        s1.addToStopPoint("NSR:StopPlace:333");

        createSubscription("s2", "NSR:StopPlace:3", "NSR:StopPlace:5");
        Subscription s0 = createSubscription("s0", "NSR:StopPlace:0", "NSR:StopPlace:2");

        //Only one in correct order
        HashSet<Subscription> affectedSubscriptions =
                processor.findAffectedSubscriptions(createVehicleJourneys(asList("1", "2", "3", "4"), null, false));
        assertEquals(1, affectedSubscriptions.size());
        assertTrue(affectedSubscriptions.contains(s1));

        //None in the opposite order
        affectedSubscriptions =
                processor.findAffectedSubscriptions(createVehicleJourneys(asList("4", "3", "2", "1"), null, false));
        assertTrue(affectedSubscriptions.isEmpty());

        //All when we don't know if all stops is present in route
        assertPresent(asList(s1, s0),
                processor.findAffectedSubscriptions(createVehicleJourneys(Collections.singletonList("2"), null, true)));

        //TODO: Both affected when not all stops is present in route
        // - should be only one but we have no way of telling as only affected stops are present in the sx message
        assertPresent(asList(s1, s0),
                processor.findAffectedSubscriptions(createVehicleJourneys(Collections.singletonList("2"), "123", true)));
    }

    @Test
    public void oneAffectedUnsubscribedStopOnJourney() throws Exception {
        createSubscription("test", "NSR:StopPlace:2", "NSR:StopPlace:3");

        String sxWithOneAffectedStopOnJourney =
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                        + "<PtSituationElement xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" "
                        + "xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\">\n"
                        + "   <CreationTime>2018-01-26T11:36:2901:00</CreationTime>\n"
                        + "   <ParticipantRef>NSB</ParticipantRef>\n"
                        + "   <SituationNumber>status-168101694</SituationNumber>\n"
                        + "   <Version>1</Version>\n"
                        + "   <Source>\n"
                        + "     <SourceType>web</SourceType>\n"
                        + "   </Source>\n"
                        + "   <Progress>published</Progress>\n"
                        + "   <ValidityPeriod>\n"
                        + "     <StartTime>2018-04-11T00:00:0002:00</StartTime>\n"
                        + "     <EndTime>2018-04-12T04:35:0002:00</EndTime>\n"
                        + "   </ValidityPeriod>\n"
                        + "   <UndefinedReason/>\n"
                        + "   <ReportType>incident</ReportType>\n"
                        + "   <Keywords/>\n"
                        + "   <Description xml:lang=\"NO\">Toget vil bytte togmateriell på Voss. Du må dessverre bytte tog på denne stasjonen."
                        + " På strekningen Bergen-Voss gjelder ikke plassreservasjoner.</Description>\n"
                        + "   <Description xml:lang=\"EN\">You must change trains at Voss. We apologize for the inconvenience. "
                        + "No seat reservations Bergen-Voss.</Description>\n"
                        + "   <Affects>\n"
                        + "     <VehicleJourneys>\n"
                        + "       <AffectedVehicleJourney>\n"
                        + "         <VehicleJourneyRef>64</VehicleJourneyRef>\n"
                        + "         <Route>\n"
                        + "           <StopPoints>\n"
                        + "             <AffectedOnly>true</AffectedOnly>\n"
                        + "             <AffectedStopPoint>\n"
                        + "               <StopPointRef>NSR:StopPlace:440</StopPointRef>\n"
                        + "               <StopPointName>Voss</StopPointName>\n"
                        + "               <Extensions>\n"
                        + "                 <BoardingRelevance xmlns:ifopt=\"http://www.ifopt.org.uk/ifopt\" "
                        + "xmlns:datex2=\"http://datex2.eu/schema/2_0RC1/2_0\" "
                        + "xmlns:acsb=\"http://www.ifopt.org.uk/acsb\" arrive=\"true\" depart=\"true\" pass=\"true\" transfer=\"true\"/>\n"
                        + "               </Extensions>\n"
                        + "             </AffectedStopPoint>\n"
                        + "           </StopPoints>\n"
                        + "         </Route>\n"
                        + "         <OriginAimedDepartureTime>2018-04-11T00:00:0002:00</OriginAimedDepartureTime>\n"
                        + "       </AffectedVehicleJourney>\n"
                        + "     </VehicleJourneys>\n"
                        + "   </Affects>\n"
                        + "</PtSituationElement>\n";
        PtSituationElement ptSituationElement = siriMarshaller.unmarshall(sxWithOneAffectedStopOnJourney, PtSituationElement.class);

        assertNotNull(ptSituationElement);
        assertNotNull(ptSituationElement.getAffects());

        HashSet<String> affectedStopPointRefs = processor.findAffectedStopPlaceRefs(ptSituationElement.getAffects().getStopPlaces());
        assertEquals(0, affectedStopPointRefs.size());
        List<AffectedVehicleJourneyStructure> affectedVehicleJourneies1 =
                ptSituationElement.getAffects().getVehicleJourneys().getAffectedVehicleJourneies();
        HashSet<Subscription> affectedSubscriptions =
                processor.findAffectedSubscriptions(affectedVehicleJourneies1);
        assertEquals(0, affectedSubscriptions.size());
        //modify affected journey so we have no route data
        List<VehicleJourneyRef> vehicleJourneyReves = affectedVehicleJourneies1.get(0).getVehicleJourneyReves();
        vehicleJourneyReves.clear();
        VehicleJourneyRef journeyRef = new VehicleJourneyRef();
        journeyRef.setValue("NoHit");
        vehicleJourneyReves.add(journeyRef);
        affectedSubscriptions = processor.findAffectedSubscriptions(affectedVehicleJourneies1);
        assertEquals(0, affectedSubscriptions.size());
    }

    @Test
    public void testSubscriptionsWithLineAndStops() {
        //These are supposed to be found
        Subscription sStopsLine = createSubscription("from-to-line", "NSR:StopPlace:10", "NSR:StopPlace:20", null);
        List<Subscription> expectedSubscriptions = Collections.singletonList(sStopsLine);
        //These are not supposed to be found as they have one or more conditions set that doesn't match:
        createSubscription("line", null, null, "line#1"); //TODO: Is not found as the SX message does not contain LineRef
        createSubscription("NOHIT3-from-to-line", "NSR:StopPlace:40", "NSR:StopPlace:30", "line#1");
        createSubscription("NOHIT4-from-to-line", "NSR:StopPlace:10", "NSR:StopPlace:20", "line#2");
        createSubscription("NOHIT8-line", null, null, "line#2");

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
            errorMessage = "Expected " + expectedSubscriptions.size() + " subscriptions, but found " + actualSubscriptions.size() + ". ";
        }
        if (!missing.isEmpty()) {
            errorMessage += "Missing subscriptions: " + Arrays.toString(missing.stream().map(Subscription::getName).toArray()) + ". ";
        }
        if (!toMany.isEmpty()) {
            errorMessage += "Unexpected subscriptions: " + Arrays.toString(toMany.stream().map(Subscription::getName).toArray()) + ". ";
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

    private StopPointRef createStopPointRef(String value) {
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(value);
        return stopPointRef;
    }

    private Subscription createSubscription(String name, String fromStop, String toStop, String line) {
        Subscription s = new Subscription();
        s.setPushAddress("push");
        if (name != null) {

            s.setName(name);
        }
        if (fromStop != null) {

            s.addFromStopPoint(fromStop);
        }
        if (toStop != null) {

            s.addToStopPoint(toStop);
        }
        if (line != null) {

            s.addLineRef(line);
        }
        return subscriptionManager.addOrUpdate(s);
    }

    private Subscription createSubscription(String name, String fromStop, String toStop) {
        return createSubscription(name, fromStop, toStop, null);
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