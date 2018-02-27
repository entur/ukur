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
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.routedata.LiveRouteService;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Test;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NsbSXSubscriptionProcessorTest {

    @Test
    public void findAffectedSubscriptions() throws JAXBException {
        Subscription s1 = new Subscription();
        s1.setName("s1");
        s1.addFromStopPoint("NSR:StopPlace:2");
        s1.addToStopPoint("NSR:StopPlace:3");
        s1.setPushAddress("push");

        Subscription s2 = new Subscription();
        s2.setName("s2");
        s2.addFromStopPoint("NSR:StopPlace:3");
        s2.addToStopPoint("NSR:StopPlace:5");
        s2.setPushAddress("push");

        Subscription s0 = new Subscription();
        s0.setName("s0");
        s0.addFromStopPoint("NSR:StopPlace:0");
        s0.addToStopPoint("NSR:StopPlace:2");
        s0.setPushAddress("push");
        SubscriptionManager subscriptionManager = createSubscriptionManager();
        subscriptionManager.add(s1);
        subscriptionManager.add(s2);
        subscriptionManager.add(s0);
        LiveRouteService liveRouteServiceMock = mock(LiveRouteService.class);
        NsbSXSubscriptionProcessor processor = new NsbSXSubscriptionProcessor(subscriptionManager,
                new SiriMarshaller(), liveRouteServiceMock, mock(FileStorageService.class));

        //Only one in correct order
        HashSet<Subscription> affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(Arrays.asList("1", "2", "3", "4"), null, false));
        assertEquals(1, affectedSubscriptions.size());
        assertTrue(affectedSubscriptions.contains(s1));

        //None in the opposite order
        affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(Arrays.asList("4", "3", "2", "1"), null, false));
        assertTrue(affectedSubscriptions.isEmpty());

        //All when we don't know if all stops is present in camelroute
        affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(Collections.singletonList("2"), null, true));
        assertEquals(2, affectedSubscriptions.size());
        Set<String> names = affectedSubscriptions.stream().map(Subscription::getName).collect(Collectors.toSet());
        assertTrue(names.contains("s1"));
        assertTrue(names.contains("s0"));

        //Only one when we look up the camelroute if not all stops is present in camelroute
        when(liveRouteServiceMock.getJourneys()).thenReturn(Collections.singletonList(createLiveJourney("123", Arrays.asList("1", "2", "3"))));
        affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(Collections.singletonList("2"), "123", true));
        assertEquals(1, affectedSubscriptions.size());
        assertEquals("s1", affectedSubscriptions.iterator().next().getName());
    }

    private SubscriptionManager createSubscriptionManager() throws JAXBException {
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        return new SubscriptionManager(new DataStorageService(new HashMap<>(), new HashMap<>(), new HashMap<>(), liveJourneyIMap), new SiriMarshaller());
    }

    @Test
    public void one_affected_unsubscribed_stop_on_journey() throws Exception {
        Subscription s1 = new Subscription();
        s1.setName("test");
        s1.addFromStopPoint("NSR:StopPlace:2");
        s1.addToStopPoint("NSR:StopPlace:3");
        s1.setPushAddress("push");

        SubscriptionManager subscriptionManager = createSubscriptionManager();
        subscriptionManager.add(s1);
        LiveRouteService liveRouteServiceMock = mock(LiveRouteService.class);
        SiriMarshaller siriMarshaller = new SiriMarshaller();
        NsbSXSubscriptionProcessor processor = new NsbSXSubscriptionProcessor(subscriptionManager,
                siriMarshaller, liveRouteServiceMock, mock(FileStorageService.class));

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
        PtSituationElement ptSituationElement = siriMarshaller.unmarhall(SX_with_one_affected_stop_on_journey, PtSituationElement.class);

        when(liveRouteServiceMock.getJourneys()).thenReturn(Collections.singletonList(createLiveJourney("64", Arrays.asList("1", "2", "440"))));

        assertNotNull(ptSituationElement);
        assertNotNull(ptSituationElement.getAffects());

        HashSet<String> affectedStopPointRefs = processor.findAffectedStopPointRefs(ptSituationElement.getAffects());
        assertEquals(0, affectedStopPointRefs.size());
        HashSet<Subscription> affectedSubscriptions = processor.findAffectedSubscriptions(ptSituationElement.getAffects().getVehicleJourneys());
        assertEquals(0, affectedSubscriptions.size());
        //modify affected journey so we have no camelroute data
        List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = ptSituationElement.getAffects().getVehicleJourneys().getAffectedVehicleJourneies();
        List<VehicleJourneyRef> vehicleJourneyReves = affectedVehicleJourneies.get(0).getVehicleJourneyReves();
        vehicleJourneyReves.clear();
        VehicleJourneyRef journeyRef = new VehicleJourneyRef();
        journeyRef.setValue("NoHit");
        vehicleJourneyReves.add(journeyRef);
        affectedSubscriptions = processor.findAffectedSubscriptions(ptSituationElement.getAffects().getVehicleJourneys());
        assertEquals(0, affectedSubscriptions.size());

    }

    private LiveJourney createLiveJourney(String vehicleJourneyRef, List<String> stops) {
        EstimatedVehicleJourney someJourney = new EstimatedVehicleJourney();
        VehicleRef value = new VehicleRef();
        value.setValue(vehicleJourneyRef);
        someJourney.setVehicleRef(value);
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        someJourney.setEstimatedCalls(estimatedCalls);
        List<EstimatedCall> calls = estimatedCalls.getEstimatedCalls();
        ZonedDateTime now = ZonedDateTime.now();
        for (String stop : stops) {
            now = now.plusMinutes(10);
            EstimatedCall call = new EstimatedCall();
            StopPointRef stopPointRef = new StopPointRef();
            stopPointRef.setValue("NSR:StopPlace:"+stop);
            call.setStopPointRef(stopPointRef);
            call.getStopPointNames();
            call.setAimedArrivalTime(now);
            call.setExpectedArrivalTime(now);
            call.setArrivalStatus(CallStatusEnumeration.ON_TIME);
            call.setAimedDepartureTime(now);
            call.setExpectedDepartureTime(now);
            call.setDepartureStatus(CallStatusEnumeration.ON_TIME);
            calls.add(call);
        }
        return new LiveJourney(someJourney);
    }

    private AffectsScopeStructure.VehicleJourneys createVehicleJourneys(List<String> stops, String vehicleJourneyRef, boolean affectedOnly) {
        AffectsScopeStructure.VehicleJourneys vehicleJourneys = new AffectsScopeStructure.VehicleJourneys();
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
            AffectedStopPointStructure affectedStopPointStructure = new AffectedStopPointStructure();
            StopPointRef stopPointRef = new StopPointRef();
            stopPointRef.setValue("NSR:StopPlace:"+stop);
            affectedStopPointStructure.setStopPointRef(stopPointRef);
            affectedStopPointsAndLinkProjectionToNextStopPoints.add(affectedStopPointStructure);
        }

        affectedVehicleJourneyStructure.getRoutes().add(routeStructure);
        vehicleJourneys.getAffectedVehicleJourneies().add(affectedVehicleJourneyStructure);
        return vehicleJourneys;
    }

}