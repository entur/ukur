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

import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.routedata.LiveRouteService;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

        SubscriptionManager subscriptionManager = new SubscriptionManager(new HashMap<>(), new HashMap<>(), new HashMap<>(), new SiriMarshaller());
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

        //All when we don't know if all stops is present in route
        affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(Arrays.asList("2"), null, true));
        assertEquals(2, affectedSubscriptions.size());
        Set<String> names = affectedSubscriptions.stream().map(s -> s.getName()).collect(Collectors.toSet());
        assertTrue(names.contains("s1"));
        assertTrue(names.contains("s0"));

        //Only one when we look up the route if not all stops is present in route
        when(liveRouteServiceMock.getJourneys()).thenReturn(Collections.singletonList(createLiveJourney("123", Arrays.asList("1", "2", "3"))));
        affectedSubscriptions = processor.findAffectedSubscriptions(createVehicleJourneys(Arrays.asList("2"), "123", true));
        assertEquals(1, affectedSubscriptions.size());
        assertEquals("s1", affectedSubscriptions.iterator().next().getName());
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
            stopPoints.setAffectedOnly(affectedOnly);
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