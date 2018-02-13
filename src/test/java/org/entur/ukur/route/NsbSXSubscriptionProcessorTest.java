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

import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Test;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class NsbSXSubscriptionProcessorTest {

    @Test
    public void findAffectedSubscriptions() throws JAXBException {
        Subscription s1 = new Subscription();
        s1.addFromStopPoint("NSR:StopPlace:2");
        s1.addToStopPoint("NSR:StopPlace:3");
        s1.setPushAddress("push");

        Subscription s2 = new Subscription();
        s2.addFromStopPoint("NSR:StopPlace:3");
        s2.addToStopPoint("NSR:StopPlace:5");
        s2.setPushAddress("push");

        Subscription s0 = new Subscription();
        s0.addFromStopPoint("NSR:StopPlace:0");
        s0.addToStopPoint("NSR:StopPlace:2");
        s0.setPushAddress("push");

        SubscriptionManager subscriptionManager = new SubscriptionManager(new HashMap<>(), new HashMap<>(), new HashMap<>(), new SiriMarshaller());
        subscriptionManager.add(s1);
        subscriptionManager.add(s2);
        subscriptionManager.add(s0);
        NsbSXSubscriptionProcessor processor = new NsbSXSubscriptionProcessor(subscriptionManager, new SiriMarshaller(), mock(FileStorageService.class));

        //Only one in correct order
        AffectsScopeStructure.VehicleJourneys vehicleJourneys = createVehicleJourneys(Arrays.asList("1", "2", "3", "4"));
        HashSet<Subscription> affectedSubscriptions = processor.findAffectedSubscriptions(vehicleJourneys);
        assertEquals(1, affectedSubscriptions.size());
        assertTrue(affectedSubscriptions.contains(s1));

        //All when we don't know if all stops is present in route
        vehicleJourneys.getAffectedVehicleJourneies().get(0).getRoutes().get(0).getStopPoints().setAffectedOnly(true);
        affectedSubscriptions = processor.findAffectedSubscriptions(vehicleJourneys);
        assertEquals(3, affectedSubscriptions.size());

        //None in the opposite order
        vehicleJourneys = createVehicleJourneys(Arrays.asList("4", "3", "2", "1"));
        affectedSubscriptions = processor.findAffectedSubscriptions(vehicleJourneys);
        assertTrue(affectedSubscriptions.isEmpty());
    }

    private AffectsScopeStructure.VehicleJourneys createVehicleJourneys(List<String> stops) {
        AffectsScopeStructure.VehicleJourneys vehicleJourneys = new AffectsScopeStructure.VehicleJourneys();
        AffectedVehicleJourneyStructure affectedVehicleJourneyStructure = new AffectedVehicleJourneyStructure();
        AffectedRouteStructure routeStructure = new AffectedRouteStructure();
        AffectedRouteStructure.StopPoints stopPoints = new AffectedRouteStructure.StopPoints();
        routeStructure.setStopPoints(stopPoints);
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