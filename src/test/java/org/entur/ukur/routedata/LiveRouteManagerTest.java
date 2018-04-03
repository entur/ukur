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

package org.entur.ukur.routedata;

import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.entur.ukur.service.DataStorageHazelcastService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.junit.Test;
import uk.org.siri.siri20.*;

import java.time.ZonedDateTime;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class LiveRouteManagerTest {

    @Test
    public void updateJourney() {
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        LiveRouteManager service = new LiveRouteManager(new DataStorageHazelcastService(new HashMap<>(), new HashMap<>(), liveJourneyIMap), mock(QuayAndStopPlaceMappingService.class));
        service.updateJourney(createEstimatedVehicleJourney("1", "NSB:Line:Test1", false, ZonedDateTime.now().plusHours(1)));
        service.updateJourney(createEstimatedVehicleJourney("2", "NSB:Line:Test1", true, ZonedDateTime.now().plusHours(2)));
        service.updateJourney(createEstimatedVehicleJourney("3", "NSB:Line:Test2", false, ZonedDateTime.now()));
        service.updateJourney(createEstimatedVehicleJourney("3", "NSB:Line:Test2", true, ZonedDateTime.now().minusMinutes(1)));
        assertEquals(3, service.getJourneys().size());
        assertEquals(0, service.getJourneys("NSB:Line:Nada").size());
        assertEquals(1, service.getJourneys("NSB:Line:Test2").size());
        assertEquals(2, service.getJourneys("NSB:Line:Test1").size());
        service.flushOldJourneys();
        assertEquals(3, service.getJourneys().size());
        service.updateJourney(createEstimatedVehicleJourney("3", "NSB:Line:Test2", true, ZonedDateTime.now().minusMinutes(15).minusSeconds(1)));
        service.flushOldJourneys();
        assertEquals(2, service.getJourneys().size());
        assertEquals(0, service.getJourneys("NSB:Line:Test2").size());
        assertEquals(2, service.getJourneys("NSB:Line:Test1").size());
    }

    private EstimatedVehicleJourney createEstimatedVehicleJourney(String vehicle, String line, boolean onlyRecordedCalls, ZonedDateTime last) {
        EstimatedVehicleJourney evj = new EstimatedVehicleJourney();
        evj.setIsCompleteStopSequence(true);
        if (line != null) {
            LineRef lineRef = new LineRef();
            lineRef.setValue(line);
            evj.setLineRef(lineRef);
        }

        if (vehicle != null) {
            VehicleRef vehicleRef = new VehicleRef();
            vehicleRef.setValue(vehicle);
            evj.setVehicleRef(vehicleRef);
        }

        ZonedDateTime second = last.minusMinutes(5);
        ZonedDateTime first = second.minusMinutes(10);

        evj.setRecordedCalls(new EstimatedVehicleJourney.RecordedCalls());
        evj.setEstimatedCalls(new EstimatedVehicleJourney.EstimatedCalls());
        evj.getRecordedCalls().getRecordedCalls().add(newRecordedCall("NSR:StopPlace:1", first));
        if (onlyRecordedCalls) {
            evj.getRecordedCalls().getRecordedCalls().add(newRecordedCall("NSR:StopPlace:2", second));
            evj.getRecordedCalls().getRecordedCalls().add(newRecordedCall("NSR:StopPlace:3", last));
        } else {
            evj.getEstimatedCalls().getEstimatedCalls().add(newEstimatedCall("NSR:StopPlace:2", second));
            evj.getEstimatedCalls().getEstimatedCalls().add(newEstimatedCall("NSR:StopPlace:3", last));
        }

        return evj;
    }

    private EstimatedCall newEstimatedCall(String stop, ZonedDateTime arrival) {
        EstimatedCall call = new EstimatedCall();
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(stop);
        call.setStopPointRef(stopPointRef);
        call.setAimedDepartureTime(arrival);
        call.setExpectedDepartureTime(arrival);
        call.setAimedArrivalTime(arrival);
        call.setExpectedArrivalTime(arrival);
        return call;
    }

    private RecordedCall newRecordedCall(String stop, ZonedDateTime arrival) {
        RecordedCall call = new RecordedCall();
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(stop);
        call.setStopPointRef(stopPointRef);
        call.setAimedDepartureTime(arrival);
        call.setExpectedDepartureTime(arrival);
        call.setActualDepartureTime(arrival);
        call.setAimedArrivalTime(arrival);
        call.setExpectedArrivalTime(arrival);
        call.setActualArrivalTime(arrival);
        return call;
    }
}