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

import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.junit.Test;
import uk.org.siri.siri20.*;

import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LiveJourneyTest {

    @Test
    public void verifyInsertStopPlaces() {
        EstimatedVehicleJourney estimatedVehicleJourney = new EstimatedVehicleJourney();

        LineRef lineRef = new LineRef();
        lineRef.setValue("NSB:Line:Test1");
        estimatedVehicleJourney.setLineRef(lineRef);

        VehicleRef vehicleRef = new VehicleRef();
        vehicleRef.setValue("1");
        estimatedVehicleJourney.setVehicleRef(vehicleRef);

        DatedVehicleJourneyRef datedVehicleJourneyRef = new DatedVehicleJourneyRef();
        datedVehicleJourneyRef.setValue("1:2018-04-03");
        estimatedVehicleJourney.setDatedVehicleJourneyRef(datedVehicleJourneyRef);

        DirectionRefStructure directionRef = new DirectionRefStructure();
        directionRef.setValue("direction");
        estimatedVehicleJourney.setDirectionRef(directionRef);

        estimatedVehicleJourney.setRecordedCalls(new EstimatedVehicleJourney.RecordedCalls());
        RecordedCall recordedCall = createRecordedCall("NSR:Quay:R1", "RecordedCall 1", 2);
        estimatedVehicleJourney.getRecordedCalls().getRecordedCalls().add(recordedCall);
        estimatedVehicleJourney.getRecordedCalls().getRecordedCalls().add(createRecordedCall("NSR:Quay:R2", "RecordedCall 2", 1));

        estimatedVehicleJourney.setEstimatedCalls(new EstimatedVehicleJourney.EstimatedCalls());
        EstimatedCall estimatedCall = createEstimatedCall("NSR:Quay:E1", "EstimatedCall 1", 1);
        estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls().add(estimatedCall);
        estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls().add(createEstimatedCall("NSR:Quay:E2", "EstimatedCall 2", 2));


        QuayAndStopPlaceMappingService mock = mock(QuayAndStopPlaceMappingService.class);


        //First test without any quay to stopplace mappings:
        LiveJourney quaysOnly = new LiveJourney(estimatedVehicleJourney, mock);
        List<Call> quaysOnlyCalls = quaysOnly.getCalls();
        assertEquals(4, quaysOnlyCalls.size());
        assertEquals("1", quaysOnly.getVehicleRef());
        assertEquals("1:2018-04-03", quaysOnly.getDatedVehicleJourneyRef());
        assertEquals("NSB:Line:Test1", quaysOnly.getLineRef());
        assertEquals("direction", quaysOnly.getDirectionRef());
        //Make sure both a recorded and an estimated call have correct values:
        Call recorded = quaysOnlyCalls.get(0);
        assertEquals("RecordedCall 1", recorded.getStopPointName());
        assertEquals("NSR:Quay:R1", recorded.getStopPointRef());
        assertEquals(recordedCall.getAimedArrivalTime(), recorded.getAimedArrivalTime());
        assertEquals(recordedCall.getActualArrivalTime(), recorded.getArrivalTime());
        assertEquals(CallStatusEnumeration.ON_TIME, recorded.getArrivalStatus());
        assertEquals(recordedCall.getAimedDepartureTime(), recorded.getAimedDepartureTime());
        assertEquals(recordedCall.getActualDepartureTime(), recorded.getDepartureTime());
        assertEquals(CallStatusEnumeration.ON_TIME, recorded.getDepartureStatus());
        assertFalse(recorded.isCancellation());
        assertFalse(recorded.isExtraCall());
        assertFalse(recorded.isEstimated());
        Call estimated = quaysOnlyCalls.get(2);
        assertEquals("EstimatedCall 1", estimated.getStopPointName());
        assertEquals("NSR:Quay:E1", estimated.getStopPointRef());
        assertEquals(estimatedCall.getAimedArrivalTime(), estimated.getAimedArrivalTime());
        assertEquals(estimatedCall.getExpectedArrivalTime(), estimated.getArrivalTime());
        assertEquals(CallStatusEnumeration.ON_TIME, estimated.getArrivalStatus());
        assertEquals(estimatedCall.getAimedDepartureTime(), estimated.getAimedDepartureTime());
        assertEquals(estimatedCall.getExpectedDepartureTime(), estimated.getDepartureTime());
        assertEquals(CallStatusEnumeration.ON_TIME, estimated.getDepartureStatus());
        assertFalse(estimated.isCancellation());
        assertFalse(estimated.isExtraCall());
        assertTrue(estimated.isEstimated());


        //Then when we mock the quay mappings:
        when(mock.mapQuayToStopPlace(eq("NSR:Quay:R1"))).thenReturn("NSR:StopPlace:StopR1");
        when(mock.mapQuayToStopPlace(eq("NSR:Quay:R2"))).thenReturn("NSR:StopPlace:StopR2");
        when(mock.mapQuayToStopPlace(eq("NSR:Quay:E1"))).thenReturn("NSR:StopPlace:StopE1");
        when(mock.mapQuayToStopPlace(eq("NSR:Quay:E2"))).thenReturn("NSR:StopPlace:StopE2");
        LiveJourney withStops = new LiveJourney(estimatedVehicleJourney, mock);
        List<Call> calls = withStops.getCalls();
        assertEquals(8, calls.size());
        assertEquals("NSR:StopPlace:StopR1", calls.get(0).getStopPointRef());
        assertEquals("NSR:Quay:R1", calls.get(1).getStopPointRef());
        assertEquals("NSR:StopPlace:StopR2", calls.get(2).getStopPointRef());
        assertEquals("NSR:Quay:R2", calls.get(3).getStopPointRef());
        assertEquals("NSR:StopPlace:StopE1", calls.get(4).getStopPointRef());
        assertEquals("NSR:Quay:E1", calls.get(5).getStopPointRef());
        assertEquals("NSR:StopPlace:StopE2", calls.get(6).getStopPointRef());
        assertEquals("NSR:Quay:E2", calls.get(7).getStopPointRef());

        //assert that all fields are present in both the quay-call and its stopplace clone
        Call callR1stopplace = calls.get(0);
        Call callR1Quay = calls.get(1);
        assertEquals("NSR:Quay:R1", callR1Quay.getStopPointRef());
        assertEquals("NSR:StopPlace:StopR1", callR1stopplace.getStopPointRef());
        assertEquals(callR1stopplace.getStopPointName(), callR1Quay.getStopPointName());
        assertEquals(callR1stopplace.getAimedArrivalTime(), callR1Quay.getAimedArrivalTime());
        assertEquals(callR1stopplace.getArrivalTime(), callR1Quay.getArrivalTime());
        assertEquals(callR1stopplace.getArrivalStatus(), callR1Quay.getArrivalStatus());
        assertEquals(callR1stopplace.getAimedDepartureTime(), callR1Quay.getAimedDepartureTime());
        assertEquals(callR1stopplace.getDepartureTime(), callR1Quay.getDepartureTime());
        assertEquals(callR1stopplace.getDepartureStatus(), callR1Quay.getDepartureStatus());
        assertEquals(callR1stopplace.isCancellation(), callR1Quay.isCancellation());
        assertEquals(callR1stopplace.isExtraCall(), callR1Quay.isExtraCall());
        assertEquals(callR1stopplace.isEstimated(), callR1Quay.isEstimated());

    }

    private RecordedCall createRecordedCall(String stopPoint, String StopPointName, int hoursAgo) {
        RecordedCall recordedCall = new RecordedCall();
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(stopPoint);
        recordedCall.setStopPointRef(stopPointRef);
        NaturalLanguageStringStructure name = new NaturalLanguageStringStructure();
        name.setValue(StopPointName);
        recordedCall.getStopPointNames().add(name);
        ZonedDateTime time = ZonedDateTime.now().minusHours(hoursAgo);
        recordedCall.setAimedArrivalTime(time);
        recordedCall.setActualArrivalTime(time.plusSeconds(1));
        recordedCall.setAimedDepartureTime(time.plusSeconds(2));
        recordedCall.setActualDepartureTime(time.plusSeconds(3));
        recordedCall.setCancellation(false);
        recordedCall.setExtraCall(false);
        return recordedCall;
    }

    private EstimatedCall createEstimatedCall(String stopPoint, String StopPointName, int hoursTo) {
        EstimatedCall estimatedCall = new EstimatedCall();
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(stopPoint);
        estimatedCall.setStopPointRef(stopPointRef);
        NaturalLanguageStringStructure name = new NaturalLanguageStringStructure();
        name.setValue(StopPointName);
        estimatedCall.getStopPointNames().add(name);
        ZonedDateTime time = ZonedDateTime.now().plusHours(hoursTo);
        estimatedCall.setAimedArrivalTime(time);
        estimatedCall.setExpectedArrivalTime(time.plusSeconds(1));
        estimatedCall.setArrivalStatus(CallStatusEnumeration.ON_TIME);
        estimatedCall.setAimedDepartureTime(time.plusSeconds(2));
        estimatedCall.setExpectedDepartureTime(time.plusSeconds(3));
        estimatedCall.setDepartureStatus(CallStatusEnumeration.ON_TIME);
        estimatedCall.setCancellation(false);
        estimatedCall.setExtraCall(false);
        return estimatedCall;
    }

}