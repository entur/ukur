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

import org.apache.commons.lang3.SerializationUtils;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import uk.org.siri.siri20.*;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.*;

@SuppressWarnings("WeakerAccess")
public class LiveJourney implements Serializable {

    private final ZonedDateTime lastArrivalTime ;
    private String directionRef = null;
    private String vehicleRef = null;
    private String lineRef = null;
    private String datedVehicleJourneyRef = null;
    private LinkedList<Call> calls = new LinkedList<>();

    public LiveJourney(EstimatedVehicleJourney journey, QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {

        if (journey.getLineRef() != null) {
            lineRef = journey.getLineRef().getValue();
        }
        if (journey.getDatedVehicleJourneyRef()!= null) {
            datedVehicleJourneyRef = journey.getDatedVehicleJourneyRef().getValue();
        }
        if (journey.getVehicleRef() != null) {
            vehicleRef = journey.getVehicleRef().getValue();
        }
        if (journey.getDirectionRef() != null) {
            directionRef = journey.getDirectionRef().getValue();
        }
        add(journey.getRecordedCalls());
        add(journey.getEstimatedCalls());
        insertStopPlacesForQuays(quayAndStopPlaceMappingService);
        Call lastCall = calls.get(calls.size() - 1); //simply assumes calls are in correct order
        lastArrivalTime = lastCall.getArrivalTime();
    }

    /**
     * If there are quays in the calls list, we insert new calls (clone of the quay call) with stopplace id
     * so existing logic will handle subscription for both quays and stopplaces.
     */
    private void insertStopPlacesForQuays(QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {
        for (int i = calls.size()-1; i > -1 ; i--) {
            Call call = calls.get(i);
            String stopPointRef = call.getStopPointRef();
            if (stopPointRef.startsWith("NSR:Quay:")) {
                String stopPlace = quayAndStopPlaceMappingService.mapQuayToStopPlace(stopPointRef);
                if (stopPlace != null) {
                    Call callWithStopPlace = SerializationUtils.clone(call);
                    callWithStopPlace.setStopPointRef(stopPlace);
                    calls.add(i, callWithStopPlace);
                }
            }
        }
    }

    private void add(EstimatedVehicleJourney.RecordedCalls recordedCalls) {
        if (recordedCalls == null) {
            return;
        }
        for (RecordedCall recordedCall : recordedCalls.getRecordedCalls()) {
            calls.add(new Call(recordedCall));
        }
    }

    private void add(EstimatedVehicleJourney.EstimatedCalls estimatedCalls) {
        if (estimatedCalls == null) {
            return;
        }
        for (EstimatedCall estimatedCall : estimatedCalls.getEstimatedCalls()) {
            calls.add(new Call(estimatedCall));
        }
    }

    public ZonedDateTime getLastArrivalTime() {
        return lastArrivalTime;
    }

    public String getDirectionRef() {
        return directionRef;
    }

    public String getVehicleRef() {
        return vehicleRef;
    }

    public String getLineRef() {
        return lineRef;
    }

    public String getDatedVehicleJourneyRef() {
        return datedVehicleJourneyRef;
    }

    public List<Call> getCalls() {
        return Collections.unmodifiableList(calls);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiveJourney liveRoute = (LiveJourney) o;
        return Objects.equals(lastArrivalTime, liveRoute.lastArrivalTime) &&
                Objects.equals(directionRef, liveRoute.directionRef) &&
                Objects.equals(vehicleRef, liveRoute.vehicleRef) &&
                Objects.equals(lineRef, liveRoute.lineRef) &&
                Objects.equals(datedVehicleJourneyRef, liveRoute.datedVehicleJourneyRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastArrivalTime, directionRef, vehicleRef, lineRef, datedVehicleJourneyRef);
    }
}
