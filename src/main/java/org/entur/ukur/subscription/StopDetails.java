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

package org.entur.ukur.subscription;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class StopDetails {
    private final String stopPointRef;
    private Set<DeviationType> deviationTypes = new HashSet<>();
    private boolean delayedDeparture = false;
    private boolean delayedArrival = false;
    private Duration delayDuration = null;

    public StopDetails(String stopPointRef) {
        this.stopPointRef = stopPointRef;
    }

    public static StopDetails cancelled(String stopPointRef) {
        StopDetails stopDetails = new StopDetails(stopPointRef);
        stopDetails.deviationTypes.add(DeviationType.CANCELED);
        return stopDetails;
    }

    public static StopDetails trackChange(String stopPointRef) {
        StopDetails stopDetails = new StopDetails(stopPointRef);
        stopDetails.deviationTypes.add(DeviationType.TRACK_CHANGE);
        return stopDetails;
    }

    public static StopDetails delayed(String stopPointRef, boolean delayedDeparture, boolean delayedArrival, Duration delayedDuration) {
        StopDetails stopDetails = new StopDetails(stopPointRef);
        stopDetails.delayedDeparture = delayedDeparture;
        stopDetails.delayedArrival= delayedArrival;
        stopDetails.delayDuration = delayedDuration;
        stopDetails.deviationTypes.add(DeviationType.DELAYED);
        return stopDetails;
    }

    public String getStopPointRef() {
        return stopPointRef;
    }

    public Set<DeviationType> getDeviationTypes() {
        return deviationTypes;
    }

    public boolean isDelayedDeparture() {
        return delayedDeparture;
    }

    public boolean isDelayedArrival() {
        return delayedArrival;
    }

    public Duration getDelayDuration() {
        return delayDuration;
    }
}
