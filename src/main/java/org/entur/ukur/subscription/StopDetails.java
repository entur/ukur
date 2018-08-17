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

public class StopDetails {
    private final String stopPointRef;
    private boolean cancelledOrTrackChange = false;
    private boolean delayedDeparture = false;
    private boolean delayedArrival = false;

    public StopDetails(String stopPointRef) {
        this.stopPointRef = stopPointRef;
    }

    public static StopDetails cancelledOrTrackChange(String stopPointRef) {
        StopDetails stopDetails = new StopDetails(stopPointRef);
        stopDetails.cancelledOrTrackChange = true;
        return stopDetails;
    }

    public static StopDetails delayed(String stopPointRef, boolean delayedDeparture, boolean delayedArrival) {
        StopDetails stopDetails = new StopDetails(stopPointRef);
        stopDetails.delayedDeparture = delayedDeparture;
        stopDetails.delayedArrival= delayedArrival;
        return stopDetails;
    }

    public String getStopPointRef() {
        return stopPointRef;
    }

    public boolean isCancelledOrTrackChange() {
        return cancelledOrTrackChange;
    }

    public boolean isDelayedDeparture() {
        return delayedDeparture;
    }

    public boolean isDelayedArrival() {
        return delayedArrival;
    }
}
