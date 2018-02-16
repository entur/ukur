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

import uk.org.siri.siri20.*;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

public class Call implements Serializable {

    private boolean extraCall;
    private boolean cancellation;
    private final boolean estimated;
    private CallStatusEnumeration departureStatus;
    private ZonedDateTime aimedArrivalTime;
    private ZonedDateTime arrivalTime;
    private CallStatusEnumeration arrivalStatus;
    private ZonedDateTime aimedDepartureTime;
    private ZonedDateTime departureTime;
    private String stopPointRef;
    private String stopPointName;

    public Call(RecordedCall recordedCall) {
        estimated = false;
        setStopPointRef(recordedCall.getStopPointRef());
        setStopPointName(recordedCall.getStopPointNames());
        setAimedArrivalTime(recordedCall.getAimedArrivalTime());
        setArrivalTime(recordedCall.getActualArrivalTime());
        setArrivalStatus(calculateStatus(recordedCall.getAimedArrivalTime(), recordedCall.getActualArrivalTime()));
        setAimedDepartureTime(recordedCall.getAimedDepartureTime());
        setDepartureTime(recordedCall.getActualDepartureTime());
        setDepartureStatus(calculateStatus(recordedCall.getAimedDepartureTime(), recordedCall.getActualDepartureTime()));
        setCancellation(recordedCall.isCancellation());
        setExtraCall(recordedCall.isExtraCall());
    }

    public Call(EstimatedCall estimatedCall) {
        estimated = true;
        setStopPointRef(estimatedCall.getStopPointRef());
        setStopPointName(estimatedCall.getStopPointNames());
        setAimedArrivalTime(estimatedCall.getAimedArrivalTime());
        setArrivalTime(estimatedCall.getExpectedArrivalTime());
        setArrivalStatus(estimatedCall.getArrivalStatus());
        setAimedDepartureTime(estimatedCall.getAimedDepartureTime());
        setDepartureTime(estimatedCall.getExpectedDepartureTime());
        setDepartureStatus(estimatedCall.getDepartureStatus());
        setCancellation(estimatedCall.isCancellation());
        setExtraCall(estimatedCall.isExtraCall());
    }

    private CallStatusEnumeration calculateStatus(ZonedDateTime aimed, ZonedDateTime actual) {
        if (aimed!= null && actual!=null) {
            long between = ChronoUnit.MINUTES.between(aimed, actual);
            if (between > 0) {
                return CallStatusEnumeration.DELAYED;
            } else if (between < 0 ) {
                return CallStatusEnumeration.EARLY;
            } else return CallStatusEnumeration.ON_TIME;
        }
        return null;
    }

    public boolean isExtraCall() {
        return extraCall;
    }

    public boolean isCancellation() {
        return cancellation;
    }

    public boolean isEstimated() {
        return estimated;
    }

    public CallStatusEnumeration getDepartureStatus() {
        return departureStatus;
    }

    public ZonedDateTime getAimedArrivalTime() {
        return aimedArrivalTime;
    }

    public ZonedDateTime getArrivalTime() {
        return arrivalTime;
    }

    public CallStatusEnumeration getArrivalStatus() {
        return arrivalStatus;
    }

    public ZonedDateTime getAimedDepartureTime() {
        return aimedDepartureTime;
    }

    public ZonedDateTime getDepartureTime() {
        return departureTime;
    }

    public String getStopPointRef() {
        return stopPointRef;
    }

    public String getStopPointName() {
        return stopPointName;
    }

    private void setExtraCall(Boolean extraCall) {
        this.extraCall = Boolean.TRUE.equals(extraCall);
    }

    private void setCancellation(Boolean cancellation) {
        this.cancellation = Boolean.TRUE.equals(cancellation);
    }

    private void setDepartureStatus(CallStatusEnumeration departureStatus) {
        this.departureStatus = departureStatus;
    }

    private void setStopPointName(List<NaturalLanguageStringStructure> stopPointNames) {
        if (stopPointNames != null && !stopPointNames.isEmpty()) {
            //TODO: simply picks the first as NSB/BaneNOR only provides one
            NaturalLanguageStringStructure naturalLanguageStringStructure = stopPointNames.get(0);
            stopPointName = naturalLanguageStringStructure.getValue();
        }
    }

    private void setStopPointRef(StopPointRef stopPointRef) {
        this.stopPointRef = stopPointRef == null ? null : stopPointRef.getValue();
    }

    public void setAimedArrivalTime(ZonedDateTime aimedArrivalTime) {
        this.aimedArrivalTime = aimedArrivalTime;
    }

    public void setArrivalTime(ZonedDateTime arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public void setArrivalStatus(CallStatusEnumeration arrivalStatus) {
        this.arrivalStatus = arrivalStatus;
    }

    public void setAimedDepartureTime(ZonedDateTime aimedDepartureTime) {
        this.aimedDepartureTime = aimedDepartureTime;
    }

    public void setDepartureTime(ZonedDateTime departureTime) {
        this.departureTime = departureTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Call call = (Call) o;
        return extraCall == call.extraCall &&
                cancellation == call.cancellation &&
                estimated == call.estimated &&
                departureStatus == call.departureStatus &&
                Objects.equals(aimedArrivalTime, call.aimedArrivalTime) &&
                Objects.equals(arrivalTime, call.arrivalTime) &&
                arrivalStatus == call.arrivalStatus &&
                Objects.equals(aimedDepartureTime, call.aimedDepartureTime) &&
                Objects.equals(departureTime, call.departureTime) &&
                Objects.equals(stopPointRef, call.stopPointRef) &&
                Objects.equals(stopPointName, call.stopPointName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(extraCall, cancellation, estimated, departureStatus, aimedArrivalTime, arrivalTime, arrivalStatus, aimedDepartureTime, departureTime, stopPointRef, stopPointName);
    }
}
