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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

public class Subscription implements Serializable {

    private String id;
    private String name;
    private String pushAddress;
    private HashSet<String> fromStopPoints = new HashSet<>();
    private HashSet<String> toStopPoints = new HashSet<>();
    @JsonIgnore
    private int failedPushCounter = 0;

    //TODO: varslingsdetaljer, gyldighet (fra-til, ukedag), "holdbarhet på subscriptionen"

    public Set<String> getFromStopPoints() {
        return Collections.unmodifiableSet(fromStopPoints);
    }

    public void addFromStopPoint(String stopPointRef) {
        fromStopPoints.add(stopPointRef);
    }

    public boolean removeFromStopPoint(String stopPointRef) {
        return fromStopPoints.remove(stopPointRef);
    }

    public Set<String> getToStopPoints() {
        return Collections.unmodifiableSet(toStopPoints);
    }

    public void addToStopPoint(String stopPointRef) {
        toStopPoints.add(stopPointRef);
    }

    public boolean removeToStopPoint(String stopPointRef) {
        return toStopPoints.remove(stopPointRef);
    }

    public String getName() {
        if (name == null) return Integer.toString(hashCode());
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPushAddress() {
        return pushAddress;
    }

    public void setPushAddress(String pushAddress) {
        this.pushAddress = pushAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public void resetFailedPushCounter() {
        failedPushCounter = 0;
    }

    public int increaseFailedPushCounter() {
        failedPushCounter++;
        return failedPushCounter;
    }

    public int getFailedPushCounter() {
        return failedPushCounter;
    }

    public void normalizeAndRemoveIgnoredStops() {
        fromStopPoints = normalizeAndRemoveIgnoredStops(fromStopPoints);
        toStopPoints = normalizeAndRemoveIgnoredStops(toStopPoints);
    }

    private HashSet<String> normalizeAndRemoveIgnoredStops(HashSet<String> set) {
        HashSet<String> result = new HashSet<>();
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            String next = StringUtils.trimToEmpty(iterator.next());
            if (next.startsWith("NSR:")) {
                result.add(next);
            }
        }
        return result;
    }
}
