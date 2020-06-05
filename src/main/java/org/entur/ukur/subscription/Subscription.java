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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import javax.xml.datatype.Duration;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Subscription implements Serializable {

    private static final String SIRI_NAME_PREFIX = "SIRI-XML";
    private String id;
    private String name;
    private String pushAddress;
    private URL pushAddressURL;
    private Boolean pushAllData = false;
    private HashSet<String> fromStopPoints = new HashSet<>();
    private HashSet<String> toStopPoints = new HashSet<>();
    private HashSet<String> lineRefs = new HashSet<>();
    private HashSet<String> codespaces = new HashSet<>();
    private SubscriptionTypeEnum type = SubscriptionTypeEnum.ALL;
    private Boolean useSiriSubscriptionModel;
    @JsonIgnore
    private long failedPushCounter = 0;
    @JsonIgnore
    private ZonedDateTime firstErrorSeen = null;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private ZonedDateTime initialTerminationTime;
    private Duration heartbeatInterval;
    private Duration minimumDelay;
    private DeviationType deviationType = DeviationType.ALL;

    static String getName(String requestorRef, String subscriptionIdentifier) {
        return SIRI_NAME_PREFIX+"-REF("+requestorRef+")-ID("+subscriptionIdentifier+")";
    }

    boolean isSiriXMLBasedSubscription() {
        return StringUtils.startsWith(name, SIRI_NAME_PREFIX);
    }

    String getSiriRequestor() {
        return StringUtils.trimToNull(StringUtils.substringBetween(name, SIRI_NAME_PREFIX + "-REF(", ")-ID("));
    }

    String getSiriClientGeneratedId() {
        String clientId = StringUtils.substringAfter(name, ")-ID(");
        return StringUtils.trimToNull(StringUtils.substringBeforeLast(clientId, ")"));
    }

    public Set<String> getFromStopPoints() {
        return Collections.unmodifiableSet(fromStopPoints);
    }

    public void setFromStopPlaces(Collection<String> fromStopPoints) {
        this.fromStopPoints.clear();
        this.fromStopPoints.addAll(fromStopPoints);
    }

    public void addFromStopPoint(String stopPointRef) {
        fromStopPoints.add(stopPointRef);
    }

    public void removeFromStopPoint(String stopPointRef) {
        fromStopPoints.remove(stopPointRef);
    }

    public Set<String> getToStopPoints() {
        return Collections.unmodifiableSet(toStopPoints);
    }

    public void setToStopPlaces(Collection<String> toStopPoints) {
        this.toStopPoints.clear();
        this.toStopPoints.addAll(toStopPoints);
    }

    public void addToStopPoint(String stopPointRef) {
        toStopPoints.add(stopPointRef);
    }

    public void removeToStopPoint(String stopPointRef) {
        toStopPoints.remove(stopPointRef);
    }

    public Set<String> getLineRefs() {
        return Collections.unmodifiableSet(lineRefs);
    }

    public void setLineRefs(Collection<String> lineRefs) {
        this.lineRefs.clear();
        this.lineRefs.addAll(lineRefs);
    }

    public void addLineRef(String lineref) {
        lineRefs.add(lineref);
    }

    public void addCodespace(String ref) {
        codespaces.add(ref);
    }

    public Set<String> getCodespaces() {
        return Collections.unmodifiableSet(codespaces);
    }

    public void setCodespaces(Collection<String> codespaces) {
        this.codespaces.clear();
        this.codespaces.addAll(codespaces);
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

    public String getPushHost() {
        return pushAddressURL != null ? pushAddressURL.getHost() : null;
    }

    public void setPushAddress(String pushAddress) {
        this.pushAddress = pushAddress;
        try {
            this.pushAddressURL = new URL(pushAddress);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public SubscriptionTypeEnum getType() {
        if (type == null) {
            type = SubscriptionTypeEnum.ALL;
        }
        return type;
    }

    public void setType(SubscriptionTypeEnum type) {
        if (type == null) {
            type = SubscriptionTypeEnum.ALL;
        }
        this.type = type;
    }

    public DeviationType getDeviationType() {
        if (deviationType == null) {
            deviationType = DeviationType.ALL;
        }
        return deviationType;
    }

    public void setDeviationType(DeviationType deviationType) {
        if (deviationType == null) {
            deviationType = DeviationType.ALL;
        }
        this.deviationType = deviationType;
    }

    public boolean isPushAllData() {
        if (pushAllData == null) return false;
        return pushAllData;
    }

    public void setPushAllData(boolean pushAllData) {
        this.pushAllData = pushAllData;
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

    @Override
    public String toString() {
        return "Subscription{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    void resetFailedPushCounter() {
        failedPushCounter = 0;
        firstErrorSeen = null;
    }

    public boolean shouldRemove() {
        if (failedPushCounter == 0 || firstErrorSeen == null) {
            firstErrorSeen = ZonedDateTime.now();
        }
        failedPushCounter++;
        return failedPushCounter > 3 && ZonedDateTime.now().minusMinutes(10).isAfter(firstErrorSeen);
    }

    public long getFailedPushCounter() {
        return failedPushCounter;
    }

    public ZonedDateTime getFirstErrorSeen() {
        return firstErrorSeen;
    }

    public void setFirstErrorSeen(ZonedDateTime firstErrorSeen) {
        this.firstErrorSeen = firstErrorSeen;
    }

    void normalizeAndRemoveIgnoredStops() {
        fromStopPoints = normalizeAndRemoveIgnoredStops(fromStopPoints);
        toStopPoints = normalizeAndRemoveIgnoredStops(toStopPoints);
    }

    private HashSet<String> normalizeAndRemoveIgnoredStops(HashSet<String> set) {
        HashSet<String> result = new HashSet<>();
        for (String aSet : set) {
            String next = StringUtils.trimToEmpty(aSet);
            if (next.startsWith("NSR:")) {
                result.add(next);
            }
        }
        return result;
    }

    public void setFailedPushCounter(long failedPushCounter) {
        this.failedPushCounter = failedPushCounter;
    }

    public boolean hasNoStops() {
        return fromStopPoints.isEmpty() && toStopPoints.isEmpty();
    }

    public boolean isUseSiriSubscriptionModel() {
        return Boolean.TRUE.equals(useSiriSubscriptionModel);
    }

    public void setUseSiriSubscriptionModel(Boolean useSiriSubscriptionModel) {
        this.useSiriSubscriptionModel = useSiriSubscriptionModel;
    }

    public void setInitialTerminationTime(ZonedDateTime initialTerminationTime) {
        this.initialTerminationTime = initialTerminationTime;
    }

    public ZonedDateTime getInitialTerminationTime() {
        return initialTerminationTime;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public Duration getMinimumDelay() {
        return minimumDelay;
    }

    public void setMinimumDelay(Duration minimumDelay) {
        this.minimumDelay = minimumDelay;
    }
}
