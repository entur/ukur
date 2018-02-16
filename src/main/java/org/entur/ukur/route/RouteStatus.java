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

/**
 * Some route statistics for a single node.
 */
@SuppressWarnings("unused")
public class RouteStatus {

    private String hostname;
    private String nodeStartTime;
    private int numberOfSubscriptions;
    private boolean isLeaderForJourneyFlush = false;
    private boolean isLeaderForETPolling = false;
    private boolean isLeaderForSXPolling = false;
    private SubscriptionStatus etSubscriptionStatus;
    private SubscriptionStatus sxSubscriptionStatus;

    public boolean isLeaderForETPolling() {
        return isLeaderForETPolling;
    }

    public void setLeaderForETPolling(boolean leaderForETPolling) {
        isLeaderForETPolling = leaderForETPolling;
    }

    public SubscriptionStatus getEtSubscriptionStatus() {
        return etSubscriptionStatus;
    }

    public void setEtSubscriptionStatus(SubscriptionStatus etSubscriptionStatus) {
        this.etSubscriptionStatus = etSubscriptionStatus;
    }

    public boolean isLeaderForSXPolling() {
        return isLeaderForSXPolling;
    }

    public void setLeaderForSXPolling(boolean leaderForSXPolling) {
        isLeaderForSXPolling = leaderForSXPolling;
    }

    public SubscriptionStatus getSxSubscriptionStatus() {
        return sxSubscriptionStatus;
    }

    public void setSxSubscriptionStatus(SubscriptionStatus sxSubscriptionStatus) {
        this.sxSubscriptionStatus = sxSubscriptionStatus;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getNumberOfSubscriptions() {
        return numberOfSubscriptions;
    }

    public void setNumberOfSubscriptions(int numberOfSubscriptions) {
        this.numberOfSubscriptions = numberOfSubscriptions;
    }

    public boolean isLeaderForJourneyFlush() {
        return isLeaderForJourneyFlush;
    }

    public void setLeaderForJourneyFlush(boolean leaderForJourneyFlush) {
        isLeaderForJourneyFlush = leaderForJourneyFlush;
    }

    public void setNodeStartTime(String nodeStartTime) {
        this.nodeStartTime = nodeStartTime;
    }

    public String getNodeStartTime() {
        return nodeStartTime;
    }
}
