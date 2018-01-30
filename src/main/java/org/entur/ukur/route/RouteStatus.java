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

    private boolean isLeaderForETPolling = false;
    private SubscriptionStatus etSusbcriptionStatus;
    private boolean isLeaderForSXPolling = false;
    private SubscriptionStatus sxSusbcriptionStatus;

    public boolean isLeaderForETPolling() {
        return isLeaderForETPolling;
    }

    public void setLeaderForETPolling(boolean leaderForETPolling) {
        isLeaderForETPolling = leaderForETPolling;
    }

    public SubscriptionStatus getEtSusbcriptionStatus() {
        return etSusbcriptionStatus;
    }

    public void setEtSusbcriptionStatus(SubscriptionStatus etSusbcriptionStatus) {
        this.etSusbcriptionStatus = etSusbcriptionStatus;
    }

    public boolean isLeaderForSXPolling() {
        return isLeaderForSXPolling;
    }

    public void setLeaderForSXPolling(boolean leaderForSXPolling) {
        isLeaderForSXPolling = leaderForSXPolling;
    }

    public SubscriptionStatus getSxSusbcriptionStatus() {
        return sxSusbcriptionStatus;
    }

    public void setSxSusbcriptionStatus(SubscriptionStatus sxSusbcriptionStatus) {
        this.sxSusbcriptionStatus = sxSusbcriptionStatus;
    }
}
