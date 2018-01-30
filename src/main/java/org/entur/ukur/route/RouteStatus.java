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
