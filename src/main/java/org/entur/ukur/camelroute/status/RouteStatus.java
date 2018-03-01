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

package org.entur.ukur.camelroute.status;

import java.util.HashMap;

/**
 * Some route statistics for a single node.
 */
@SuppressWarnings("unused")
public class RouteStatus {

    private String hostname;
    private String nodeStartTime;
    private int numberOfSubscriptions;
    private long numberOfPushedMessages;
    private String statusJourneyFlush;
    private String statusETPolling;
    private String statusSXPolling;
    private HashMap<String, Long> meterCounts = new HashMap<>();
    private HashMap<String, Double> meterOneMinuteRates = new HashMap<>();
    private HashMap<String, Double> meterFiveMinuteRates = new HashMap<>();
    private HashMap<String, Double> meterFifteenMinuteRates = new HashMap<>();


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

    public void setNodeStartTime(String nodeStartTime) {
        this.nodeStartTime = nodeStartTime;
    }

    public String getNodeStartTime() {
        return nodeStartTime;
    }

    public void addMeterCount(String name, long count) {
        meterCounts.put(name, count);
    }

    public void addMeterOneMinuteRate(String name, double rate) {
        meterOneMinuteRates.put(name, rate);
    }

    public void addMeterFiveMinuteRate(String name, double rate) {
        meterFiveMinuteRates.put(name, rate);
    }

    public void addMeterFifteenMinuteRate(String name, double rate) {
        meterFifteenMinuteRates.put(name, rate);
    }

    public HashMap<String, Long> getMeterCounts() {
        return meterCounts;
    }

    public HashMap<String, Double> getMeterOneMinuteRates() {
        return meterOneMinuteRates;
    }

    public HashMap<String, Double> getMeterFiveMinuteRates() {
        return meterFiveMinuteRates;
    }

    public HashMap<String, Double> getMeterFifteenMinuteRates() {
        return meterFifteenMinuteRates;
    }

    public String getStatusJourneyFlush() {
        return statusJourneyFlush;
    }

    public void setStatusJourneyFlush(String statusJourneyFlush) {
        this.statusJourneyFlush = statusJourneyFlush;
    }

    public String getStatusETPolling() {
        return statusETPolling;
    }

    public void setStatusETPolling(String statusETPolling) {
        this.statusETPolling = statusETPolling;
    }

    public String getStatusSXPolling() {
        return statusSXPolling;
    }

    public void setStatusSXPolling(String statusSXPolling) {
        this.statusSXPolling = statusSXPolling;
    }

    public long getNumberOfPushedMessages() {
        return numberOfPushedMessages;
    }

    public void setNumberOfPushedMessages(long numberOfPushedMessages) {
        this.numberOfPushedMessages = numberOfPushedMessages;
    }
}
