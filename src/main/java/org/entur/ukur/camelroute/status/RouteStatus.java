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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.util.HashMap;

/**
 * Some route statistics for a single node.
 */
@SuppressWarnings("unused")
public class RouteStatus {

    private String hostname;
    private String nodeStartTime;
    private String statusHeartbeat;
    private String statusET;
    private String statusSX;
    private String statusSubscriptionRenewer;
    private String statusSubscriptionChecker;
    private HashMap<String, Object> gauges = new HashMap<>();
    private HashMap<String, Long> meterCounts = new HashMap<>();
    private HashMap<String, Double> meterOneMinuteRates = new HashMap<>();
    private HashMap<String, Long> timerCounts = new HashMap<>();
    private HashMap<String, Double> timerOneMinuteRates = new HashMap<>();
    private HashMap<String, Long> timerMax_ms = new HashMap<>();
    private HashMap<String, Long> timerMean_ms = new HashMap<>();
    private HashMap<String, Long> timer95thPersentile_ms = new HashMap<>();
    private HashMap<String, Long> histogramCounts = new HashMap<>();
    private HashMap<String, Long> histogramMax_ms = new HashMap<>();
    private HashMap<String, Double> histogramMean_ms = new HashMap<>();
    private HashMap<String, Double> histogram95thPersentile_ms = new HashMap<>();

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setNodeStartTime(String nodeStartTime) {
        this.nodeStartTime = nodeStartTime;
    }

    public String getNodeStartTime() {
        return nodeStartTime;
    }

    public HashMap<String, Long> getMeterCounts() {
        return meterCounts;
    }

    public HashMap<String, Double> getMeterOneMinuteRates() {
        return meterOneMinuteRates;
    }

    public HashMap<String, Long> getTimerCounts() {
        return timerCounts;
    }

    public HashMap<String, Double> getTimerOneMinuteRates() {
        return timerOneMinuteRates;
    }

    public HashMap<String, Object> getGauges() {
        return gauges;
    }

    public HashMap<String, Long> getTimerMax_ms() {
        return timerMax_ms;
    }

    public HashMap<String, Long> getTimerMean_ms() {
        return timerMean_ms;
    }

    public HashMap<String, Long> getTimer95thPersentile_ms() {
        return timer95thPersentile_ms;
    }

    public HashMap<String, Long> getHistogramCounts() {
        return histogramCounts;
    }

    public HashMap<String, Long> getHistogramMax_ms() {
        return histogramMax_ms;
    }

    public HashMap<String, Double> getHistogramMean_ms() {
        return histogramMean_ms;
    }

    public HashMap<String, Double> getHistogram95thPersentile_ms() {
        return histogram95thPersentile_ms;
    }

    public String getStatusHeartbeat() {
        return statusHeartbeat;
    }

    public void setStatusHeartbeat(String statusHeartbeat) {
        this.statusHeartbeat = statusHeartbeat;
    }

    public String getStatusET() {
        return statusET;
    }

    public void setStatusET(String statusETPolling) {
        this.statusET = statusETPolling;
    }

    public String getStatusSX() {
        return statusSX;
    }

    public void setStatusSX(String statusSXPolling) {
        this.statusSX = statusSXPolling;
    }

    public void addMeter(String name, Meter meter) {
        meterCounts.put(name, meter.getCount());
        meterOneMinuteRates.put(name, meter.getOneMinuteRate());
    }

    public void addGauge(String name, Gauge gauge) {
        gauges.put(name, gauge.getValue());
    }

    public void addTimer(String name, Timer timer) {
        Snapshot snapshot = timer.getSnapshot();
        timerCounts.put(name, timer.getCount());
        timerOneMinuteRates.put(name, timer.getOneMinuteRate());
        timerMax_ms.put(name, convertToMilliseconds(snapshot.getMax()));
        timerMean_ms.put(name, convertToMilliseconds(snapshot.getMean()));
        timer95thPersentile_ms.put(name, convertToMilliseconds(snapshot.get95thPercentile()));
    }

    public void addHistogram(String name, Histogram histogram) {
        Snapshot snapshot = histogram.getSnapshot();
        histogramCounts.put(name, histogram.getCount());
        histogramMax_ms.put(name, snapshot.getMax());
        histogramMean_ms.put(name, snapshot.getMean());
        histogram95thPersentile_ms.put(name, snapshot.get95thPercentile());
    }

    private long convertToMilliseconds(double nanos) {
        return convertToMilliseconds(Math.round(nanos));
    }

    private long convertToMilliseconds(long nanos) {
        return nanos / 1000000;
    }

    public void setStatusSubscriptionRenewer(String statusSubscriptionRenewer) {
        this.statusSubscriptionRenewer = statusSubscriptionRenewer;
    }

    public String getStatusSubscriptionRenewer() {
        return statusSubscriptionRenewer;
    }

    public void setStatusSubscriptionChecker(String statusSubscriptionChecker) {
        this.statusSubscriptionChecker = statusSubscriptionChecker;
    }

    public String getStatusSubscriptionChecker() {
        return statusSubscriptionChecker;
    }
}
