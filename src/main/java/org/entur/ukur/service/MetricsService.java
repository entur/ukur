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

package org.entur.ukur.service;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import org.apache.camel.component.metrics.MetricsComponent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("WeakerAccess")
@Service
@Configuration
public class MetricsService {
    public static final String TIMER_PUSH                  = "timer.push.http";
    public static final String TIMER_ET_PROCESS            = "timer.process.EstimatedVehicleJourney";
    public static final String TIMER_SX_PROCESS            = "timer.process.PtSituationElement";
    public static final String TIMER_ET_UNMARSHALL         = "timer.unmarshall.EstimatedVehicleJourney";
    public static final String TIMER_SX_UNMARSHALL         = "timer.unmarshall.PtSituationElement";
    public static final String GAUGE_SUBSCRIPTIONS         = "gauge.subscriptions";
    public static final String GAUGE_STOPPLACES            = "gauge.stopPlaces";
    public static final String GAUGE_PUSH_QUEUE            = "gauge.pushQueue";
    public static final String METER_ET_IGNORED            = "message.et-ignored";
    public static final String METER_ET_WITHOUT_DEVIATIONS = "message.et-without-deviations";
    public static final String METER_ET_WITH_DEVIATIONS    = "message.et-with-deviations";
    public static final String HISTOGRAM_RECEIVED_DELAY    = "histogram.received_delay";
    public static final String HISTOGRAM_PROCESSED_DELAY   = "histogram.processed_delay";
    public static final String SUBSCRIPTION_ADD            = "subscription.add";
    public static final String SUBSCRIPTION_UPDATE         = "subscription.update";
    public static final String SUBSCRIPTION_DELETE         = "subscription.delete";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MetricRegistry metrics = new MetricRegistry();

    @Autowired
    PrometheusMetricsService prometheusMetricsService;

    @Autowired
    public MetricsService() {

    }

    /**
     * Bean factory method so we can use this metricregistry directly in camel routes.
     */
    @Bean(name = MetricsComponent.METRIC_REGISTRY_NAME)
    public MetricRegistry getMetricRegistry() {
        return metrics;
    }

    @SuppressWarnings("unused") //Used directly from Camel route
    public void registerSentMessage(String messagetype) {
        String counterName = "message.sent." + messagetype;
        metrics.meter(counterName).mark();
        prometheusMetricsService.registerOutboundData(messagetype, 1);
    }

    public void registerMessageDelay(String name, ZonedDateTime timestamp) {
        if (timestamp != null) {
            final Histogram delays = metrics.histogram(name, () -> new Histogram(getReservoir()));
            long delay = ChronoUnit.MILLIS.between(timestamp, ZonedDateTime.now());
            delays.update(delay);
        }
    }

    @SuppressWarnings("unused") //Used directly from Camel route
    public void registerMessageDelay(String name, String timestamp) {
        if (StringUtils.isNotBlank(timestamp)) {
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            registerMessageDelay(name, zonedDateTime);
        }
    }

    private SlidingTimeWindowArrayReservoir getReservoir() {
        return new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES);
    }

    public void registerReceivedMessage(Class messageClass) {
        String counterName = "message.received." + messageClass.getSimpleName();
        metrics.meter(counterName).mark();
        prometheusMetricsService.registerIncomingData(messageClass.getSimpleName(), 1);
    }

    public Timer getTimer(String name) {
        return metrics.timer(name, () -> new Timer(getReservoir()));
    }

    public void registerGauge(String name, Gauge<?> gauge) {
        metrics.register(name, gauge);
    }

    public SortedMap<String, Timer> getTimers() {
        return metrics.getTimers();
    }

    public SortedMap<String, Gauge> getGauges() {
        return metrics.getGauges();
    }

    public SortedMap<String, Meter> getMeters() {
        return metrics.getMeters();
    }

    public SortedMap<String, Histogram> getHistograms() {
        return metrics.getHistograms();
    }

    public Meter getMeter(String name) {
        return metrics.meter(name);
    }

    public void reset() {
        logger.warn("Resets all metrics!");
        metrics.removeMatching(MetricFilter.ALL);
    }
}
