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

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.camel.component.metrics.MetricsComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricAttribute.*;

@Service
@Configuration
public class MetricsService {
    public static final String TIMER_PUSH          = "timer.push.http";
    public static final String TIMER_ET_PULL       = "timer.pull.anshar-et";
    public static final String TIMER_SX_PULL       = "timer.pull.anshar-sx";
    public static final String TIMER_TIAMAT        = "timer.tiamat.StopPlacesAndQuays";
    public static final String TIMER_ET_PROCESS    = "timer.process.EstimatedVehicleJourney";
    public static final String TIMER_SX_PROCESS    = "timer.process.PtSituationElement";
    public static final String TIMER_ET_UNMARSHALL = "timer.unmarshall.EstimatedVehicleJourney";
    public static final String TIMER_SX_UNMARSHALL = "timer.unmarshall.PtSituationElement";
    public static final String GAUGE_SUBSCRIPTIONS = "gauge.subscriptions";
    public static final String GAUGE_LIVE_JOURNEYS = "gauge.liveJourneys";
    public static final String GAUGE_STOPPLACES    = "gauge.stopPlaces";
    public static final String GAUGE_PUSH_QUEUE    = "gauge.pushQueue";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MetricRegistry metrics = new MetricRegistry();
    private final boolean graphiteEnabled;
    private GraphiteReporter reporter;
    private Graphite graphite;

    /**
     * Bean factory method so we can use this metricregistry directly in camel routes.
     */
    @Bean(name = MetricsComponent.METRIC_REGISTRY_NAME)
    public MetricRegistry getMetricRegistry() {
        return metrics;
    }

    @Autowired
    public MetricsService(@Value("${ukur.graphite.host:}") String graphiteHost,
                          @Value("${ukur.graphite.port:2003}") int graphitePort) {

        if (Strings.isNullOrEmpty(graphiteHost) ) {
            graphiteEnabled = false;
            logger.info("Setting up local metrics service");
        } else {
            graphiteEnabled = true;
            String hostName;
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                logger.warn("Could not get hostname ", e);
                hostName = UUID.randomUUID().toString();
            }
            String prefix = "app.ukur." + hostName;
            logger.info("Setting up metrics reporter with graphite server: host={}, port={}, prefix={}", graphiteHost, graphitePort, prefix);
            graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));
            reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith(prefix)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    //each of the metric attributes below results in a 3.1mb database per metric in graphite - disables the one we don't need:
                    .disabledMetricAttributes(Sets.newHashSet(STDDEV, P50, P75, P95, P98, P99, P999, M5_RATE, M15_RATE, MEAN_RATE))
                    .build(graphite);
        }
    }

    @PostConstruct
    public void startGraphiteReporter() {
        if (graphiteEnabled) {
            logger.info("Starting graphite reporter");
            reporter.start(10, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void shutdownGraphiteReporter() throws IOException {
        if (graphiteEnabled) {
            reporter.stop();
            if (graphite.isConnected()) {
                graphite.flush();
                graphite.close();
            }
        }
    }

    @SuppressWarnings("unused") //Used directly from Camel route
    public void registerReceivedSubscribedMessage(String requestorId, String type) {
        String counterName = "message.subs-received." + requestorId +"."+type;
        metrics.meter(counterName).mark();
    }

    @SuppressWarnings("unused") //Used directly from Camel route
    public void registerSentMessage(String messagetype) {
        String counterName = "message.sent." + messagetype;
        metrics.meter(counterName).mark();
    }

    public void registerReceivedMessage(Class messageClass) {
        String counterName = "message.received." + messageClass.getSimpleName();
        metrics.meter(counterName).mark();
    }

    public Timer getTimer(String name) {
        return metrics.timer(name, () -> new Timer(new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)));
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

    public Meter getMeter(String meterName) {
        return metrics.meter(meterName);
    }
}
