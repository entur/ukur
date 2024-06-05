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

package org.entur.ukur.camelroute;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.support.builder.Namespaces;
import org.apache.http.entity.ContentType;
import org.entur.avro.realtime.siri.converter.avro2jaxb.Avro2JaxbConverter;
import org.entur.avro.realtime.siri.helper.JsonReader;
import org.entur.ukur.camelroute.policy.InterruptibleHazelcastRoutePolicy;
import org.entur.ukur.camelroute.status.RouteStatus;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.PrometheusMetricsService;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.xml.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import uk.org.siri.siri21.EstimatedVehicleJourney;
import uk.org.siri.siri21.PtSituationElement;
import uk.org.siri.siri21.Siri;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.entur.ukur.camelroute.policy.SingletonRoutePolicyFactory.SINGLETON_ROUTE_DEFINITION_GROUP_NAME;

@Component
@Configuration
public class UkurCamelRouteBuilder extends RouteBuilder {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static final String ROUTE_HEARTBEAT_CHECKER = "seda:heartbeatChecker";
  private static final String ROUTEID_HEARTBEAT_CHECKER  = "Check Subscriptions For Missing Heartbeats";
  private static final String ROUTEID_HEARTBEAT_TRIGGER  = "Check Subscriptions Trigger";

  static final String ROUTE_ET_RETRIEVER = "seda:retrieveAnsharET";
            static final String ROUTE_SX_RETRIEVER = "seda:retrieveAnsharSX";

    private final UkurConfiguration config;
    private final ETSubscriptionProcessor ETSubscriptionProcessor;
    private final SXSubscriptionProcessor SXSubscriptionProcessor;
    private final MetricsService metricsService;

    private final PrometheusMetricsService prometheusMeterRegistry;

    private final String nodeStarted;
    private final StopPlaceQuaysProcessor stopPlaceQuaysProcessor;
    private final Namespaces siriNamespace = new Namespaces("s", "http://www.siri.org.uk/siri");
    private final KryoSerializer kryoSerializer = new KryoSerializer();

    @Autowired
    public UkurCamelRouteBuilder(UkurConfiguration config,
                                 ETSubscriptionProcessor ETSubscriptionProcessor,
                                 SXSubscriptionProcessor SXSubscriptionProcessor,
                                 StopPlaceQuaysProcessor stopPlaceQuaysProcessor,
                                 MetricsService metricsService, PrometheusMetricsService prometheusMeterRegistry) {
        this.config = config;
        this.ETSubscriptionProcessor = ETSubscriptionProcessor;
        this.SXSubscriptionProcessor = SXSubscriptionProcessor;
        this.stopPlaceQuaysProcessor = stopPlaceQuaysProcessor;
        this.metricsService = metricsService;
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        nodeStarted = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        siriNamespace.add("ns2", "http://www.ifopt.org.uk/acsb");
    }

    @Override
    public void configure() {
        onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.WARN, "Caught ${exception}")
                .transform().simple("${exception.message}");

        onException(InvalidSubscriptionIdException.class)
                .handled(true)
                // use HTTP status 400 when input data is invalid
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setBody(constant(""));

        createJaxbProcessingRoutes();
        createWorkerRoutes(config.getStopPlaceQuaysURL(), config.getStopPlaceQuaysUpdateIntervalMillis());
        createRestRoutes(config.getRestPort());
        createQuartzRoutes(config.getHeartbeatCheckInterval());

    }

    private void createQuartzRoutes(int subscriptionCheckerRepatInterval) {

      createSingletonQuartzRoute(
          "subscriptionHeartbeatAndTermination",
          subscriptionCheckerRepatInterval,
          ROUTEID_HEARTBEAT_TRIGGER,
          ROUTEID_HEARTBEAT_CHECKER,
          ROUTE_HEARTBEAT_CHECKER
      );

    }

    private void createJaxbProcessingRoutes() {

        from("direct:map.avro.et.to.jaxb")
                .process(p -> {
                    p.getOut().setBody(JsonReader.readEstimatedVehicleJourney(p.getIn().getBody(String.class)));
                    p.getOut().setHeaders(p.getIn().getHeaders());
                })
                .bean(Avro2JaxbConverter.class, "convert")
                .process(p -> {
                    final EstimatedVehicleJourney body = p.getIn().getBody(EstimatedVehicleJourney.class);
                    p.getOut().setBody(body);
                    p.getOut().setHeaders(p.getIn().getHeaders());
                })
        ;

        from("direct:map.avro.sx.to.jaxb")
                .process(p -> {
                    p.getOut().setBody(
                            JsonReader.readPtSituationElement(p.getIn().getBody(String.class))
                    );
                    p.getOut().setHeaders(p.getIn().getHeaders());
                })
                .bean(Avro2JaxbConverter.class, "convert")
                .process(p -> {
                    final PtSituationElement body = p.getIn().getBody(PtSituationElement.class);
                    p.getOut().setBody(body);
                    p.getOut().setHeaders(p.getIn().getHeaders());
                })
        ;
    }

    private void createWorkerRoutes(String stopPlaceQuaysURL, long stopPlaceUpdaterRepeatInterval) {

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(() -> {
                    try {
                        stopPlaceQuaysProcessor.readFileFromInputStream(new URL(stopPlaceQuaysURL).openStream());
                        logger.info("StopPlaceQuays initialized from {}", stopPlaceQuaysURL);
                    } catch (IOException e) {
                        logger.info("Initializing StopPlaceQuays failed - fallback to camelroutes", e);
                    }
        }, 0,
        stopPlaceUpdaterRepeatInterval,
        TimeUnit.MILLISECONDS);


        from(config.getEtPubsubQueue())
                .routeId("ET pubsub Listener")
                .log(LoggingLevel.DEBUG, "About to handle ET message from queue")
                .to("direct:map.avro.et.to.jaxb")
                .process(ETSubscriptionProcessor)
                .log(LoggingLevel.DEBUG, "Done handling ET message from queue")
                .end();

        from(config.getSxPubsubQueue())
                .routeId("SX pubsub Listener")
                .log(LoggingLevel.DEBUG, "About to handle SX message from queue")
                .to("direct:map.avro.sx.to.jaxb")
                .process(SXSubscriptionProcessor)
                .log(LoggingLevel.DEBUG, "Done handling SX message from queue")
                .end();

      from(ROUTE_HEARTBEAT_CHECKER)
                .routeId(ROUTEID_HEARTBEAT_CHECKER)
                .to("bean:subscriptionManager?method=handleHeartbeatAndTermination()");

    }

    private void createSingletonQuartzRoute(String timerName, int repeatInterval, String triggerRouteId, String toRouteId, String toRoute) {
        String uri = "quartz://ukur/" + timerName + "?trigger.repeatInterval=" + repeatInterval;
        singletonFrom(uri, triggerRouteId)
            .filter(e -> isLeader(e.getFromRouteId()))
            .filter(e -> isNotRunning(toRouteId))
            .log(LoggingLevel.DEBUG, timerName + " triggered by timer")
            .to(toRoute);
    }

    private void createRestRoutes(int jettyPort) {
        restConfiguration()
                .component("jetty")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .port(jettyPort)
                .contextPath("/")
                .apiContextPath("/external/swagger.json")
                .apiProperty("api.title", "Ukur API")
                .apiProperty("api.description", "Ukur offers subscriptions to deviations in traffic. This API doc describes " +
                        "both external and internal api's, but only the external are reachable from Internet (without external " +
                        "as part of the url).")
                .apiProperty("api.version", "v1.0.1")
                .apiProperty("cors", "true");

        rest("/internal/health")
                .bindingMode(RestBindingMode.json)
                .get("/subscriptions").to("bean:subscriptionManager?method=listAll")
                .get("/subscriptions/reload").to("bean:subscriptionManager?method=reloadSubscriptionCache")
                .get("/routes").to("direct:routeStatus")
                .get("/live").to("direct:OK")
                .get("/ready").to("direct:ready")
                .get("/scrape").to("direct:scrape");

        rest("/external/subscription")
                .bindingMode(RestBindingMode.json)
                .post().type(Subscription.class).outType(Subscription.class).to("bean:subscriptionManager?method=addOrUpdate(${body})")
                .delete("{id}").to("bean:subscriptionManager?method=remove(${header.id})");

        rest("/external/siri-subscription")
                .bindingMode(RestBindingMode.xml)
                .post().type(Siri.class).outType(Siri.class).to("bean:siriXMLSubscriptionHandler?method=handle(${body}, null)")
                .post("{codespace}").type(Siri.class).outType(Siri.class).to("bean:siriXMLSubscriptionHandler?method=handle(${body}, ${header.codespace})");

        from("direct:scrape")
                .routeId("Prometheus scrape")
                .bean(prometheusMeterRegistry, "scrape()")
                .setHeader(Exchange.CONTENT_TYPE, constant(ContentType.TEXT_PLAIN))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"));

        from("direct:ready")
                .routeId("Ready checker")
                .choice()
                .when(exchange -> stopPlaceQuaysProcessor.hasRun()).to("direct:OK")
                .otherwise()
                .log(LoggingLevel.WARN, "not ready (has not retrieved stopplace data yet)")
                .setBody(simple("NOT OK    \n\n"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("500"));

        from("direct:OK")
                .routeId("OK response")
                .log(LoggingLevel.TRACE, "Return hardcoded 'OK' on uri '${header." + Exchange.HTTP_URI + "}'")
                .setBody(simple("OK    \n\n"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"));

        from("direct:routeStatus")
                .routeId("Route Status")
                .process(exchange -> {
                    RouteStatus status = new RouteStatus();
                    status.setNodeStartTime(nodeStarted);
                    status.setHostname(getHostName());
                    for (Map.Entry<String, Meter> entry : metricsService.getMeters().entrySet()) {
                        status.addMeter(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Timer> entry : metricsService.getTimers().entrySet()) {
                        status.addTimer(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Gauge> entry : metricsService.getGauges().entrySet()) {
                        status.addGauge(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Histogram> entry : metricsService.getHistograms().entrySet()) {
                        status.addHistogram(entry.getKey(), entry.getValue());
                    }
                    exchange.getOut().setBody(status);
                });

    }

    private String getHostName(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "Ukur-UnknownHost";
        }
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }

    @Bean(name = "json-jackson")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public JacksonDataFormat jacksonDataFormat(ObjectMapper objectMapper) {
        return new JacksonDataFormat(objectMapper, HashMap.class);
    }

    private boolean isNotRunning(String routeId) {
        int size = getContext().getInflightRepository().size(routeId);
        boolean notRunning = size == 0;
        logger.trace("Number of running instances of camelroute '{}' is {} - returns {}", routeId, size, notRunning);
        return notRunning;
    }
    private String routeStatus(String triggerRoute) {
        return isLeader(triggerRoute) ? "LEADER" : "NOT LEADER";
    }

    /**
    * Create a new singleton camelroute definition from URI. Only one such camelroute should be active throughout the cluster at any time.
    */
    private RouteDefinition singletonFrom(String uri, String routeId) {
        return this.from(uri)
            .group(SINGLETON_ROUTE_DEFINITION_GROUP_NAME)
            .routeId(routeId)
            .autoStartup(true);
    }


    private boolean isLeader(String routeId) {
        List<RoutePolicy> routePolicyList =getContext().getRoute(routeId).getRoutePolicyList();
        if (routePolicyList != null) {
            for (RoutePolicy routePolicy : routePolicyList) {
                if (routePolicy instanceof InterruptibleHazelcastRoutePolicy) {
                    return ((InterruptibleHazelcastRoutePolicy) (routePolicy)).isLeader();
                }
            }
        }
        return false;
    }

}