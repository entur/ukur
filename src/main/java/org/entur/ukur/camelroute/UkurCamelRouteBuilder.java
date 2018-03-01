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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.IMap;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Predicate;
import org.apache.camel.Route;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.language.XPathExpression;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spring.SpringRouteBuilder;
import org.entur.ukur.camelroute.status.RouteStatus;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.camelroute.policy.InterruptibleHazelcastRoutePolicy;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.entur.ukur.camelroute.policy.SingletonRoutePolicyFactory.SINGLETON_ROUTE_DEFINITION_GROUP_NAME;

@Component
public class UkurCamelRouteBuilder extends SpringRouteBuilder {

    public static final String ROUTE_ET_RETRIEVER = "seda:retrieveAnsharET";
    public static final String ROUTE_SX_RETRIEVER = "seda:retrieveAnsharSX";
    private static final String ROUTE_FLUSHJOURNEYS = "seda:flushOldJourneys";
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String ROUTEID_SX_RETRIEVER = "SX Retriever";
    private static final String ROUTEID_ET_RETRIEVER = "ET Retriever";
    private static final String MORE_DATA = "MoreData";
    private static final String ROUTEID_ET_TRIGGER = "ET trigger";
    private static final String ROUTEID_SX_TRIGGER = "SX trigger";
    private static final String ROUTEID_FLUSHJOURNEYS_TRIGGER = "Flush Old Journeys";
    private UkurConfiguration config;
    private final NsbETSubscriptionProcessor nsbETSubscriptionProcessor;
    private NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor;
    private IMap<String, String> sharedProperties;
    private SubscriptionManager subscriptionManager;
    private MetricsService metricsService;
    private String nodeStarted;

    @Autowired
    public UkurCamelRouteBuilder(UkurConfiguration config,
                                 NsbETSubscriptionProcessor nsbETSubscriptionProcessor,
                                 NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor,
                                 IMap<String, String> sharedProperties,
                                 SubscriptionManager subscriptionManager,
                                 MetricsService metricsService) {
        this.config = config;
        this.nsbETSubscriptionProcessor = nsbETSubscriptionProcessor;
        this.nsbSXSubscriptionProcessor = nsbSXSubscriptionProcessor;
        this.sharedProperties = sharedProperties;
        this.subscriptionManager = subscriptionManager;
        this.metricsService = metricsService;
        nodeStarted = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void configure() {
        String proposedValue = "ukur-" + UUID.randomUUID();
        String requestorId = sharedProperties.putIfAbsent("AnsharRequestorId", proposedValue);
        requestorId = requestorId == null ? proposedValue : requestorId;
        logger.debug("Uses requestorId: '{}' - proposded value was {}", requestorId, proposedValue);
        String siriETurl = config.getAnsharETCamelUrl(requestorId);
        String siriSXurl = config.getAnsharSXCamelUrl(requestorId);

        createWorkerRoutes(siriETurl, siriSXurl);
        createRestRoutes(config.getRestPort(), config.isEtPollingEnabled(), config.isSxPollingEnabled());
        createQuartzRoutes(config.isEtPollingEnabled(), config.isSxPollingEnabled(), config.getPollingInterval());

    }

    private void createRestRoutes(int jettyPort, boolean etPollingEnabled, boolean sxPollingEnabled) {
        restConfiguration()
                .component("jetty")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .port(jettyPort);

        rest("/health")
                .get("/subscriptions").to("bean:subscriptionManager?method=listAll")
                .get("/routes").to("direct:routeStatus")
                .get("/live").to("direct:OK")
                .get("/ready").to("direct:OK");

        rest("/journeys")
                .get("/").to("bean:liveRouteManager?method=getJourneys()")
                .get("/{lineref}/").to("bean:liveRouteManager?method=getJourneys(${header.lineref})");

        rest("/subscription")
                .post().type(Subscription.class).outType(Subscription.class).to("bean:subscriptionManager?method=add(${body})")
                .delete("{id}").to("bean:subscriptionManager?method=remove(${header.id})");

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
                    status.setHostname(InetAddress.getLocalHost().getHostName());
                    Timer timer = metricsService.getTimer(MetricsService.TIMER_PUSH);
                    status.setNumberOfPushedMessages(timer.getCount());
                    status.setStatusJourneyFlush(routeStatus(ROUTEID_FLUSHJOURNEYS_TRIGGER, null));
                    status.setStatusETPolling(routeStatus(ROUTEID_ET_TRIGGER, etPollingEnabled));
                    status.setStatusSXPolling(routeStatus(ROUTEID_SX_TRIGGER, sxPollingEnabled));
                    status.setNumberOfSubscriptions(subscriptionManager.getNoSubscriptions());
                    for (String name : metricsService.getMeterNames()) {
                        Meter meter = metricsService.getMeter(name);
                        status.addMeterCount(name, meter.getCount());
                        status.addMeterOneMinuteRate(name, meter.getOneMinuteRate());
                        status.addMeterFiveMinuteRate(name, meter.getFiveMinuteRate());
                        status.addMeterFifteenMinuteRate(name, meter.getFifteenMinuteRate());
                    }
                    exchange.getOut().setBody(status);
                });
    }

    private void createQuartzRoutes(boolean etPollingEnabled, boolean sxPollingEnabled, int repatInterval) {

        if (etPollingEnabled) {
            singletonFrom("quartz2://ukur/pollAnsharET?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_ET_TRIGGER)
                    .filter(e -> isLeader(e.getFromRouteId()))
                    .filter(new NotRunningPredicate(ROUTEID_ET_RETRIEVER))
                    .log(LoggingLevel.DEBUG, "ET: Triggered by timer")
                    .to(ROUTE_ET_RETRIEVER);
        } else {
            logger.warn("ET polling is disabled");
        }

        if (sxPollingEnabled) {
            singletonFrom("quartz2://ukur/pollAnsharSX?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_SX_TRIGGER)
                    .filter(e -> isLeader(e.getFromRouteId()))
                    .filter(new NotRunningPredicate(ROUTEID_SX_RETRIEVER))
                    .log(LoggingLevel.DEBUG, "SX: Triggered by timer")
                    .to(ROUTE_SX_RETRIEVER);
        } else {
            logger.warn("SX polling is disabled");
        }

        singletonFrom("quartz2://ukur/flushOldJourneys?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_FLUSHJOURNEYS_TRIGGER)
                .filter(e -> isLeader(e.getFromRouteId()))
                .filter(new NotRunningPredicate(ROUTE_FLUSHJOURNEYS))
                .log(LoggingLevel.DEBUG, "'Flush old journeys' triggered by timer")
                .to(ROUTE_FLUSHJOURNEYS);

    }

    private void createWorkerRoutes(String siriETurl, String siriSXurl) {

        Predicate splitComplete = exchangeProperty(Exchange.SPLIT_COMPLETE).isEqualTo(true);
        Predicate moreData = exchangeProperty(MORE_DATA).isEqualToIgnoreCase("true");
        Predicate callAnsharAgain = PredicateBuilder.and(splitComplete, moreData);

        Namespaces ns = new Namespaces("s", "http://www.siri.org.uk/siri");
        XPathExpression moreDataExpression = ns.xpath("/s:Siri/s:ServiceDelivery/s:MoreData/text()", String.class);

        from(ROUTE_ET_RETRIEVER)
                .routeId(ROUTEID_ET_RETRIEVER)
                .to("metrics:timer:"+MetricsService.TIMER_ET_PULL+"?action=start")
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriETurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriETurl)
                .convertBodyTo(org.w3c.dom.Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .to("metrics:timer:"+MetricsService.TIMER_ET_PULL+"?action=stop")
                //TODO: this only selects elements with NSB as operator
                .split(ns.xpath("//s:EstimatedVehicleJourney[s:OperatorRef/text()='NSB']"))
                .bean(metricsService, "registerSentMessage('EstimatedVehicleJourney')")
                .to("activemq:queue:" + UkurConfiguration.ET_QUEUE)
                .choice()
                .when(callAnsharAgain)
                .log(LoggingLevel.DEBUG, "Call Anshar again since there are more ET data")
                .to(ROUTE_ET_RETRIEVER)
                .end();

        from("activemq:queue:" + UkurConfiguration.ET_QUEUE)
                .routeId("ET ActiveMQ Listener")
                .process(nsbETSubscriptionProcessor)
                .end();


        from(ROUTE_SX_RETRIEVER)
                .routeId(ROUTEID_SX_RETRIEVER)
                .to("metrics:timer:"+MetricsService.TIMER_SX_PULL+"?action=start")
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriSXurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriSXurl)
                .convertBodyTo(org.w3c.dom.Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .to("metrics:timer:"+MetricsService.TIMER_SX_PULL+"?action=stop")
                //TODO: this only selects elements with NSB as participant
                .split(ns.xpath("//s:PtSituationElement[s:ParticipantRef/text()='NSB']"))
                .bean(metricsService, "registerSentMessage('PtSituationElement')")
                .to("activemq:queue:" + UkurConfiguration.SX_QUEUE)
                .choice()
                .when(callAnsharAgain)
                .log(LoggingLevel.DEBUG, "Call Anshar again since there are more SX data")
                .to(ROUTE_SX_RETRIEVER)
                .end();

        from("activemq:queue:" + UkurConfiguration.SX_QUEUE)
                .routeId("SX ActiveMQ Listener")
                .process(nsbSXSubscriptionProcessor)
                .end();

        from(ROUTE_FLUSHJOURNEYS)
                .routeId("Flush Old Journeys Asynchronously")
                .to("bean:liveRouteManager?method=flushOldJourneys()");
    }

    private String routeStatus(String routeidSxTrigger, Boolean enabled) {
        String s = isLeader(routeidSxTrigger) ? "LEADER" : "NOT LEADER";
        if (Boolean.FALSE.equals(enabled)) {
            s += " (disabled)";
        }
        return s;
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
        Route route = getContext().getRoute(routeId);
        if (route != null) {
            RouteContext routeContext = route.getRouteContext();
            List<RoutePolicy> routePolicyList = routeContext.getRoutePolicyList();
            if (routePolicyList != null) {
                for (RoutePolicy routePolicy : routePolicyList) {
                    if (routePolicy instanceof InterruptibleHazelcastRoutePolicy) {
                        return ((InterruptibleHazelcastRoutePolicy) (routePolicy)).isLeader();
                    }
                }
            }
        }
        return false;
    }

    @Bean(name = "json-jackson")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public JacksonDataFormat jacksonDataFormat(ObjectMapper objectMapper) {
        return new JacksonDataFormat(objectMapper, HashMap.class);
    }
}