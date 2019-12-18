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
import com.hazelcast.core.IMap;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Message;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.entur.ukur.camelroute.policy.InterruptibleHazelcastRoutePolicy;
import org.entur.ukur.camelroute.status.RouteStatus;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import uk.org.siri.siri20.EstimatedTimetableRequestStructure;
import uk.org.siri.siri20.EstimatedTimetableSubscriptionStructure;
import uk.org.siri.siri20.MessageQualifierStructure;
import uk.org.siri.siri20.RequestorRef;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.SituationExchangeRequestStructure;
import uk.org.siri.siri20.SituationExchangeSubscriptionStructure;
import uk.org.siri.siri20.SubscriptionContextStructure;
import uk.org.siri.siri20.SubscriptionQualifierStructure;
import uk.org.siri.siri20.SubscriptionRequest;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.entur.ukur.camelroute.policy.SingletonRoutePolicyFactory.SINGLETON_ROUTE_DEFINITION_GROUP_NAME;
import static org.entur.ukur.subscription.SiriXMLSubscriptionHandler.SIRI_VERSION;

@Component
public class UkurCamelRouteBuilder extends SpringRouteBuilder {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
            static final String ROUTE_ET_RETRIEVER = "seda:retrieveAnsharET";
            static final String ROUTE_SX_RETRIEVER = "seda:retrieveAnsharSX";
    private static final String ROUTE_HEARTBEAT_CHECKER = "seda:heartbeatChecker";
    private static final String ROUTE_TIAMAT_MAP = "seda:getStopPlacesAndQuays";
    private static final String ROUTE_ANSHAR_SUBSRENEWER = "seda:ansharSubscriptionRenewer";
    private static final String ROUTE_ANSHAR_SUBSCHECKER = "seda:ansharSubscriptionChecker";
    private static final String ROUTEID_SX_RETRIEVER = "SX Retriever";
    private static final String ROUTEID_ET_RETRIEVER = "ET Retriever";
    private static final String ROUTEID_TIAMAT_MAP = "Tiamat StopPlacesAndQuays";
    private static final String ROUTEID_HEARTBEAT_CHECKER  = "Check Subscriptions For Missing Heartbeats";
    private static final String ROUTEID_ANSHAR_SUBSRENEWER = "Anshar Subscription Renewer";
    private static final String ROUTEID_ANSHAR_SUBSCHECKER = "Anshar Subscription Checker";
    private static final String ROUTEID_HEARTBEAT_TRIGGER  = "Check Subscriptions Trigger";
    private static final String ROUTEID_ET_TRIGGER = "ET trigger";
    private static final String ROUTEID_SX_TRIGGER = "SX trigger";
    private static final String ROUTEID_TIAMAT_MAP_TRIGGER = "Tiamat trigger";
    private static final String ROUTEID_ANSHAR_SUBSRENEWER_TRIGGER = "Anshar Subscription Renewer Trigger";
    private static final String ROUTEID_ANSHAR_SUBSCHECKER_TRIGGER = "Anshar Subscription Checker Trigger";

    private static final String MORE_DATA = "MoreData";
    private final UkurConfiguration config;
    private final ETSubscriptionProcessor ETSubscriptionProcessor;
    private final SXSubscriptionProcessor SXSubscriptionProcessor;
    private final IMap<String, String> sharedProperties;
    private final MetricsService metricsService;
    private final PrometheusMeterRegistry prometheusMeterRegistry;
    private final String nodeStarted;
    private final TiamatStopPlaceQuaysProcessor tiamatStopPlaceQuaysProcessor;
    private final Namespaces siriNamespace = new Namespaces("s", "http://www.siri.org.uk/siri");
    private final int HEARTBEAT_INTERVAL_MS = 60_000;
    private final int SUBSCRIPTION_DURATION_MIN = 60;

    @Autowired
    public UkurCamelRouteBuilder(UkurConfiguration config,
                                 ETSubscriptionProcessor ETSubscriptionProcessor,
                                 SXSubscriptionProcessor SXSubscriptionProcessor,
                                 TiamatStopPlaceQuaysProcessor tiamatStopPlaceQuaysProcessor,
                                 @Qualifier("sharedProperties") IMap<String, String> sharedProperties,
                                 MetricsService metricsService, PrometheusMeterRegistry prometheusMeterRegistry) {
        this.config = config;
        this.ETSubscriptionProcessor = ETSubscriptionProcessor;
        this.SXSubscriptionProcessor = SXSubscriptionProcessor;
        this.tiamatStopPlaceQuaysProcessor = tiamatStopPlaceQuaysProcessor;
        this.sharedProperties = sharedProperties;
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

        createWorkerRoutes(config.getTiamatStopPlaceQuaysURL());
        createRestRoutes(config.getRestPort(), config.isEtEnabled(), config.isSxEnabled(), config.useAnsharSubscription());
        createQuartzRoutes(config.getHeartbeatCheckInterval(), config.isTiamatStopPlaceQuaysEnabled(), config.getTiamatStopPlaceQuaysInterval());
        createSiriProcessingRoutes();

        String proposedValue = "ukur-" + UUID.randomUUID();
        String requestorId = sharedProperties.putIfAbsent("AnsharRequestorId", proposedValue);
        requestorId = (requestorId == null) ? proposedValue : requestorId;
        logger.debug("Uses requestorId: '{}' - proposed value was {}", requestorId, proposedValue);
        if (config.useAnsharSubscription()) {
            logger.info("Configures camel routes for subscribing to Anshar");
            configureAnsharSubscriptionRoutes(config.isEtEnabled(), config.isSxEnabled(), config.isSubscriptionCheckingEnabled(),  requestorId);
        } else {
            logger.info("Configures camel routes for polling Anshar");
            String siriETurl = config.getAnsharETCamelUrl(requestorId);
            String siriSXurl = config.getAnsharSXCamelUrl(requestorId);
            createAnsharPollingRoutes(config.isEtEnabled(), config.isSxEnabled(), config.getPollingInterval(), siriETurl, siriSXurl);
        }
    }

    private void createWorkerRoutes(String tiamatStopPlaceQuaysURL) {

        from("activemq:queue:" + UkurConfiguration.ET_QUEUE)
                .routeId("ET ActiveMQ Listener")
                .log(LoggingLevel.DEBUG, "About to handle ET message from queue")
                .process(ETSubscriptionProcessor)
                .log(LoggingLevel.DEBUG, "Done handling ET message from queue")
                .end();

        from("activemq:queue:" + UkurConfiguration.SX_QUEUE)
                .routeId("SX ActiveMQ Listener")
                .log(LoggingLevel.DEBUG, "About to handle SX message from queue")
                .process(SXSubscriptionProcessor)
                .log(LoggingLevel.DEBUG, "Done handling SX message from queue")
                .end();

        from(ROUTE_HEARTBEAT_CHECKER)
                .routeId(ROUTEID_HEARTBEAT_CHECKER)
                .to("bean:subscriptionManager?method=handleHeartbeatAndTermination()");

        from(ROUTE_TIAMAT_MAP)
                .routeId(ROUTEID_TIAMAT_MAP)
                .to("metrics:timer:" + MetricsService.TIMER_TIAMAT + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Tiamat with url: " + tiamatStopPlaceQuaysURL)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .setHeader("ET-Client-Name", constant("Ukur"))
                .setHeader("ET-Client-ID", constant(getHostName()))
                .to(tiamatStopPlaceQuaysURL)
                .process(tiamatStopPlaceQuaysProcessor)
                .to("metrics:timer:" + MetricsService.TIMER_TIAMAT + "?action=stop")
                .end();

        //If messages time out, they end up on the dead letter queue - this routes removes them and logs error
        from("activemq:queue:" + UkurConfiguration.ET_DLQ)
                .routeId("ET DLQ ActiveMQ Listener")
                .log(LoggingLevel.ERROR, "Received and removed a message from the ET DLQ")
                .to("metrics:meter:"+MetricsService.METER_ET_DLQ)
                .end();
        from("activemq:queue:" + UkurConfiguration.SX_DLQ)
                .routeId("SX DLQ ActiveMQ Listener")
                .log(LoggingLevel.ERROR, "Received and removed a message from the SX DLQ")
                .to("metrics:meter:"+MetricsService.METER_SX_DLQ)
                .end();
        //this registers the dlq meters so we can see they have value=0 before anything ends up on the DLQ (which it really shouldn't...)
        metricsService.getMeter(MetricsService.METER_ET_DLQ);
        metricsService.getMeter(MetricsService.METER_SX_DLQ);
    }

    private void createRestRoutes(int jettyPort, boolean etEnabled, boolean sxEnabled, boolean createSubscriptionReceievers) {
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
                .to("bean:prometheusMeterRegistry?method=scrape")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"))
                .setHeader(Exchange.CONTENT_TYPE, constant(ContentType.APPLICATION_JSON));

        from("direct:ready")
                .routeId("Ready checker")
                .choice()
                .when(exchange -> tiamatStopPlaceQuaysProcessor.hasRun()).to("direct:OK")
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
                    status.setStatusHeartbeat(routeStatus(ROUTEID_HEARTBEAT_TRIGGER));
                    status.setStatusET(routeStatus(ROUTEID_ET_TRIGGER, etEnabled, createSubscriptionReceievers));
                    status.setStatusSX(routeStatus(ROUTEID_SX_TRIGGER, sxEnabled, createSubscriptionReceievers));
                    status.setStatusSubscriptionRenewer(routeStatus(ROUTEID_ANSHAR_SUBSRENEWER_TRIGGER));
                    status.setStatusSubscriptionChecker(routeStatus(ROUTEID_ANSHAR_SUBSCHECKER_TRIGGER));
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

    private void createQuartzRoutes(int subscriptionCheckerRepatInterval, boolean stopPlaceToQuayEnabled, int tiamatRepatInterval) {

        createSingletonQuartz2Route("subscriptionHeartbeatAndTermination", subscriptionCheckerRepatInterval, ROUTEID_HEARTBEAT_TRIGGER, ROUTEID_HEARTBEAT_CHECKER, ROUTE_HEARTBEAT_CHECKER);

        if (stopPlaceToQuayEnabled) {
            from("quartz2://ukur/getStopPlacesFromTiamat?trigger.repeatInterval=" + tiamatRepatInterval + "&fireNow=true")
                    .routeId(ROUTEID_TIAMAT_MAP_TRIGGER)
                    .filter(e -> isNotRunning(ROUTEID_TIAMAT_MAP))
                    .log(LoggingLevel.DEBUG, "getStopPlacesFromTiamat triggered by timer")
                    .to(ROUTE_TIAMAT_MAP);
        }
    }

    private void createSiriProcessingRoutes() {
        NamespaceContext nsContext = new NamespaceContext() {
            @Override
            public String getNamespaceURI(String prefix) {
                if ("s" .equals(prefix)) return "http://www.siri.org.uk/siri";
                return null;
            }

            @Override
            public String getPrefix(String namespaceURI) {
                return null;
            }

            @Override
            public Iterator getPrefixes(String namespaceURI) {
                return null;
            }
        };

        XPathExpression responseTimestampExpression = siriNamespace.xpath("/s:Siri/s:ServiceDelivery/s:ResponseTimestamp/text()", String.class);

        from("direct:processPtSituationElements")
                .routeId("processPtSituationElements")
                .process(exchange -> {
                    if (logger.isDebugEnabled()) {
                        Message in = exchange.getIn();
                        Document xmlDocument = in.getBody(Document.class);
                        XPath xPath = XPathFactory.newInstance().newXPath();
                        xPath.setNamespaceContext(nsContext);
                        Double total = (Double) xPath.compile("count(//s:PtSituationElement)").evaluate(xmlDocument, XPathConstants.NUMBER);
                        logger.debug("Received XML with {} PtSituationElements", Math.round(total));
                    }
                })
                .setHeader("responseTimestamp", responseTimestampExpression)
                .bean(metricsService, "registerMessageDelay("+MetricsService.HISTOGRAM_RECEIVED_DELAY +", ${header.responseTimestamp} )")
                .to("xslt:xsl/prepareSiriSplit.xsl")
                .split(siriNamespace.xpath("//s:Siri"))
                .bean(metricsService, "registerSentMessage('PtSituationElement')")
                .to("activemq:queue:" + UkurConfiguration.SX_QUEUE);

        from("direct:processEstimatedVehicleJourneys")
                .routeId("processEstimatedVehicleJourneys")
                .process(exchange -> {
                    if (logger.isDebugEnabled()) {
                        Message in = exchange.getIn();
                        Document xmlDocument = in.getBody(Document.class);
                        XPath xPath = XPathFactory.newInstance().newXPath();
                        xPath.setNamespaceContext(nsContext);
                        Double total = (Double) xPath.compile("count(//s:EstimatedVehicleJourney)").evaluate(xmlDocument, XPathConstants.NUMBER);
                        logger.debug("Received XML with {} EstimatedVehicleJourneys", Math.round(total));
                    }
                })
                .setHeader("responseTimestamp", responseTimestampExpression)
                .bean(metricsService, "registerMessageDelay("+MetricsService.HISTOGRAM_RECEIVED_DELAY +", ${header.responseTimestamp} )")
                .to("xslt:xsl/prepareSiriSplit.xsl")
                .split(siriNamespace.xpath("//s:Siri"))
                .bean(metricsService, "registerSentMessage('EstimatedVehicleJourney')")
                .to("activemq:queue:" + UkurConfiguration.ET_QUEUE);
    }

    private void configureAnsharSubscriptionRoutes(boolean etEnabled, boolean sxEnabled, boolean createSubscription, String requestorId) {

        rest("/internal/siriMessages")
                .apiDocs(false)
                .consumes("application/xml")
                .bindingMode(RestBindingMode.off)
                .post("/{requestorId}/{type}")
                .to("direct:checkRequestorId");

        from("direct:checkRequestorId")
                .routeId("Check requestorId")
                .choice()
                .when(header("requestorId").isNotEqualTo(requestorId))
                    .log(LoggingLevel.WARN, "Received unknown requestorId ('${header.requestorId}')")
                    .to("direct:FORBIDDEN")
                    .endChoice()
                .when(PredicateBuilder.and(exchange -> sxEnabled, header("type").isEqualTo("sx")))
                    .bean(metricsService, "registerReceivedSubscribedMessage(${header.type} )")
                    .wireTap("direct:receivePtSituationElements")
                    .setBody(simple("OK\n\n"))
                    .endChoice()
                .when(PredicateBuilder.and(exchange -> etEnabled, header("type").isEqualTo("et")))
                    .bean(metricsService, "registerReceivedSubscribedMessage(${header.type} )")
                    .wireTap("direct:receiveEstimatedVehicleJourneys")
                    .setBody(simple("OK\n\n"))
                    .endChoice()
                .otherwise()
                    .log(LoggingLevel.WARN, "Unhandled type ('${header.type}') - sxEnabled="+sxEnabled+", etEnabled="+etEnabled)
                    .to("direct:FORBIDDEN")
                    .endChoice()
                .end();

        from("direct:FORBIDDEN")
                .routeId("FORBIDDEN response")
                .log(LoggingLevel.INFO, "Return 'FORBIDDEN' on uri '${header." + Exchange.HTTP_URI + "}'")
                .setBody(simple("FORBIDDEN    \n\n"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("403"));

        Processor heartbeatRegistrer = exchange -> {
            String type = exchange.getIn().getHeader("type", String.class);
            String key = "AnsharLastReceived-" + type;
            String value = Long.toString(System.currentTimeMillis());
            logger.trace("register received notification of type '{}' with key '{}' and value '{}'", type, key, value);
            sharedProperties.set(key, value);
        };

        from("direct:receivePtSituationElements")
                .routeId("Handle subscribed SX message")
                .convertBodyTo(Document.class)
                .process(heartbeatRegistrer)
                .to("direct:processPtSituationElements");

        from("direct:receiveEstimatedVehicleJourneys")
                .routeId("Handle subscribed ET message")
                .convertBodyTo(Document.class)
                .process(heartbeatRegistrer)
                .to("direct:processEstimatedVehicleJourneys");

        if (createSubscription) {
            //renew subscriptions:
            createSingletonQuartz2Route("AnsharSubscriptionRenewer", SUBSCRIPTION_DURATION_MIN * 60_000, ROUTEID_ANSHAR_SUBSRENEWER_TRIGGER, ROUTEID_ANSHAR_SUBSRENEWER, ROUTE_ANSHAR_SUBSRENEWER);
            //re-create subscriptions if nothing is received from Anshar for some time (3 x heartbeat):
            createSingletonQuartz2Route("AnsharSubscriptionChecker", HEARTBEAT_INTERVAL_MS, ROUTEID_ANSHAR_SUBSCHECKER_TRIGGER, ROUTEID_ANSHAR_SUBSCHECKER, ROUTE_ANSHAR_SUBSCHECKER);
        }

        from(ROUTE_ANSHAR_SUBSRENEWER)
                .routeId(ROUTEID_ANSHAR_SUBSRENEWER)
                .process(exchange -> {
                    if (etEnabled) {
                        SubscriptionRequest request = createSubscriptionRequest(requestorId, "et");
                        EstimatedTimetableRequestStructure etRequest = new EstimatedTimetableRequestStructure();
                        etRequest.setRequestTimestamp(ZonedDateTime.now());
                        etRequest.setVersion(SIRI_VERSION);
                        etRequest.setMessageIdentifier(request.getMessageIdentifier());
                        EstimatedTimetableSubscriptionStructure etSubscriptionReq = new EstimatedTimetableSubscriptionStructure();
                        etSubscriptionReq.setEstimatedTimetableRequest(etRequest);
                        SubscriptionQualifierStructure subscriptionIdentifier = new SubscriptionQualifierStructure();
                        subscriptionIdentifier.setValue(requestorId + "-ET");
                        etSubscriptionReq.setSubscriptionIdentifier(subscriptionIdentifier);
                        etSubscriptionReq.setInitialTerminationTime(ZonedDateTime.now().plusMinutes(SUBSCRIPTION_DURATION_MIN));
                        etSubscriptionReq.setSubscriberRef(request.getRequestorRef());
                        request.getEstimatedTimetableSubscriptionRequests().add(etSubscriptionReq);
                        logger.info("Sets up subscription for ET messages with duration of {} minutes", SUBSCRIPTION_DURATION_MIN);
                        postSubscriptionRequest(request);
                    }

                    if (sxEnabled) {
                        SubscriptionRequest request = createSubscriptionRequest(requestorId, "sx");
                        SituationExchangeRequestStructure sxRequest = new SituationExchangeRequestStructure();
                        sxRequest.setRequestTimestamp(ZonedDateTime.now());
                        sxRequest.setVersion(SIRI_VERSION);
                        sxRequest.setMessageIdentifier(request.getMessageIdentifier());
                        SituationExchangeSubscriptionStructure sxSubscriptionReq = new SituationExchangeSubscriptionStructure();
                        sxSubscriptionReq.setSituationExchangeRequest(sxRequest);
                        SubscriptionQualifierStructure subscriptionIdentifier = new SubscriptionQualifierStructure();
                        subscriptionIdentifier.setValue(requestorId + "-SX");
                        sxSubscriptionReq.setSubscriptionIdentifier(subscriptionIdentifier);
                        sxSubscriptionReq.setInitialTerminationTime(ZonedDateTime.now().plusMinutes(SUBSCRIPTION_DURATION_MIN));
                        sxSubscriptionReq.setSubscriberRef(request.getRequestorRef());
                        request.getSituationExchangeSubscriptionRequests().add(sxSubscriptionReq);
                        logger.info("Sets up subscription for SX messages with duration of {} minutes", SUBSCRIPTION_DURATION_MIN);
                        postSubscriptionRequest(request);
                    }
                });

        from(ROUTE_ANSHAR_SUBSCHECKER)
                .routeId(ROUTEID_ANSHAR_SUBSCHECKER)
                .filter(exchange -> {
                    String lastReceivedET = sharedProperties.get("AnsharLastReceived-et");
                    String lastReceivedSX = sharedProperties.get("AnsharLastReceived-sx");
                    long current = System.currentTimeMillis();
                    if (etEnabled && StringUtils.isNotBlank(lastReceivedET)) {
                        long last = Long.parseLong(lastReceivedET);
                        logger.trace("Last ET message was received {} ms ago", last);
                        if (current - last > (3 * HEARTBEAT_INTERVAL_MS)) {
                            logger.info("Renews subscription as the last ET message was received {} ms ago", last);
                            return true;
                        }
                    }
                    if (sxEnabled && StringUtils.isNotBlank(lastReceivedSX)) {
                        long last = Long.parseLong(lastReceivedSX);
                        logger.trace("Last SX message was received {} ms ago", last);
                        if (current - last > (3 * HEARTBEAT_INTERVAL_MS)) {
                            logger.info("Renews subscription as the last SX message was received {} ms ago", last);
                            return true;
                        }
                    }

                    return false;
                })
                .to(ROUTE_ANSHAR_SUBSRENEWER);

    }

    private void createAnsharPollingRoutes(boolean etPollingEnabled, boolean sxPollingEnabled, int repatInterval, String siriETurl, String siriSXurl) {

        Predicate splitComplete = exchangeProperty(Exchange.SPLIT_COMPLETE).isEqualTo(true);
        Predicate moreData = exchangeProperty(MORE_DATA).isEqualToIgnoreCase("true");
        Predicate callAnsharAgain = PredicateBuilder.and(splitComplete, moreData);

        XPathExpression moreDataExpression = siriNamespace.xpath("/s:Siri/s:ServiceDelivery/s:MoreData/text()", String.class);

        from(ROUTE_ET_RETRIEVER)
                .routeId(ROUTEID_ET_RETRIEVER)
                .to("metrics:timer:" + MetricsService.TIMER_ET_PULL + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriETurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .setHeader("ET-Client-Name", constant("Ukur"))
                .setHeader("ET-Client-ID", constant(getHostName()))
                .to(siriETurl)
                .convertBodyTo(Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .to("metrics:timer:" + MetricsService.TIMER_ET_PULL + "?action=stop")
                .to("direct:processEstimatedVehicleJourneys")
                .choice()
                .when(callAnsharAgain)
                .log(LoggingLevel.DEBUG, "Call Anshar again since there are more ET data")
                .to(ROUTE_ET_RETRIEVER)
                .end();

        from(ROUTE_SX_RETRIEVER)
                .routeId(ROUTEID_SX_RETRIEVER)
                .to("metrics:timer:" + MetricsService.TIMER_SX_PULL + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriSXurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .setHeader("ET-Client-Name", constant("Ukur"))
                .setHeader("ET-Client-ID", constant(getHostName()))
                .to(siriSXurl)
                .convertBodyTo(Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .to("metrics:timer:" + MetricsService.TIMER_SX_PULL + "?action=stop")
                .to("direct:processPtSituationElements")
                .choice()
                .when(callAnsharAgain)
                .log(LoggingLevel.DEBUG, "Call Anshar again since there are more SX data")
                .to(ROUTE_SX_RETRIEVER)
                .end();

        if (etPollingEnabled) {
            createSingletonQuartz2Route("pollAnsharET", repatInterval, ROUTEID_ET_TRIGGER, ROUTEID_ET_RETRIEVER, ROUTE_ET_RETRIEVER);
        } else {
            logger.warn("ET polling is disabled");
        }

        if (sxPollingEnabled) {
            createSingletonQuartz2Route("pollAnsharSX", repatInterval, ROUTEID_SX_TRIGGER, ROUTEID_SX_RETRIEVER, ROUTE_SX_RETRIEVER);
        } else {
            logger.warn("SX polling is disabled");
        }

    }

    private void postSubscriptionRequest(SubscriptionRequest request) {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        siri.setSubscriptionRequest(request);
        try {
            SiriMarshaller siriMarshaller = new SiriMarshaller();
            HttpURLConnection connection = (HttpURLConnection) new URL(config.getAnsharSubscriptionUrl()).openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            out.writeBytes(siriMarshaller.marshall(siri));
            out.flush();
            out.close();
            int responseCode = connection.getResponseCode();
            if (200 == responseCode) {
                logger.info("Successfully created subscription to Anshar!");
            } else {
                logger.error("Unexpected response code from Anshar when subscribing: {}", responseCode);
            }
        } catch (Exception e) {
            logger.error("Could not subscribe to Anshar", e);
        }
    }

    private SubscriptionRequest createSubscriptionRequest(String requestorId, String type) {
        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue("Ukur");
        DatatypeFactory datatypeFactory;
        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException(e);
        }
        MessageQualifierStructure messageIdentifier = new MessageQualifierStructure();
        messageIdentifier.setValue("required-by-siri-spec-"+System.currentTimeMillis());
        SubscriptionRequest request = new SubscriptionRequest();
        request.setRequestorRef(requestorRef);
        request.setMessageIdentifier(messageIdentifier);
        request.setAddress(config.getOwnSubscriptionURL()+"siriMessages/"+requestorId+"/"+type);
        request.setRequestTimestamp(ZonedDateTime.now());
        SubscriptionContextStructure ctx = new SubscriptionContextStructure();
        ctx.setHeartbeatInterval(datatypeFactory.newDuration(HEARTBEAT_INTERVAL_MS));
        request.setSubscriptionContext(ctx);
        return request;
    }

    private void createSingletonQuartz2Route(String timerName, int repatInterval, String triggerRouteId, String toRouteId, String toRoute) {
        String uri = "quartz2://ukur/" + timerName + "?trigger.repeatInterval=" + repatInterval + "&startDelayedSeconds=5&fireNow=true";
        singletonFrom(uri, triggerRouteId)
                .filter(e -> isLeader(e.getFromRouteId()))
                .filter(e -> isNotRunning(toRouteId))
                .log(LoggingLevel.DEBUG, timerName + " triggered by timer")
                .to(toRoute);
    }

    private String getHostName(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "Ukur-UnknownHost";
        }
    }

    private String routeStatus(String triggerRoute, boolean enabled, boolean subscriptionBased) {
        String s;
        if (subscriptionBased) {
            s = "Subscribing";
        } else {
            s = "Polling " + routeStatus(triggerRoute);
        }
        if (!enabled) {
            s += " (disabled)";
        }
        return s;
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

    private boolean isNotRunning(String routeId) {
        int size = getContext().getInflightRepository().size(routeId);
        boolean notRunning = size == 0;
        logger.trace("Number of running instances of camelroute '{}' is {} - returns {}", routeId, size, notRunning);
        return notRunning;
    }

    @Bean(name = "json-jackson")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public JacksonDataFormat jacksonDataFormat(ObjectMapper objectMapper) {
        return new JacksonDataFormat(objectMapper, HashMap.class);
    }

}