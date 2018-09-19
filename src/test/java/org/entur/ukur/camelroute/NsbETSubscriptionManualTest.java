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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.FileStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.testsupport.DatastoreTest;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.siri.siri20.EstimatedVehicleJourney;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"SameParameterValue", "Duplicates"})
public class NsbETSubscriptionManualTest extends DatastoreTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
    private SubscriptionManager subscriptionManager;
    private ETSubscriptionProcessor ETSubscriptionProcessor;
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;
    private SiriMarshaller siriMarshaller;
    private MetricsService metricsService = new MetricsService();
    private static final NumberFormat FORMATTER = NumberFormat.getInstance(Locale.forLanguageTag("no"));

    @Before
    public void setUp() throws Exception {
        super.setUp();
        HazelcastInstance hazelcastInstance = new TestHazelcastInstanceFactory().newHazelcastInstance();
        ITopic<String> subscriptionTopic = hazelcastInstance.getTopic("subscriptions");
        siriMarshaller = new SiriMarshaller();
        DataStorageService dataStorageService = new DataStorageService(datastore, subscriptionTopic);
        quayAndStopPlaceMappingService = new QuayAndStopPlaceMappingService(metricsService);
        subscriptionManager = new SubscriptionManager(dataStorageService, siriMarshaller, metricsService, new HashMap<>(), quayAndStopPlaceMappingService);
        ETSubscriptionProcessor = new ETSubscriptionProcessor(subscriptionManager, siriMarshaller, mock(FileStorageService.class), metricsService, quayAndStopPlaceMappingService);
        ETSubscriptionProcessor.skipCallTimeChecks = true; //since we post old recorded ET messages
    }

    @Test
    @Ignore //So idea don't run it as part of package/folder tests
    public void prosessRecordedETMessages() throws Exception {

        LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
        logCtx.getLogger("org.entur").setLevel(Level.INFO); //pauses TRACE logging as we trace a lot during setup...

        populateQuayAndStopPlaceMappingService("https://api-test.entur.org/stop_places/1.0/list/stop_place_quays/");

//        logCtx.getLogger("org.entur").setLevel(Level.DEBUG);

        stubFor(post(urlMatching("/subscription.*/et"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        String pushAddressBase = "http://localhost:" + wireMockRule.port();

        Subscription askerTilOslo = new Subscription();
        askerTilOslo.setName("Asker til OsloS");
        askerTilOslo.setPushAddress(pushAddressBase + "/subscription1");
        askerTilOslo.addFromStopPoint("NSR:StopPlace:418");
        askerTilOslo.addFromStopPoint("NSR:Quay:695");
        askerTilOslo.addFromStopPoint("NSR:Quay:696");
        askerTilOslo.addFromStopPoint("NSR:Quay:697");
        askerTilOslo.addFromStopPoint("NSR:Quay:698");
        askerTilOslo.addFromStopPoint("NSR:Quay:699");
        askerTilOslo.addFromStopPoint("NSR:Quay:700");
        askerTilOslo.addToStopPoint("NSR:StopPlace:337");
        askerTilOslo.addToStopPoint("NSR:Quay:550");
        askerTilOslo.addToStopPoint("NSR:Quay:551");
        askerTilOslo.addToStopPoint("NSR:Quay:553");
        askerTilOslo.addToStopPoint("NSR:Quay:554");
        askerTilOslo.addToStopPoint("NSR:Quay:555");
        askerTilOslo.addToStopPoint("NSR:Quay:556");
        askerTilOslo.addToStopPoint("NSR:Quay:563");
        askerTilOslo.addToStopPoint("NSR:Quay:557");
        askerTilOslo.addToStopPoint("NSR:Quay:559");
        askerTilOslo.addToStopPoint("NSR:Quay:561");
        askerTilOslo.addToStopPoint("NSR:Quay:562");
        askerTilOslo.addToStopPoint("NSR:Quay:564");
        askerTilOslo.addToStopPoint("NSR:Quay:566");
        askerTilOslo.addToStopPoint("NSR:Quay:567");
        askerTilOslo.addToStopPoint("NSR:Quay:568");
        askerTilOslo.addToStopPoint("NSR:Quay:569");
        askerTilOslo.addToStopPoint("NSR:Quay:565");
        askerTilOslo.addToStopPoint("NSR:Quay:570");
        askerTilOslo.addToStopPoint("NSR:Quay:571");
        subscriptionManager.addOrUpdate(askerTilOslo);

        Subscription askerOslo1 = new Subscription();
        askerOslo1.setName("Asker-OsloS #1 (kun stopplace)");
        askerOslo1.setPushAddress(pushAddressBase + "/subscription2");
        askerOslo1.addFromStopPoint("NSR:StopPlace:418");
        askerOslo1.addToStopPoint("NSR:StopPlace:337");
        subscriptionManager.addOrUpdate(askerOslo1);

        Subscription lineL13 = new Subscription();
        lineL13.setName("Line L13");
        lineL13.setPushAddress(pushAddressBase + "/subscription3");
        lineL13.addLineRef("NSB:Line:L13");
        subscriptionManager.addOrUpdate(lineL13);

        Subscription codespaceABC = new Subscription();
        codespaceABC.setName("Non-existing codespace");
        codespaceABC.setPushAddress(pushAddressBase + "/subscription4");
        codespaceABC.addCodespace("ABC");
        subscriptionManager.addOrUpdate(codespaceABC);

        assertEquals(4, subscriptionManager.listAll().size());

        List<Path> etMessages = Files.walk(Paths.get("/home/jon/Documents/Entur/sanntidsmelding_alle/raw"))
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().contains("ET"))
                .collect(Collectors.toList());
        long start = System.currentTimeMillis();
        for (int i = 0; i < etMessages.size(); i++) {
            long deltaStart = System.currentTimeMillis();
            ETSubscriptionProcessor.process(createExchangeMock(new FileInputStream(etMessages.get(i).toFile())));
            logger.info("Processed message {}/{} in {} ms", (i+1), etMessages.size(), formatTimeSince(deltaStart));
        }
        logger.info("DONE!");
        logger.info("Processed all {} messages in {} ms", etMessages.size(), formatTimeSince(start));

        long sleepStart = System.currentTimeMillis();
        int activePushThreads = subscriptionManager.getActivePushThreads();
        while (activePushThreads > 0) {
            if (System.currentTimeMillis() - sleepStart > 10_000) fail("Seems like the push-threads never will finish...");
            List<LoggedRequest> s1Requests = findAll(postRequestedFor(urlEqualTo("/subscription1/et")));
            List<LoggedRequest> s2Requests = findAll(postRequestedFor(urlEqualTo("/subscription2/et")));
            List<LoggedRequest> s3Requests = findAll(postRequestedFor(urlEqualTo("/subscription3/et")));
            List<LoggedRequest> s4Requests = findAll(postRequestedFor(urlEqualTo("/subscription4/et")));
            logger.info("There are still {} active threads working on pushing messages - sleeps before checking received messages. Received push-messages: subscription1={}, subscription2={}, subscription3={}, subscription4={}", activePushThreads, s1Requests.size(), s2Requests.size(), s3Requests.size(), s4Requests.size());
            Thread.sleep(1000);
            activePushThreads = subscriptionManager.getActivePushThreads();
        }

        logger.info("Finished processing {} ET messages and sent all pushmessages in {} ms", etMessages.size(), formatTimeSince(start));
        logger.info("There was {} messages with deviations, and {} without - {} was skipped",
                metricsService.getMeter(MetricsService.METER_ET_WITH_DEVIATIONS).getCount(),
                metricsService.getMeter(MetricsService.METER_ET_WITHOUT_DEVIATIONS).getCount(),
                metricsService.getMeter(MetricsService.METER_ET_IGNORED).getCount());

        List<EstimatedVehicleJourney> s1ReceivedMessages = getEstimatedVehicleJourney("/subscription1/et");
        List<EstimatedVehicleJourney> s2ReceivedMessages = getEstimatedVehicleJourney("/subscription2/et");
        List<EstimatedVehicleJourney> s3ReceivedMessages = getEstimatedVehicleJourney("/subscription3/et");
        logger.info("s1ReceivedMessages (Asker-OsloS stopplaces and quays) : {}", s1ReceivedMessages.size());
        logger.info("s2ReceivedMessages (Asker-OsloS only stopplaces)      : {}", s2ReceivedMessages.size());
        logger.info("s3ReceivedMessages (Line L13)                         : {}", s3ReceivedMessages.size());

        assertEquals("Excpected the same messages received", s1ReceivedMessages.size(), s2ReceivedMessages.size());
        HashSet<String> s1_uniqueDatedVehicleJourneyRefs = getUniqueDatedVehicleJourneyRefs(s1ReceivedMessages);
        HashSet<String> s2_uniqueDatedVehicleJourneyRefs = getUniqueDatedVehicleJourneyRefs(s2ReceivedMessages);
        assertTrue("Excpected the same messages received", s1_uniqueDatedVehicleJourneyRefs.containsAll(s2_uniqueDatedVehicleJourneyRefs));
        assertTrue("Excpected the same messages received", s2_uniqueDatedVehicleJourneyRefs.containsAll(s1_uniqueDatedVehicleJourneyRefs));

        assertFalse("Expected messages regarding line L13", s3ReceivedMessages.isEmpty());

        List<LoggedRequest> allUnmatchedRequests = wireMockRule.findAllUnmatchedRequests();
        if (!allUnmatchedRequests.isEmpty()) {
            for (LoggedRequest unmatchedRequest : allUnmatchedRequests) {
                logger.warn("Unmatched Request: {}", unmatchedRequest.getUrl());
            }
            fail("Did not expect any unmatched requests - got "+allUnmatchedRequests.size());
        }
    }

    private String formatTimeSince(long start) {
        long time = System.currentTimeMillis() - start;
        return formatNumber(time);
    }

    private String formatNumber(long number) {
        return FORMATTER.format(number);
    }

    private List<EstimatedVehicleJourney> getEstimatedVehicleJourney(String pushAddress) throws JAXBException, XMLStreamException {
        List<LoggedRequest> loggedRequests = findAll(postRequestedFor(urlEqualTo(pushAddress)));
        ArrayList<EstimatedVehicleJourney> result = new ArrayList<>(loggedRequests.size());
        for (LoggedRequest request : loggedRequests) {
            result.add(siriMarshaller.unmarshall(request.getBodyAsString(), EstimatedVehicleJourney.class));
        }
        HashSet<String> uniqueLineRefs = result.stream().map(e -> e.getLineRef().getValue()).collect(Collectors.toCollection(HashSet::new));
        HashSet<String> uniqueDatedVehicleJourneyRefs = getUniqueDatedVehicleJourneyRefs(result);
        logger.info("Received {} push-messages for push address {}: ",result.size(), pushAddress);
        logger.info("  with {} unique DatedVehicleJourneyRefs: {}",uniqueDatedVehicleJourneyRefs.size(), uniqueDatedVehicleJourneyRefs);
        logger.info("  with {} unique LineRefs: {}",uniqueLineRefs.size(), uniqueLineRefs);
        return result;
    }

    private HashSet<String> getUniqueDatedVehicleJourneyRefs(List<EstimatedVehicleJourney> result) {
        return result.stream().map(e -> e.getDatedVehicleJourneyRef().getValue()).collect(Collectors.toCollection(HashSet::new));
    }

    private Exchange createExchangeMock(InputStream stream) {
        Exchange exchangeMock = mock(Exchange.class);
        Message messageMock = mock(Message.class);
        when(exchangeMock.getIn()).thenReturn(messageMock);
        when(messageMock.getBody(InputStream.class)).thenReturn(stream);
        return exchangeMock;
    }

    private void populateQuayAndStopPlaceMappingService(String url) {
        try {
            logger.info("About to get list of stops and quays from Tiamat");
            long start = System.currentTimeMillis();
            URLConnection connection = new URL(url).openConnection();
            InputStream json = connection.getInputStream();
            ObjectMapper mapper = new ObjectMapper();
            //noinspection unchecked
            HashMap<String, Collection<String>> stopsFromTiamat = mapper.readValue(json, HashMap.class);
            logger.info("Got {} stop places from Tiamat", stopsFromTiamat.size());
            quayAndStopPlaceMappingService.updateStopsAndQuaysMap(stopsFromTiamat);
            logger.info("It took {} ms to download and handle {} stop places ", formatTimeSince(start), stopsFromTiamat.size());
        } catch (IOException e) {
            logger.error("Could not get list of stopplaces and quays", e);
        }

    }

}