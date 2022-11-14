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
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.hazelcast.map.IMap;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.App;
import org.entur.ukur.camelroute.testconfig.WiremockTestConfig;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.org.siri.siri20.EstimatedVehicleJourney;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
@AutoConfigureWireMock(port = 0)
@TestPropertySource("classpath:application-loadtest.properties")
@DirtiesContext
public class SubscribingLoadManualTest extends AbstractJUnit4SpringContextTests {


    private Logger logger = LoggerFactory.getLogger("TestControl");
    private static final NumberFormat FORMATTER = NumberFormat.getInstance(Locale.forLanguageTag("no"));

    @Autowired
    private MetricsService metricsService;

    @Autowired
    private WiremockTestConfig config;

    @Autowired @Qualifier("sharedProperties")
    private IMap<String, String> sharedProperties;

    @Autowired
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;

    @Autowired
    private SubscriptionManager subscriptionManager;

    private SiriMarshaller siriMarshaller = new SiriMarshaller();

    public SubscribingLoadManualTest() throws JAXBException {
    }

    @Before
    public void setUp() throws Exception {
        metricsService.reset();
        reset();
        waitUntilReceiverIsReady();
    }


    @Test
    @Ignore //so idea don't pick it up
    public void runETload() throws Exception {

        LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
        logCtx.getLogger("org.entur").setLevel(Level.INFO); //disables TRACE logging as we trace a lot during setup...

        assertEquals(0, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
        assertEquals(0, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_PUSH).getCount());

        populateQuayAndStopPlaceMappingService("https://api-test.entur.org/stop_places/1.0/list/stop_place_quays/");

        stubFor(post(urlMatching("/push.*/sx"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        stubFor(post(urlMatching("/push.*/et"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        Subscription osloAsker = createSubscription("/push1", "NSR:StopPlace:337", "NSR:StopPlace:418", null, "Oslo-Asker", null);
        logger.info("Created subscription from OsloS to Asker with id = {}", osloAsker.getId());

        Subscription codespaceBNR = createSubscription("/push2", null, null, null, "All delayed BNR", "BNR");
        logger.info("Created subscription for codespace BNR with id = {}", codespaceBNR.getId());

        Subscription lineL1 = createSubscription("/push3", null, null, "NSB:Line:R11", "Line R11", null);
        logger.info("Created subscription for line L1 with id = {}", lineL1.getId());

        Subscription codespaceRUT = createSubscription("/push4", null, null, "All delayed RUT", null, "RUT");
        logger.info("Created subscription for codespace RUT with id = {}", codespaceRUT.getId());

        List<Path> etMessages = Files.walk(Paths.get("/home/jon/Documents/Entur/sanntidsmelding_alle/raw"))
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().contains("ET"))
                .collect(Collectors.toList());
        long start = System.currentTimeMillis();
        for (int i = 0; i < etMessages.size(); i++) {
            long deltaStart = System.currentTimeMillis();
            postFile(etMessages.get(i), "et");
            logger.info("Sends message {}/{} in {} ms", (i + 1), etMessages.size(), formatTimeSince(deltaStart));
        }
        logger.info("DONE SENDING!");
        logger.info("Sends all {} messages in {} ms", etMessages.size(), formatTimeSince(start));

        //wait until all messages are processed
        Meter sentET = metricsService.getMeter("message.sent.EstimatedVehicleJourney");
        Meter receivedET = metricsService.getMeter("message.received.EstimatedVehicleJourney");
        while (sentET.getCount() > receivedET.getCount()) {
            logger.info("Still processing messages: sentET.getCount()={}, receivedET.getCount()={}", sentET.getCount(), receivedET.getCount());
            Thread.sleep(500);
        }

        long sleepStart = System.currentTimeMillis();
        int activePushThreads = subscriptionManager.getActivePushThreads();
        while (activePushThreads > 0) {
            if (System.currentTimeMillis() - sleepStart > 10_000) fail("Seems like the push-threads never will finish...");
            List<LoggedRequest> s1Requests = findAll(postRequestedFor(urlEqualTo("/push1/et")));
            List<LoggedRequest> s2Requests = findAll(postRequestedFor(urlEqualTo("/push2/et")));
            List<LoggedRequest> s3Requests = findAll(postRequestedFor(urlEqualTo("/push3/et")));
            List<LoggedRequest> s4Requests = findAll(postRequestedFor(urlEqualTo("/push4/et")));
            logger.info("There are still {} active threads working on pushing messages - sleeps before checking received messages. Received push-messages: s1={}, s2={}, s3={}, s4={}", activePushThreads, s1Requests.size(), s2Requests.size(), s3Requests.size(), s4Requests.size());
            Thread.sleep(1000);
            activePushThreads = subscriptionManager.getActivePushThreads();
        }
        logger.info("Finished processing {} ET messages and sent all pushmessages in {} ms", etMessages.size(), formatTimeSince(start));
        logger.info("Ukur pushed {} messages", metricsService.getTimer(MetricsService.TIMER_PUSH).getCount());
        Thread.sleep(1000); //to let all pushed messages arrive
        getEstimatedVehicleJourney("/push1/et");
        getEstimatedVehicleJourney("/push2/et");
        getEstimatedVehicleJourney("/push3/et");
        getEstimatedVehicleJourney("/push4/et");

    }

    @SuppressWarnings("UnusedReturnValue")
    private List<EstimatedVehicleJourney> getEstimatedVehicleJourney(String pushAddress) throws JAXBException, XMLStreamException {
        List<LoggedRequest> loggedRequests = findAll(postRequestedFor(urlEqualTo(pushAddress)));
        ArrayList<EstimatedVehicleJourney> result = new ArrayList<>(loggedRequests.size());
        for (LoggedRequest request : loggedRequests) {
            result.add(siriMarshaller.unmarshall(request.getBodyAsString(), EstimatedVehicleJourney.class));
        }
        HashSet<String> uniqueLineRefs = result.stream().map(e -> e.getLineRef().getValue()).collect(Collectors.toCollection(HashSet::new));
        HashSet<String> uniqueDatedVehicleJourneyRefs = result.stream().map(e -> e.getDatedVehicleJourneyRef().getValue()).collect(Collectors.toCollection(HashSet::new));
        logger.info("Received {} push-messages for push address {}: ",result.size(), pushAddress);
        logger.info("  with {} unique DatedVehicleJourneyRefs: {}",uniqueDatedVehicleJourneyRefs.size(), uniqueDatedVehicleJourneyRefs);
        logger.info("  with {} unique LineRefs: {}",uniqueLineRefs.size(), uniqueLineRefs);
        return result;
    }

    private Subscription createSubscription(String pushAddress, String from, String to, String line, String name, String codespace) throws Exception {
        Subscription subscription = new Subscription();
        if (from != null) subscription.addFromStopPoint(from);
        if (to != null) subscription.addToStopPoint(to);
        if (line != null) subscription.addLineRef(line);
        if (codespace != null) subscription.addCodespace(codespace);
        subscription.setName(name);
        subscription.setPushAddress("http://localhost:" + config.getWiremockPort() + pushAddress);
        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = mapper.writeValueAsString(subscription).getBytes();
        String postUrl = "http://localhost:"+config.getRestPort()+"/external/subscription";
        HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Content-Length", "" + bytes.length);
        connection.setDoOutput(true);
        connection.setDoInput(true);
        DataOutputStream out = new DataOutputStream(connection.getOutputStream());
        out.write(bytes);
        out.flush();
        out.close();
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        return mapper.readValue(response.toString(), Subscription.class);
    }

    @SuppressWarnings("SameParameterValue")
    private void postFile(Path file, String type) throws IOException {
        String requestorId = sharedProperties.get("AnsharRequestorId");
        assertNotNull(requestorId);
        String postUrl = "http://localhost:"+config.getRestPort()+"/internal/siriMessages/"+requestorId+"/"+type+"/";
        HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/xml");
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(file.toFile()));
        connection.setRequestProperty("Content-Length", "" + bytes.length);
        connection.setDoOutput(true);
        DataOutputStream out = new DataOutputStream(connection.getOutputStream());
        out.write(bytes);
        out.flush();
        out.close();
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode);
    }

    private void waitUntilReceiverIsReady() throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5_000) {
            String postUrl = "http://localhost:" + config.getRestPort() + "/internal/siriMessages/illegal-requestorId/illegal-type/";
            String body = "Will not be read";
            logger.debug("Check if server responds on uri: {}", postUrl);
            HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/xml");
            connection.setRequestProperty("Content-Length", "" + body.getBytes().length);
            connection.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            out.writeBytes(body);
            out.flush();
            out.close();
            int responseCode = connection.getResponseCode();
            logger.debug("... responseCode = {}", responseCode);
            if (responseCode == 403) {
                logger.info("The server responds FORBIDDEN on illegal input and is ready!");
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for receiver");
    }

    @SuppressWarnings("SameParameterValue")
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

    private String formatTimeSince(long start) {
        long time = System.currentTimeMillis() - start;
        return formatNumber(time);
    }

    private String formatNumber(long number) {
        return FORMATTER.format(number);
    }

}