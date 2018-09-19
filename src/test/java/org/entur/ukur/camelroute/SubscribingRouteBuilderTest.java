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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.Sets;
import com.hazelcast.core.IMap;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.App;
import org.entur.ukur.camelroute.testconfig.WiremockTestConfig;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Before;
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
import uk.org.siri.siri20.PtSituationElement;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
@AutoConfigureWireMock(port = 0)
@TestPropertySource("classpath:application-subscribing.properties")
@DirtiesContext
public class SubscribingRouteBuilderTest extends AbstractJUnit4SpringContextTests {


    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private MetricsService metricsService;

    @Autowired
    private WiremockTestConfig config;

    @Autowired @Qualifier("sharedProperties")
    private IMap<String, String> sharedProperties;

    @Autowired
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;

    private SiriMarshaller siriMarshaller = new SiriMarshaller();

    public SubscribingRouteBuilderTest() throws JAXBException {
    }

    @Before
    public void setUp() throws Exception {
        metricsService.reset();
        reset();
        waitUntilReceiverIsReady();
    }

    @Test
    public void testETreceive() throws Exception {
        assertEquals(0, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(0, metricsService.getMeter("message.subs-received.et").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());

        postFile("/et-pretty.xml", "et");
        waitUntil(MetricsService.TIMER_ET_PROCESS, 10);

        assertEquals(1, metricsService.getMeter("message.subs-received.et").getCount());
        assertEquals(10, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(10, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());
    }

    @Test
    public void testSXreceive() throws Exception {
        assertEquals(0, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
        assertEquals(0, metricsService.getMeter("message.subs-received.sx").getCount());

        postFile("/sx-pretty.xml", "sx");
        waitUntil(MetricsService.TIMER_SX_PROCESS, 9);

        assertEquals(1, metricsService.getMeter("message.subs-received.sx").getCount());
        assertEquals(9, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(9, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
    }

    @Test
    public void testPushOnSXFromJourneyLookup() throws Exception {
        assertEquals(0, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
        assertEquals(0, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_PUSH).getCount());

        logger.info("Adds quays and stopplace maps");
        HashMap<String, Collection<String>> stopPlacesAndQuays = new HashMap<>();
        stopPlacesAndQuays.put("NSR:StopPlace:337", Sets.newHashSet("NSR:Quay:553"));
        stopPlacesAndQuays.put("NSR:StopPlace:418", Sets.newHashSet("NSR:Quay:696"));
        quayAndStopPlaceMappingService.updateStopsAndQuaysMap(stopPlacesAndQuays);

        stubFor(post(urlMatching("/push.*/sx"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        String osloAskerUrl = "/push1/sx";
        Subscription osloAsker = createSubscription(osloAskerUrl, "NSR:StopPlace:337", "NSR:StopPlace:418", null, "Oslo-Asker");
        logger.info("TestControl: Created subscription from OsloS to Asker with id = {}", osloAsker.getId());

        String askerOsloUrl = "/push2/sx";
        Subscription askerOslo = createSubscription(askerOsloUrl, "NSR:StopPlace:418", "NSR:StopPlace:337", null, "Asker-Oslo");
        logger.info("TestControl: Created subscription from Asker to OsloS with id = {}", askerOslo.getId());

        String lineL1Url = "/push3/sx";
        Subscription lineL1 = createSubscription(lineL1Url, null, null, "NSB:Line:L1", "Line L1");
        logger.info("TestControl: Created subscription for line L1 with id = {}", lineL1.getId());

        logger.info("TestControl: Sends SX messages that will trigger notifications");
        postFile("/sx-vehiclejourneyref2123-pretty.xml", "sx");
        waitUntil(MetricsService.TIMER_SX_PROCESS, 1);
        waitUntil(MetricsService.TIMER_PUSH, 2);
        Thread.sleep(100); //Sleeps a little longer to detect if we send an unwanted push message

        logger.info("TestControl: Asserts expected results");
        assertEquals(1, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(1, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
        assertEquals(2, metricsService.getTimer(MetricsService.TIMER_PUSH).getCount());
        List<PtSituationElement> askerOsloMessages = getReceivedMessages(askerOsloUrl);
        logger.info("TestControl: received {} messages for subscription from Asker to Oslo", askerOsloMessages.size());
        List<PtSituationElement> l1Messages = getReceivedMessages(lineL1Url);
        logger.info("TestControl: received {} messages for subscription on L1", l1Messages.size());
        List<PtSituationElement> osloAskerMessages = getReceivedMessages(osloAskerUrl);
        logger.info("TestControl: received {} messages for subscription from Oslo to Asker", osloAskerMessages.size());
        assertEquals(1, askerOsloMessages.size()); //TODO: Should be 0 - But there are no way of detecting the direction as only affected stops are in the sx message
        assertEquals(0, l1Messages.size()); //TODO: Should be 1 - But the SX message from NSB does not contain any LineRef.
        assertEquals(1, osloAskerMessages.size()); //TODO: Correct - But pure luck as there are no way of detecting the direction as only affected stops are in the sx message
    }

    @Test
    public void testRuterSX() throws Exception {
        assertEquals(0, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_PUSH).getCount());

        stubFor(post(urlMatching("/push.*/sx"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        String line5Url = "/push1/sx";
        Subscription line5 = createSubscription(line5Url, null, null, "RUT:Line:5", "Line 5");
        logger.info("TestControl: Created subscription for line 5 with id = {}", line5.getId());

        String codespaceUrl = "/push2/sx";
        Subscription codespace = createSubscription(codespaceUrl, null, null, null, "codespace RUT", "RUT");
        logger.info("TestControl: Created subscription for codespace RUT with id = {}", codespace.getId());

        logger.info("TestControl: Sends SX messages that will trigger notifications");
        postFile("/sx-ruter.xml", "sx");
        waitUntil(MetricsService.TIMER_SX_PROCESS, 1);
        waitUntil(MetricsService.TIMER_PUSH, 2);
        Thread.sleep(100); //Sleeps a little longer to detect if we send an unwanted push message

        logger.info("TestControl: Asserts expected results");
        assertEquals(1, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(1, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
        assertEquals(2, metricsService.getTimer(MetricsService.TIMER_PUSH).getCount());
        List<PtSituationElement> l5Messages = getReceivedMessages(line5Url);
        logger.info("TestControl: received {} messages for subscription on Line 5", l5Messages.size());
        assertEquals(1, l5Messages.size());
        List<PtSituationElement> rutMessages = getReceivedMessages(codespaceUrl);
        logger.info("TestControl: received {} messages for subscription on codespace RUT", rutMessages.size());
        assertEquals(1, rutMessages.size());
    }


    private List<PtSituationElement> getReceivedMessages(String pushAddress) throws JAXBException, XMLStreamException {
        List<LoggedRequest> loggedRequests = findAll(postRequestedFor(urlEqualTo(pushAddress)));
        ArrayList<PtSituationElement> result = new ArrayList<>(loggedRequests.size());
        for (LoggedRequest request : loggedRequests) {
            result.add(siriMarshaller.unmarshall(request.getBodyAsString(), PtSituationElement.class));
        }
        return result;
    }

    private Subscription createSubscription(String pushAddress, String from, String to, String line, String name) throws Exception {
        return createSubscription(pushAddress, from, to, line, name, null);
    }

    private Subscription createSubscription(String pushAddress, String from, String to, String line, String name, String codespace) throws Exception {
        Subscription subscription = new Subscription();
        if (from != null) subscription.addFromStopPoint(from);
        if (to != null) subscription.addToStopPoint(to);
        if (line != null) subscription.addLineRef(line);
        if (codespace != null) subscription.addCodespace(codespace);
        subscription.setName(name);
        pushAddress = pushAddress.substring(0, pushAddress.length()-3); //last '/et' (or '/sx') is added by the subscription manager
        subscription.setPushAddress("http://localhost:" + config.getWiremockPort() + pushAddress);
        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = mapper.writeValueAsString(subscription).getBytes();
        String postUrl = "http://localhost:"+config.getRestPort()+"/external/subscription";
        HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Content-Length", "" +Integer.toString(bytes.length));
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

    private void postFile(String classpathResource, String type) throws IOException {
        String requestorId = sharedProperties.get("AnsharRequestorId");
        assertNotNull(requestorId);
        byte[] bytes = IOUtils.toByteArray(this.getClass().getResourceAsStream(classpathResource));
        String postUrl = "http://localhost:"+config.getRestPort()+"/internal/siriMessages/"+requestorId+"/"+type+"/";
        HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/xml");
        connection.setRequestProperty("Content-Length", "" +Integer.toString(bytes.length));
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
            connection.setRequestProperty("Content-Length", "" +Integer.toString(body.getBytes().length));
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

    private void waitUntil(String timer, int expectedCount) throws InterruptedException {
        //things are asynchronous: wait until expected conditions are met (or time out)
        long start = System.currentTimeMillis();
        while (metricsService.getTimer(timer).getCount() < expectedCount) {
            if ((System.currentTimeMillis() - start) > 5000) {
                fail("This takes to long...");
            }
            Thread.sleep(10);
        }
    }


}