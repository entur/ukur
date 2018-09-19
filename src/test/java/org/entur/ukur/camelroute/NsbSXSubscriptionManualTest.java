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
import uk.org.siri.siri20.PtSituationElement;

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
import java.util.*;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"SameParameterValue", "Duplicates"})
public class NsbSXSubscriptionManualTest extends DatastoreTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());
    private SubscriptionManager subscriptionManager;
    private SXSubscriptionProcessor SXSubscriptionProcessor;
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;
    private SiriMarshaller siriMarshaller;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        HazelcastInstance hazelcastInstance = new TestHazelcastInstanceFactory().newHazelcastInstance();
        ITopic<String> subscriptionTopic = hazelcastInstance.getTopic("subscriptions");
        MetricsService metricsService = new MetricsService();
        siriMarshaller = new SiriMarshaller();
        DataStorageService dataStorageService = new DataStorageService(datastore, subscriptionTopic);
        quayAndStopPlaceMappingService = new QuayAndStopPlaceMappingService(metricsService);
        subscriptionManager = new SubscriptionManager(dataStorageService, siriMarshaller, metricsService, new HashMap<>(), quayAndStopPlaceMappingService);
        SXSubscriptionProcessor = new SXSubscriptionProcessor(subscriptionManager, siriMarshaller, mock(FileStorageService.class), metricsService);
    }

    @Test
    @Ignore //So idea don't run it as part of package/folder tests
    public void prosessRecordedSXMessages() throws Exception {

        LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
        logCtx.getLogger("org.entur").setLevel(Level.INFO); //pauses TRACE logging as we trace a lot during setup...

        populateQuayAndStopPlaceMappingService("https://api-test.entur.org/stop_places/1.0/list/stop_place_quays/");

        logCtx.getLogger("org.entur").setLevel(Level.TRACE);

        stubFor(post(urlEqualTo("/subscription1/sx"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));
        stubFor(post(urlEqualTo("/subscription2/sx"))
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

        assertEquals(2, subscriptionManager.listAll().size());

        List<Path> sxMessages = Files.walk(Paths.get("/home/jon/Documents/Entur/nsb_sanntidsmeldinger/sx/"))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        for (int i = 0; i < sxMessages.size(); i++) {
            logger.info("About to process message {}/{} ...", (i+1), sxMessages.size());
            long start = System.currentTimeMillis();
            SXSubscriptionProcessor.process(createExchangeMock(new FileInputStream(sxMessages.get(i).toFile())));
            logger.info("... processing took {} ms", String.format("%,d", (System.currentTimeMillis()-start)));
        }

        long sleepStart = System.currentTimeMillis();
        int activePushThreads = subscriptionManager.getActivePushThreads();
        while (activePushThreads > 0) {
            if (System.currentTimeMillis() - sleepStart > 10_000) fail("Seems like the push-threads never will finish...");
            List<LoggedRequest> s1Requests = findAll(postRequestedFor(urlEqualTo("/subscription1/sx")));
            List<LoggedRequest> s2Requests = findAll(postRequestedFor(urlEqualTo("/subscription2/sx")));
            logger.info("There are still {} active threads working on pushing messages - sleeps before checking received messages. Received push-messages: subscription1={}, subscription2={}", activePushThreads, s1Requests.size(), s2Requests.size());
            Thread.sleep(1000);
            activePushThreads = subscriptionManager.getActivePushThreads();
        }
        logger.info("Finished processing {} SX messages", sxMessages.size());
        List<String> s1SituationsNumbers = getReceivedSituationNumbers("/subscription1/sx");
        List<String> s2SituationsNumbers = getReceivedSituationNumbers("/subscription2/sx");

        logger.info("s1SituationsNumbers: {}", s1SituationsNumbers.size());
        logger.info("s2SituationsNumbers: {}", s2SituationsNumbers.size());
        assertEquals(s1SituationsNumbers.size(), s2SituationsNumbers.size());
        assertTrue(s1SituationsNumbers.containsAll(s2SituationsNumbers));
        assertTrue(s2SituationsNumbers.containsAll(s1SituationsNumbers));
    }

    private List<String> getReceivedSituationNumbers(String pushAddress) throws JAXBException, XMLStreamException {
        List<LoggedRequest> loggedRequests = findAll(postRequestedFor(urlEqualTo(pushAddress)));
        List<String> s2SituationsNumbers = getSituationNumbers(loggedRequests);
        logger.info("Received {} push-messages for push address {} with {} unique situationNumbers: ",
                s2SituationsNumbers.size(), pushAddress, new HashSet<>(s2SituationsNumbers).size(), s2SituationsNumbers);
        return s2SituationsNumbers;
    }

    private List<String> getSituationNumbers(List<LoggedRequest> loggedRequests) throws JAXBException, XMLStreamException {
        ArrayList<String> result = new ArrayList<>(loggedRequests.size());
        for (LoggedRequest request : loggedRequests) {
            PtSituationElement ptSituationElement = siriMarshaller.unmarshall(request.getBodyAsString(), PtSituationElement.class);
            result.add(ptSituationElement.getSituationNumber().getValue());
        }
        return result;
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
            logger.info("It took {} ms to download and handle {} stop places ", String.format("%,d", (System.currentTimeMillis()-start)), stopsFromTiamat.size());
        } catch (IOException e) {
            logger.error("Could not get list of stopplaces and quays", e);
        }

    }

}