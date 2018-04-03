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

package org.entur.ukur.subscription;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.testsupport.DatastoreTest;
import org.entur.ukur.xml.SiriMarshaller;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SubscriptionManagerWiremockTest extends DatastoreTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());

    private SubscriptionManager subscriptionManager;
    private Map<Object, Long> alreadySentCache;
    private SiriMarshaller siriMarshaller;
    private DataStorageService dataStorageService;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        alreadySentCache =  new HashMap<>();
        siriMarshaller = new SiriMarshaller();
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        liveJourneyIMap.clear();
        MetricsService metricsService = new MetricsService(null, 0);
        dataStorageService = new DataStorageService(datastore, liveJourneyIMap);
        subscriptionManager = new SubscriptionManager(dataStorageService, siriMarshaller, metricsService, alreadySentCache, mock(QuayAndStopPlaceMappingService.class));
    }

    @Test
    public void testETPushOk()  {

        String url = "/push/ok/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        Subscription subscription = createSubscription(url, "NSR:Quay:232", "NSR:Quay:125", null);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, new EstimatedVehicleJourney());
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
    }

    @Test
    public void testETPushForget() {

        String url = "/push/forget/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse().withStatus(205)));

        Subscription subscription = createSubscription(url, "NSR:Quay:232", "NSR:Quay:125", null);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        assertThat(dataStorageService.getSubscriptions(), hasItem(subscription));
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, new EstimatedVehicleJourney());
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertThat(dataStorageService.getSubscriptions(), CoreMatchers.not(hasItem(subscription)));
        assertFalse(new HashSet<>(subscriptionManager.listAll()).contains(subscription));
        assertEquals(0, subscription.getFailedPushCounter());
    }

    @Test
    public void testETPushError() throws InterruptedException {

        String url = "/push/error/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withHeader("Content-Type", "text/plain")
                        .withBody("Internal server error")));

        Subscription subscription = createSubscription(url, "NSR:Quay:232", "NSR:Quay:125", null);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);

        EstimatedVehicleJourney estimatedVehicleJourney = new EstimatedVehicleJourney();
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertThat(dataStorageService.getSubscriptions(), hasItem(subscription));
        waitAndVerifyFailedPushCounter(1, subscription);

        OperatorRefStructure value = new OperatorRefStructure();
        value.setValue("NSB");
        estimatedVehicleJourney.setOperatorRef(value); //must add something so it differs from the previous ones
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(2, postRequestedFor(urlEqualTo(url)));
        assertThat(dataStorageService.getSubscriptions(), hasItem(subscription));
        waitAndVerifyFailedPushCounter(2, subscription);

        estimatedVehicleJourney.setCancellation(false); //must add something so it differs from the previous ones
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(3, postRequestedFor(urlEqualTo(url)));
        assertThat(dataStorageService.getSubscriptions(), hasItem(subscription));
        waitAndVerifyFailedPushCounter(3, subscription);

        estimatedVehicleJourney.setDataSource("blabla"); //must add something so it differs from the previous ones
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(4, postRequestedFor(urlEqualTo(url)));
        waitAndVerifyFailedPushCounter(4, subscription);
        Thread.sleep(10);
        assertThat(dataStorageService.getSubscriptions(), CoreMatchers.not(hasItem(subscription)));
    }

    @Test
    public void dontPushSameETMessageMoreThanOnce() throws JAXBException, XMLStreamException {

        String url = "/push/duplicates/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));

        Subscription subscription = createSubscription(url, "NSR:Quay:232", "NSR:Quay:125", null);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertTrue(alreadySentCache.keySet().isEmpty());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        EstimatedVehicleJourney estimatedVehicleJourney = createEstimatedVehicleJourney();
        //first notify
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(1, alreadySentCache.keySet().size());

        //second notify with same payload (but new instance) (should not be sent)
        EstimatedVehicleJourney estimatedVehicleJourney1 = createEstimatedVehicleJourney();
        //to demonstrate that equals and hashcode does not work for cxf generated objects...
        assertNotEquals(estimatedVehicleJourney, estimatedVehicleJourney1);
        assertNotEquals(estimatedVehicleJourney.hashCode(), estimatedVehicleJourney1.hashCode());
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney1);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(1, alreadySentCache.keySet().size());

        //third notify with payload modified but not for subscribed stops (should not be sent)
        EstimatedCall notInterestingCall = estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls().get(1);
        assertEquals("Stop2", notInterestingCall.getStopPointNames().get(0).getValue());
        notInterestingCall.setExpectedDepartureTime(notInterestingCall.getExpectedDepartureTime().plusMinutes(1));
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(1, alreadySentCache.keySet().size());

        //fourth notify with payload modified for a subscribed stop (should be sent)
        EstimatedCall interestingCall = estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls().get(0);
        assertEquals("Stop1", interestingCall.getStopPointNames().get(0).getValue());
        interestingCall.setExpectedDepartureTime(interestingCall.getExpectedDepartureTime().plusMinutes(1));
        subscriptionManager.notifySubscriptionsOnStops(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(2, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(2, alreadySentCache.keySet().size());
    }

    @Test
    public void testSXPushMessagesForSubscriptionWithStops() throws JAXBException, XMLStreamException {

        String urlWithStop = "/push/1/sx";
        stubFor(post(urlEqualTo(urlWithStop))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));
        Subscription subscriptionWithStop = createSubscription(urlWithStop, "NSR:StopPlace:1", "NSR:StopPlace:3", "NSB:Line:Line1");
        subscriptionWithStop.addVehicleRef("1234");
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscriptionWithStop);
        subscriptionManager.notifySubscriptions(subscriptions, createPtSituationElement());

        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(urlWithStop)));
        PtSituationElement msgForSubsciptionWithStops = getPushedPtSituationElement(urlWithStop);
        //For subscriptions with should all unsubscribed stops, and journeys with unsubscribed vehiclerefs og linerefs
        assertNotNull(msgForSubsciptionWithStops.getAffects());
        assertNotNull(msgForSubsciptionWithStops.getAffects().getVehicleJourneys());
        List<AffectedVehicleJourneyStructure> affectedVehicleJourneies = msgForSubsciptionWithStops.getAffects().getVehicleJourneys().getAffectedVehicleJourneies();
        assertNotNull(affectedVehicleJourneies);
        assertEquals(1, affectedVehicleJourneies.size());
        AffectedVehicleJourneyStructure affectedVehicleJourney = affectedVehicleJourneies.get(0);
        assertEquals("NSB:Line:Line1", affectedVehicleJourney.getLineRef().getValue());
        assertEquals("1234", affectedVehicleJourney.getVehicleJourneyReves().get(0).getValue());
        AffectedRouteStructure.StopPoints stopPoints = affectedVehicleJourney.getRoutes().get(0).getStopPoints();
        assertEquals(2, stopPoints.getAffectedStopPointsAndLinkProjectionToNextStopPoints().size());
    }

    @Test
    public void testSXPushMessagesForSubscriptionWithoutStops() throws JAXBException, XMLStreamException {

        String urlWithoutStop = "/push/2/sx";
        stubFor(post(urlEqualTo(urlWithoutStop))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()));
        Subscription subscriptionWithoutStop = createSubscription(urlWithoutStop, null, null, "NSB:Line:Line1");

        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscriptionWithoutStop);
        subscriptionManager.notifySubscriptions(subscriptions, createPtSituationElement());


        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(urlWithoutStop)));
        PtSituationElement msgForSubsciptionWithoutStops  = getPushedPtSituationElement(urlWithoutStop);
        //For subscriptions without stops we remove all journeys not subscribed upon: vehicle(journey)ref og lineref<-added by us from live routes, not the producer
        assertNotNull(msgForSubsciptionWithoutStops .getAffects());
        assertNotNull(msgForSubsciptionWithoutStops .getAffects().getVehicleJourneys());
        List<AffectedVehicleJourneyStructure> affectedJourneies = msgForSubsciptionWithoutStops .getAffects().getVehicleJourneys().getAffectedVehicleJourneies();
        assertNotNull(affectedJourneies);
        assertEquals(2, affectedJourneies.size());
        AffectedVehicleJourneyStructure journey1234;
        AffectedVehicleJourneyStructure journey2222;
        if ("1234".equals(affectedJourneies.get(0).getVehicleJourneyReves().get(0).getValue())) {
            journey1234 = affectedJourneies.get(0);
            journey2222 = affectedJourneies.get(1);
        } else {
            journey2222 = affectedJourneies.get(0);
            journey1234 = affectedJourneies.get(1);
        }
        assertEquals("NSB:Line:Line1", journey1234.getLineRef().getValue());
        assertEquals("1234", journey1234.getVehicleJourneyReves().get(0).getValue());
        assertEquals(4, journey1234.getRoutes().get(0).getStopPoints().getAffectedStopPointsAndLinkProjectionToNextStopPoints().size());
        assertEquals("NSB:Line:Line1", journey2222.getLineRef().getValue());
        assertEquals("2222", journey2222.getVehicleJourneyReves().get(0).getValue());
        assertEquals(3, journey2222.getRoutes().get(0).getStopPoints().getAffectedStopPointsAndLinkProjectionToNextStopPoints().size());
    }

    private PtSituationElement getPushedPtSituationElement(String urlWithStop) throws JAXBException, XMLStreamException {
        List<LoggedRequest> loggedRequests = findAll(postRequestedFor(urlEqualTo(urlWithStop)));
        assertEquals(1, loggedRequests.size());
        LoggedRequest loggedRequest = loggedRequests.get(0);
        String xml = new String(loggedRequest.getBody());
        return siriMarshaller.unmarshall(xml, PtSituationElement.class);
    }

    private PtSituationElement createPtSituationElement() throws JAXBException, XMLStreamException {
        String xml ="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<PtSituationElement xmlns=\"http://www.siri.org.uk/siri\">\n" +
                "  <CreationTime>2018-01-19T10:01:05+01:00</CreationTime>\n" +
                "  <ParticipantRef>NSB</ParticipantRef>\n" +
                "  <SituationNumber>status-167911766</SituationNumber>\n" +
                "  <Version>1</Version>\n" +
                "  <Source>\n" +
                "    <SourceType>web</SourceType>\n" +
                "  </Source>\n" +
                "  <Progress>published</Progress>\n" +
                "  <ValidityPeriod>\n" +
                "    <StartTime>2018-01-19T00:00:00+01:00</StartTime>\n" +
                "    <EndTime>2018-01-19T16:54:00+01:00</EndTime>\n" +
                "  </ValidityPeriod>\n" +
                "  <UndefinedReason/>\n" +
                "  <ReportType>incident</ReportType>\n" +
                "  <Keywords/>\n" +
                "  <Description xml:lang=\"NO\">Toget er innstilt mellom Oslo S og Drammen. Vennligst benytt neste tog.</Description>\n" +
                "  <Description xml:lang=\"EN\">The train is cancelled between Oslo S and Drammen. Passengers are requested to take the next train.</Description>\n" +
                "  <Affects>\n" +
                "    <VehicleJourneys>\n" +
                "      <AffectedVehicleJourney>\n" +
                "        <LineRef>NSB:Line:Line1</LineRef>\n" +
                "        <VehicleJourneyRef>1234</VehicleJourneyRef>\n" +
                "        <Route>\n" +
                "          <StopPoints>\n" +
                "            <AffectedOnly>true</AffectedOnly>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:1</StopPointRef>\n" +
                "              <StopPointName>Oslo S</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:2</StopPointRef>\n" +
                "              <StopPointName>Nationaltheatret</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:3</StopPointRef>\n" +
                "              <StopPointName>Skøyen</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:4</StopPointRef>\n" +
                "              <StopPointName>Lysaker</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "          </StopPoints>\n" +
                "        </Route>\n" +
                "      </AffectedVehicleJourney>\n" +
                "      <AffectedVehicleJourney>\n" +
                "        <LineRef>NSB:Line:Line1</LineRef>\n" +
                "        <VehicleJourneyRef>2222</VehicleJourneyRef>\n" +
                "        <Route>\n" +
                "          <StopPoints>\n" +
                "            <AffectedOnly>true</AffectedOnly>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:1</StopPointRef>\n" +
                "              <StopPointName>Oslo S</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:2</StopPointRef>\n" +
                "              <StopPointName>Nationaltheatret</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:3</StopPointRef>\n" +
                "              <StopPointName>Skøyen</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "          </StopPoints>\n" +
                "        </Route>\n" +
                "      </AffectedVehicleJourney>\n" +
                "      <AffectedVehicleJourney>\n" +
                "        <LineRef>NSB:Line:Line2</LineRef>\n" +
                "        <VehicleJourneyRef>4444</VehicleJourneyRef>\n" +
                "        <Route>\n" +
                "          <StopPoints>\n" +
                "            <AffectedOnly>true</AffectedOnly>\n" +
                "            <AffectedStopPoint>\n" +
                "              <StopPointRef>NSR:StopPlace:2</StopPointRef>\n" +
                "              <StopPointName>Nationaltheatret</StopPointName>\n" +
                "            </AffectedStopPoint>\n" +
                "          </StopPoints>\n" +
                "        </Route>\n" +
                "      </AffectedVehicleJourney>\n" +
                "    </VehicleJourneys>\n" +
                "  </Affects>\n" +
                "</PtSituationElement>";
        return siriMarshaller.unmarshall(xml, PtSituationElement.class);
    }

    private EstimatedVehicleJourney createEstimatedVehicleJourney() throws JAXBException, XMLStreamException {
        String xml ="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<EstimatedVehicleJourney xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\">\n" +
                "    <LineRef>NSB:Line:L123</LineRef>\n" +
                "    <DirectionRef>Test</DirectionRef>\n" +
                "    <DatedVehicleJourneyRef>2118:2018-02-08</DatedVehicleJourneyRef>\n" +
                "    <VehicleMode>rail</VehicleMode>\n" +
                "    <OriginName>Asker</OriginName>\n" +
                "    <OriginShortName>ASR</OriginShortName>\n" +
                "    <OperatorRef>NSB</OperatorRef>\n" +
                "    <ProductCategoryRef>Lt</ProductCategoryRef>\n" +
                "    <ServiceFeatureRef>passengerTrain</ServiceFeatureRef>\n" +
                "    <VehicleRef>2118</VehicleRef>\n" +
                "    <EstimatedCalls>\n" +
                "        <EstimatedCall>\n" +
                "            <StopPointRef>NSR:Quay:232</StopPointRef>\n" +
                "            <StopPointName>Stop1</StopPointName>\n" +
                "            <RequestStop>false</RequestStop>\n" +
                "            <ArrivalBoardingActivity>alighting</ArrivalBoardingActivity>\n" +
                "            <AimedDepartureTime>2018-02-08T08:48:00+01:00</AimedDepartureTime>\n" +
                "            <ExpectedDepartureTime>2018-02-08T09:00:48+01:00</ExpectedDepartureTime>\n" +
                "            <DepartureStatus>delayed</DepartureStatus>\n" +
                "            <DeparturePlatformName>6</DeparturePlatformName>\n" +
                "            <DepartureBoardingActivity>boarding</DepartureBoardingActivity>\n" +
                "        </EstimatedCall>\n" +
                "        <EstimatedCall>\n" +
                "            <StopPointRef>NSR:Quay:444</StopPointRef>\n" +
                "            <StopPointName>Stop2</StopPointName>\n" +
                "            <RequestStop>false</RequestStop>\n" +
                "            <ArrivalBoardingActivity>alighting</ArrivalBoardingActivity>\n" +
                "            <AimedDepartureTime>2018-02-08T08:49:00+01:00</AimedDepartureTime>\n" +
                "            <ExpectedDepartureTime>2018-02-08T10:00:48+01:00</ExpectedDepartureTime>\n" +
                "            <DepartureStatus>delayed</DepartureStatus>\n" +
                "            <DeparturePlatformName>6</DeparturePlatformName>\n" +
                "            <DepartureBoardingActivity>boarding</DepartureBoardingActivity>\n" +
                "        </EstimatedCall>\n" +
                "        <EstimatedCall>\n" +
                "            <StopPointRef>NSR:Quay:125</StopPointRef>\n" +
                "            <StopPointName>Stop3</StopPointName>\n" +
                "            <RequestStop>false</RequestStop>\n" +
                "            <AimedArrivalTime>2018-02-08T09:23:00+01:00</AimedArrivalTime>\n" +
                "            <ExpectedArrivalTime>2018-02-08T09:29:08+01:00</ExpectedArrivalTime>\n" +
                "            <ArrivalStatus>delayed</ArrivalStatus>\n" +
                "            <ArrivalPlatformName>9</ArrivalPlatformName>\n" +
                "            <ArrivalBoardingActivity>alighting</ArrivalBoardingActivity>\n" +
                "            <AimedDepartureTime>2018-02-08T09:26:00+01:00</AimedDepartureTime>\n" +
                "            <ExpectedDepartureTime>2018-02-08T09:29:38+01:00</ExpectedDepartureTime>\n" +
                "            <DepartureStatus>delayed</DepartureStatus>\n" +
                "            <DeparturePlatformName>9</DeparturePlatformName>\n" +
                "            <DepartureBoardingActivity>boarding</DepartureBoardingActivity>\n" +
                "        </EstimatedCall>\n" +
                "    </EstimatedCalls>\n" +
                "</EstimatedVehicleJourney>";
        return siriMarshaller.unmarshall(xml, EstimatedVehicleJourney.class);
    }

    private Subscription createSubscription(String pushAddress, String from, String to, String line) {
        Subscription subscription = new Subscription();
        if (from != null) subscription.addFromStopPoint(from);
        if (to != null) subscription.addToStopPoint(to);
        if (line != null) subscription.addLineRef(line);
        subscription.setName("Push over http test");
        pushAddress = pushAddress.substring(0, pushAddress.length()-3); //last '/et' (or '/sx') is added by the subscription manager
        subscription.setPushAddress("http://localhost:" + wireMockRule.port() + pushAddress);
        return subscriptionManager.add(subscription);
    }

    private void waitAndVerifyFailedPushCounter(int expected, Subscription subscription) {
        long start = System.currentTimeMillis();
        long actual = 0;
        while (System.currentTimeMillis() - start < 10000) {
            actual = subscription.getFailedPushCounter();
            if (actual > expected) {
                fail("Expected " + expected + " found " + actual);
            }
            if (actual == expected) {
                return;
            }
        }
        fail("Expected " + expected + " but found only " + actual+" before we timed out...");
    }

    /**
     * Since messages are pushed asynchronously, we must allow some waiting before we can verify receival.
     */
    private void waitAndVerifyAtLeast(int expected, RequestPatternBuilder requestPatternBuilder) {
        long start = System.currentTimeMillis();
        int actual = 0;
        while (System.currentTimeMillis() - start < 10000) {
            List<LoggedRequest> all = findAll(requestPatternBuilder);
            actual = all.size();
            if (actual > expected) {
                fail("Expected " + expected + " found " + actual);
            }
            if (actual == expected) {
                return;
            }
        }
        fail("Expected " + expected + " but found only " + actual+" before we timed out...");
    }

}