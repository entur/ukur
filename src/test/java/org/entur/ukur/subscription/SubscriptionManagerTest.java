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
import org.entur.ukur.service.DataStorageHazelcastService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import uk.org.siri.siri20.EstimatedCall;
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.OperatorRefStructure;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class SubscriptionManagerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());

    private SubscriptionManager subscriptionManager;
    private Map subscriptions;
    private Map<Object, Long> alreadySentCache;
    private SiriMarshaller siriMarshaller;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws JAXBException {
        Map<String, Set<String>> subscriptionsPerStopPoint = mock(Map.class);
        subscriptions = mock(Map.class);
        alreadySentCache =  new HashMap<>();
        siriMarshaller = new SiriMarshaller();
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        MetricsService metricsService = new MetricsService(null, 0);
        subscriptionManager = new SubscriptionManager(new DataStorageHazelcastService(subscriptionsPerStopPoint,
                subscriptions, liveJourneyIMap), siriMarshaller, metricsService, alreadySentCache);
    }

    @Test
    public void testPushOk()  {

        String url = "/push/ok/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody(PushAcknowledge.OK.name())));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        subscriptionManager.notify(subscriptions, new EstimatedVehicleJourney());
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
    }

    @Test
    public void testPushForget() {

        String url = "/push/forget/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody(PushAcknowledge.FORGET_ME.name())));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        subscriptionManager.notify(subscriptions, new EstimatedVehicleJourney());
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions).remove(subscription.getId());
        assertEquals(0, subscription.getFailedPushCounter());
    }

    @Test
    public void testPushError()  {

        String url = "/push/error/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withHeader("Content-Type", "text/plain")
                        .withBody("Internal server error")));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);

        EstimatedVehicleJourney estimatedVehicleJourney = new EstimatedVehicleJourney();
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(1, subscription.getFailedPushCounter());

        OperatorRefStructure value = new OperatorRefStructure();
        value.setValue("NSB");
        estimatedVehicleJourney.setOperatorRef(value); //must add something so it differs from the previous ones
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(2, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(2, subscription.getFailedPushCounter());

        estimatedVehicleJourney.setCancellation(false); //must add something so it differs from the previous ones
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(3, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(3, subscription.getFailedPushCounter());

        estimatedVehicleJourney.setDataSource("blabla"); //must add something so it differs from the previous ones
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(4, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(1)).remove(subscription.getId());
        assertEquals(4, subscription.getFailedPushCounter());
    }

    @Test
    public void dontPushSameMessageMoreThanOnce() throws JAXBException, XMLStreamException {

        String url = "/push/duplicates/et";
        stubFor(post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo("application/xml"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody(PushAcknowledge.OK.name())));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertTrue(alreadySentCache.keySet().isEmpty());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        EstimatedVehicleJourney estimatedVehicleJourney = createEstimatedVehicleJourney();
        //first notify
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(1, alreadySentCache.keySet().size());

        //second notify with same payload (but new instance) (should not be sent)
        EstimatedVehicleJourney estimatedVehicleJourney1 = createEstimatedVehicleJourney();
        //to demonstrate that equals and hashcode does not work for cxf generated objects...
        assertNotEquals(estimatedVehicleJourney, estimatedVehicleJourney1);
        assertNotEquals(estimatedVehicleJourney.hashCode(), estimatedVehicleJourney1.hashCode());
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney1);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(1, alreadySentCache.keySet().size());

        //third notify with payload modified but not for subscribed stops (should not be sent)
        EstimatedCall notInterestingCall = estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls().get(1);
        assertEquals("Stop2", notInterestingCall.getStopPointNames().get(0).getValue());
        notInterestingCall.setExpectedDepartureTime(notInterestingCall.getExpectedDepartureTime().plusMinutes(1));
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(1, alreadySentCache.keySet().size());

        //fourth notify with payload modified for a subscribed stop (should be sent)
        EstimatedCall interestingCall = estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls().get(0);
        assertEquals("Stop1", interestingCall.getStopPointNames().get(0).getValue());
        interestingCall.setExpectedDepartureTime(interestingCall.getExpectedDepartureTime().plusMinutes(1));
        subscriptionManager.notify(subscriptions, estimatedVehicleJourney);
        waitAndVerifyAtLeast(2, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        assertEquals(2, alreadySentCache.keySet().size());
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
        return siriMarshaller.unmarhall(xml, EstimatedVehicleJourney.class);
    }

    private Subscription createSubscription(String pushAddress) {
        Subscription subscription = new Subscription();
        subscription.addFromStopPoint("NSR:Quay:232");
        subscription.addToStopPoint("NSR:Quay:125");
        subscription.setName("Push over http test");
        pushAddress = pushAddress.substring(0, pushAddress.length()-3); //last '/et' (or '/sx') is added by the subscription manager
        subscription.setPushAddress("http://localhost:" + wireMockRule.port() + pushAddress);
        return subscription;
    }

    /**
     * Since messages are pushed asynchronously, we must allow some waiting before we can verify receival.
     */
    private void waitAndVerifyAtLeast(int expected, RequestPatternBuilder requestPatternBuilder) {
        long start = System.currentTimeMillis();
        int actual = 0;
        while (System.currentTimeMillis() - start < 5000) {
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