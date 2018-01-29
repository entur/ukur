package org.entur.ukur.subscription;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.core.IMap;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import uk.org.siri.siri20.EstimatedCall;
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.NaturalLanguageStringStructure;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class SubscriptionManagerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());

    private SubscriptionManager subscriptionManager;
    private IMap<String, Subscription> subscriptions;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        IMap<String, Set<String>> subscriptionsPerStopPoint = mock(IMap.class);
        subscriptions = mock(IMap.class);
        IMap<String, List<PushMessage>> pushMessagesMemoryStore = mock(IMap.class);
        IMap<String, Long> alreadySentCache = mock(IMap.class);
        subscriptionManager = new SubscriptionManager(subscriptionsPerStopPoint, subscriptions,
                pushMessagesMemoryStore, alreadySentCache, new SiriMarshaller());
    }

    @Test
    public void testPushOk() throws IOException {

        String url = "/push/ok";
        stubFor(post(urlEqualTo(url))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(json(PushAcknowledge.OK))));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        subscriptionManager.notify(subscriptions, getEstimatedCall(), new EstimatedVehicleJourney());
        verify(1, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
    }

    @Test
    public void testPushForget() throws IOException {

        String url = "/push/forget";
        stubFor(post(urlEqualTo(url))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(json(PushAcknowledge.FORGET_ME))));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);
        subscriptionManager.notify(subscriptions, getEstimatedCall(), new EstimatedVehicleJourney());
        verify(1, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions).remove(subscription.getId());
        assertEquals(0, subscription.getFailedPushCounter());
    }

    @Test
    public void testPushError()  {

        String url = "/push/error";
        stubFor(post(urlEqualTo(url))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("Internal server error")));

        Subscription subscription = createSubscription(url);
        subscriptionManager.add(subscription);
        verify(0, postRequestedFor(urlEqualTo(url)));
        assertEquals(0, subscription.getFailedPushCounter());
        HashSet<Subscription> subscriptions = new HashSet<>();
        subscriptions.add(subscription);

        subscriptionManager.notify(subscriptions, getEstimatedCall(), new EstimatedVehicleJourney());
        verify(1, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(1, subscription.getFailedPushCounter());

        subscriptionManager.notify(subscriptions, getEstimatedCall(), new EstimatedVehicleJourney());
        verify(2, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(2, subscription.getFailedPushCounter());

        subscriptionManager.notify(subscriptions, getEstimatedCall(), new EstimatedVehicleJourney());
        verify(3, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(3, subscription.getFailedPushCounter());

        subscriptionManager.notify(subscriptions, getEstimatedCall(), new EstimatedVehicleJourney());
        verify(4, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(1)).remove(subscription.getId());
        assertEquals(4, subscription.getFailedPushCounter());
    }

    private Subscription createSubscription(String s) {
        Subscription subscription = new Subscription();
        subscription.addFromStopPoint("NSR:Quay:232");
        subscription.addToStopPoint("NSR:Quay:125");
        subscription.setName("Push over http test");
        subscription.setPushAddress("http://localhost:" + wireMockRule.port() + s);
        return subscription;
    }

    private EstimatedCall getEstimatedCall() {
        EstimatedCall estimatedCall = new EstimatedCall();
        NaturalLanguageStringStructure e = new NaturalLanguageStringStructure();
        e.setValue("Test");
        estimatedCall.getStopPointNames().add(e);
        return estimatedCall;
    }

    private String json(PushAcknowledge acknowledge) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(acknowledge);
    }
}