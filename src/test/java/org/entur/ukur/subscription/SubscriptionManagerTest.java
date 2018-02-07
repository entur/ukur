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
import com.hazelcast.core.IMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import uk.org.siri.siri20.EstimatedVehicleJourney;

import java.util.HashSet;
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
    public void setUp() {
        IMap<String, Set<String>> subscriptionsPerStopPoint = mock(IMap.class);
        subscriptions = mock(IMap.class);
        IMap<Object, Long> alreadySentCache = mock(IMap.class);
        subscriptionManager = new SubscriptionManager(subscriptionsPerStopPoint, subscriptions, alreadySentCache);
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
        verify(1, postRequestedFor(urlEqualTo(url)));
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
        verify(1, postRequestedFor(urlEqualTo(url)));
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

        subscriptionManager.notify(subscriptions, new EstimatedVehicleJourney());
        verify(1, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(1, subscription.getFailedPushCounter());

        subscriptionManager.notify(subscriptions, new EstimatedVehicleJourney());
        verify(2, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(2, subscription.getFailedPushCounter());

        subscriptionManager.notify(subscriptions, new EstimatedVehicleJourney());
        verify(3, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(0)).remove(subscription.getId());
        assertEquals(3, subscription.getFailedPushCounter());

        subscriptionManager.notify(subscriptions, new EstimatedVehicleJourney());
        verify(4, postRequestedFor(urlEqualTo(url)));
        Mockito.verify(this.subscriptions, times(1)).remove(subscription.getId());
        assertEquals(4, subscription.getFailedPushCounter());
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

}