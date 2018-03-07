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

package org.entur.ukur.service;

import com.google.cloud.datastore.*;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.collect.Iterators;
import com.hazelcast.core.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.subscription.Subscription;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class GoogleDatastoreServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(GoogleDatastoreServiceTest.class);
    private static final LocalDatastoreHelper HELPER = LocalDatastoreHelper.create(1.0);

    private Datastore datastore;

    /**
     * Starts the local Datastore emulator.
     */
    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException {
        logger.info("Starts HELPER...");
        HELPER.start();
        logger.info("...HELPER started");
    }

    /**
     * Initializes Datastore and cleans out any residual values.
     */
    @Before
    public void setUp() {
        datastore = HELPER.getOptions().toBuilder().setNamespace(GoogleDatastoreServiceTest.class.getSimpleName()).build().getService();
        StructuredQuery<Key> query = Query.newKeyQueryBuilder().build();
        QueryResults<Key> result = datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        logger.info("Deletes {} entries from the local emulated datastore before test", keys.length);
        datastore.delete(keys);
    }

    /**
     * Stops the local Datastore emulator.
     */
    @AfterClass
    public static void afterClass() throws IOException, InterruptedException, TimeoutException {
        logger.info("Stops HELPER...");
        HELPER.stop(Duration.ofSeconds(10));
        logger.info("...HELPER stopped");
    }


    @Test
    public void testSubscriptionHandling() {
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        GoogleDatastoreService service = new GoogleDatastoreService(datastore, liveJourneyIMap);
        Subscription subscription = new Subscription();
        subscription.setPushAddress("http://somehost/test");
        subscription.setName("Test#1");
        subscription.addFromStopPoint("NSR:Quay:1");
        subscription.addToStopPoint("NSR:Quay:2");

        //add new
        Subscription addedSubscription = service.addSubscription(subscription);
        assertNotNull(addedSubscription.getId());
        assertEquals(subscription.getName(), addedSubscription.getName());
        assertEquals(subscription.getPushAddress(), addedSubscription.getPushAddress());
        assertThat(subscription.getFromStopPoints(), is(addedSubscription.getFromStopPoints()));
        assertThat(subscription.getToStopPoints(), is(addedSubscription.getToStopPoints()));

        //makes sure it's listed
        Collection<Subscription> subscriptions = service.getSubscriptions();
        assertEquals(1, subscriptions.size());
        assertEquals(addedSubscription.getId(), subscriptions.iterator().next().getId());

        //and that it is counted
        assertEquals(1, service.getNumberOfSubscriptions());
        
        //add another subscription to test getSubscriptionsForStopPoint
        Subscription anotherSubscription = new Subscription();
        anotherSubscription.setPushAddress("http://someotherhost/test");
        anotherSubscription.setName("Test#2");
        anotherSubscription.addFromStopPoint("NSR:Quay:3");
        anotherSubscription.addToStopPoint("NSR:Quay:2");
        Subscription anotherAddedSubscription = service.addSubscription(anotherSubscription);

        //make sure it is counted
        assertEquals(2, service.getNumberOfSubscriptions());

        //search for subscriptions with quay 3
        Set<Subscription> subscriptionsForStopPoint = service.getSubscriptionsForStopPoint("NSR:Quay:3");
        assertEquals(1, subscriptionsForStopPoint.size());
        Subscription s = subscriptionsForStopPoint.iterator().next();
        assertEquals(anotherAddedSubscription.getId(), s.getId());

        //update the subscription so it no longer has quay 3 - instead 33 that should not be found even though it still contains the previous value
        s.removeFromStopPoint("NSR:Quay:3");
        s.addFromStopPoint("NSR:Quay:33");
        assertEquals(1, s.increaseFailedPushCounter()); //tests that this counter is updated as well
        s.setName("Updated");
        s.setPushAddress("http://someotherhost/updated");
        service.updateSubscription(s);
        Iterator<Subscription> iterator = service.getSubscriptions().iterator();
        Subscription updatedSubscription = null;
        while (iterator.hasNext()) {
            Subscription next = iterator.next();
            if (next.getId().equals(s.getId())) {
                updatedSubscription = next;
            }
        }
        assertNotNull(updatedSubscription);
        assertEquals(1, updatedSubscription.getFromStopPoints().size());
        assertEquals("NSR:Quay:33", updatedSubscription.getFromStopPoints().iterator().next());
        assertEquals(1, updatedSubscription.getFailedPushCounter());
        assertEquals("Updated", updatedSubscription.getName());
        assertEquals("http://someotherhost/updated", updatedSubscription.getPushAddress());

        //still only 2 subscriptions
        assertEquals(2, service.getNumberOfSubscriptions());

        //but no results for subscriptions with quay 3 (as it was modified to 33)
        subscriptionsForStopPoint = service.getSubscriptionsForStopPoint("NSR:Quay:3");
        assertEquals(0, subscriptionsForStopPoint.size());

        //both subscriptions has quay 2
        subscriptionsForStopPoint = service.getSubscriptionsForStopPoint("NSR:Quay:2");
        assertEquals(2, subscriptionsForStopPoint.size());

        //delete the last subscription
        service.removeSubscription(updatedSubscription.getId());

    }
}