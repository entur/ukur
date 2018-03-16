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
import static org.junit.Assert.*;

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
        subscription.addLineRef("NSB:Line:Test1");
        subscription.addLineRef("NSB:Line:Test2");
        subscription.addVehicleRef("1234");

        //add new
        Subscription addedSubscription = service.addSubscription(subscription);
        assertNotNull(addedSubscription.getId());

        assertEquals(subscription.getName(), addedSubscription.getName());
        assertEquals(subscription.getPushAddress(), addedSubscription.getPushAddress());
        assertThat(addedSubscription.getFromStopPoints(), is(subscription.getFromStopPoints()));
        assertThat(addedSubscription.getToStopPoints(), is(subscription.getToStopPoints()));
        assertThat(addedSubscription.getVehicleRefs(), is(subscription.getVehicleRefs()));
        assertThat(addedSubscription.getLineRefs(), is(subscription.getLineRefs()));

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

        //Makes sure lineRefs and vehicleJourneyRefs are found (only new subscription has them set without stops - the first one has them also but with stops)
        Subscription subscriptionWithOnlyLineAndVehicleJourney = new Subscription();
        subscriptionWithOnlyLineAndVehicleJourney.setPushAddress("http://someotherhost/test_line_vehiclejourney");
        subscriptionWithOnlyLineAndVehicleJourney.setName("Test#3");
        subscriptionWithOnlyLineAndVehicleJourney.addLineRef("NSB:Line:Test1");
        subscriptionWithOnlyLineAndVehicleJourney.addVehicleRef("1234");
        subscriptionWithOnlyLineAndVehicleJourney.addVehicleRef("5678");
        Subscription newSubscription = service.addSubscription(subscriptionWithOnlyLineAndVehicleJourney);
        assertEquals(3, service.getNumberOfSubscriptions());
        //Find vehiclejourney
        Set<Subscription> subscriptionsForvehicleJourneyRef = service.getSubscriptionsForvehicleRefAndNoStops("1234");
        assertEquals(1, subscriptionsForvehicleJourneyRef.size());
        assertEquals(newSubscription.getId(), subscriptionsForvehicleJourneyRef.iterator().next().getId());
        //Find line
        Set<Subscription> subscriptionsForLineRef = service.getSubscriptionsForLineRefAndNoStops("NSB:Line:Test1");
        assertEquals(1, subscriptionsForLineRef.size());
        assertEquals(newSubscription.getId(), subscriptionsForLineRef.iterator().next().getId());

        //delete the second subscription
        assertEquals(3, service.getNumberOfSubscriptions());
        service.removeSubscription(updatedSubscription.getId());
        Collection<Subscription> subscriptionList = service.getSubscriptions();
        assertEquals(2, subscriptionList.size());
        for (Subscription sub : subscriptionList) {
            if (updatedSubscription.getId().equals(sub.getId())) {
                fail("Subscription should have been deleted");
            }
        }
    }

    @Test
    public void testLineOnlySubscription() {
        IMap<String, LiveJourney> liveJourneyIMap = new TestHazelcastInstanceFactory().newHazelcastInstance().getMap("journeys");
        GoogleDatastoreService service = new GoogleDatastoreService(datastore, liveJourneyIMap);
        Subscription subscription = new Subscription();
        subscription.setPushAddress("http://somehost/test");
        subscription.setName("Test#1");
        String line = "NSB:Line:L1";
        subscription.addLineRef(line);
        service.addSubscription(subscription);
        int unexistingVehicleRef1 = service.getSubscriptionsForvehicleRefAndNoStops("unexisting").size();
        int unexistingVehicleRef2 = service.getSubscriptionsForvehicleRefAndNoStops(line).size();
        int unexistingLineRef = service.getSubscriptionsForLineRefAndNoStops("unexisting").size();
        int existingLineRef = service.getSubscriptionsForLineRefAndNoStops(line).size();
        logger.debug("Found {} with vehicleref=unexisting, {} with vechicleref={}, {} with lineref=unexisting and {} with lineref={}", unexistingVehicleRef1, unexistingVehicleRef2, line, unexistingLineRef, existingLineRef, line);
        assertEquals(0, unexistingVehicleRef1);
        assertEquals(0, unexistingVehicleRef2);
        assertEquals(0, unexistingLineRef);
        assertEquals(1, existingLineRef);
    }
}