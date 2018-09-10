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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionTypeEnum;
import org.entur.ukur.testsupport.DatastoreTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static org.entur.ukur.subscription.SubscriptionTypeEnum.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@SuppressWarnings("ALL")
public class DataStorageServiceTest extends DatastoreTest {
    private static final Logger logger = LoggerFactory.getLogger(DataStorageServiceTest.class);
    private HazelcastInstance hazelcastInstance;


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hazelcastInstance = new TestHazelcastInstanceFactory().newHazelcastInstance();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstance.shutdown();
    }

    @Test
    public void testSubscriptionHandling() {
        ITopic<String> subscriptionTopic = hazelcastInstance.getTopic("subscriptions");
        DataStorageService service = new DataStorageService(datastore, null, subscriptionTopic);
        Subscription subscription = createSubscription("Test#1", ET, "ABC", "NSR:Quay:1", "NSR:Quay:2", "NSB:Line:Test1");

        //add new
        Subscription addedSubscription = service.addSubscription(subscription);
        assertNotNull(addedSubscription.getId());

        assertEquals(subscription.getName(), addedSubscription.getName());
        assertEquals(subscription.getPushAddress(), addedSubscription.getPushAddress());
        assertThat(addedSubscription.getFromStopPoints(), is(subscription.getFromStopPoints()));
        assertThat(addedSubscription.getToStopPoints(), is(subscription.getToStopPoints()));
        assertThat(addedSubscription.getCodespaces(), is(subscription.getCodespaces()));
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
        anotherSubscription.setType(ET);
        Subscription anotherAddedSubscription = service.addSubscription(anotherSubscription);

        //make sure it is counted
        assertEquals(2, service.getNumberOfSubscriptions());

        //search for subscriptions with quay 3
        Set<Subscription> subscriptionsForStopPoint = service.getSubscriptionsForStopPoint("NSR:Quay:3", ET);
        assertEquals(1, subscriptionsForStopPoint.size());
        Subscription s = subscriptionsForStopPoint.iterator().next();
        assertEquals(anotherAddedSubscription.getId(), s.getId());

        //test various subscriptions types on stoppoint
        assertEquals(0, service.getSubscriptionsForStopPoint("NSR:Quay:3", ALL).size());
        assertEquals(0, service.getSubscriptionsForStopPoint("NSR:Quay:3", SX).size());

        //update the subscription so it no longer has quay 3 - instead 33 that should not be found even though it still contains the previous value
        s.removeFromStopPoint("NSR:Quay:3");
        s.addFromStopPoint("NSR:Quay:33");
        assertFalse(s.shouldRemove());
        assertEquals(1, s.getFailedPushCounter());
        assertNotNull(s.getFirstErrorSeen());
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
        assertEquals(1, updatedSubscription.getFailedPushCounter());
        assertNotNull(updatedSubscription.getFirstErrorSeen());

        //still only 2 subscriptions
        assertEquals(2, service.getNumberOfSubscriptions());

        //but no results for subscriptions with quay 3 (as it was modified to 33)
        subscriptionsForStopPoint = service.getSubscriptionsForStopPoint("NSR:Quay:3", ALL);
        assertEquals(0, subscriptionsForStopPoint.size());

        //both subscriptions has quay 2
        subscriptionsForStopPoint = service.getSubscriptionsForStopPoint("NSR:Quay:2", ET);
        assertEquals(2, subscriptionsForStopPoint.size());

        //Makes sure lineRefs and vehicleJourneyRefs are found (only new subscription has them set without stops - the first one has them also but with stops)
        Subscription subscriptionWithOnlyLineAndVehicleJourney = new Subscription();
        subscriptionWithOnlyLineAndVehicleJourney.setPushAddress("http://someotherhost/test_line_vehiclejourney");
        subscriptionWithOnlyLineAndVehicleJourney.setName("Test#3");
        subscriptionWithOnlyLineAndVehicleJourney.addLineRef("NSB:Line:Test1");
        subscriptionWithOnlyLineAndVehicleJourney.addCodespace("ABC");
        subscriptionWithOnlyLineAndVehicleJourney.addCodespace("DEF");
        subscriptionWithOnlyLineAndVehicleJourney.setType(SX);
        Subscription newSubscription = service.addSubscription(subscriptionWithOnlyLineAndVehicleJourney);
        assertEquals(3, service.getNumberOfSubscriptions());
        //Find codespace
        Set<Subscription> subscriptionsForvehicleJourneyRef = service.getSubscriptionsForCodespaceAndNoStops("ABC", SX);
        assertEquals(1, subscriptionsForvehicleJourneyRef.size());
        assertEquals(newSubscription.getId(), subscriptionsForvehicleJourneyRef.iterator().next().getId());
        //test various subscriptions types on codespace
        assertEquals(0, service.getSubscriptionsForCodespaceAndNoStops("ABC", ALL).size());
        assertEquals(0, service.getSubscriptionsForCodespaceAndNoStops("ABC", ET).size());

        //Find line
        Set<Subscription> subscriptionsForLineRef = service.getSubscriptionsForLineRefAndNoStops("NSB:Line:Test1", SX);
        assertEquals(1, subscriptionsForLineRef.size());
        assertEquals(newSubscription.getId(), subscriptionsForLineRef.iterator().next().getId());
        //test various subscriptions types on line
        assertEquals(0, service.getSubscriptionsForLineRefAndNoStops("NSB:Line:Test1", ALL).size());
        assertEquals(0, service.getSubscriptionsForLineRefAndNoStops("NSB:Line:Test1", ET).size());


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
        IMap<String, LiveJourney> liveJourneyIMap = hazelcastInstance.getMap("journeys");
        ITopic<String> subscriptionTopic = hazelcastInstance.getTopic("subscriptions");
        DataStorageService service = new DataStorageService(datastore, liveJourneyIMap, subscriptionTopic);
        Subscription subscription = new Subscription();
        subscription.setPushAddress("http://somehost/test");
        subscription.setName("Test#1");
        String line = "NSB:Line:L1";
        subscription.addLineRef(line);
        service.addSubscription(subscription);

        Subscription notLineOnlySubscription = new Subscription();
        notLineOnlySubscription.setPushAddress("http://somehost/test");
        notLineOnlySubscription.setName("Both line and stops");
        notLineOnlySubscription.addLineRef(line);
        notLineOnlySubscription.addFromStopPoint("NSR:Quay:1");
        notLineOnlySubscription.addToStopPoint("NSR:Quay:2");
        service.addSubscription(notLineOnlySubscription);

        int unexistingCodespace1 = service.getSubscriptionsForCodespaceAndNoStops("unexisting", ALL).size();
        int unexistingCodespace2 = service.getSubscriptionsForCodespaceAndNoStops(line, ALL).size();
        int unexistingLineRef = service.getSubscriptionsForLineRefAndNoStops("unexisting", ALL).size();
        int existingLineRef = service.getSubscriptionsForLineRefAndNoStops(line, ALL).size();
        logger.debug("Found {} with codespace=unexisting, {} with codespace={}, {} with lineref=unexisting and {} with lineref={}", unexistingCodespace1, unexistingCodespace2, line, unexistingLineRef, existingLineRef, line);
        assertEquals(0, unexistingCodespace1);
        assertEquals(0, unexistingCodespace2);
        assertEquals(0, unexistingLineRef);
        assertEquals(1, existingLineRef);
    }

    @Test
    public void testSubscriptionSyncing() throws InterruptedException {

        ITopic<String> subscriptionTopic = hazelcastInstance.getTopic("subsync#" + System.currentTimeMillis());
        DataStorageService service1 = new DataStorageService(datastore, null, subscriptionTopic);
        service1.logger = LoggerFactory.getLogger(DataStorageService.class.getName()+"#1");
        service1.populateSubscriptionCacheFromDatastore(); //postconstruct...
        assertEquals(0, service1.getNumberOfSubscriptions());

        logger.info("Add some subscriptions to populate the datastore before second service is created...");
        Subscription addedSubscription = service1.addSubscription(createSubscription("Test#1", ET, "ABC", "NSR:Quay:1", "NSR:Quay:2","NSB:Line:Test1", "NSB:Line:Test2"));
        assertNotNull(addedSubscription.getId());
        assertEquals(1, service1.getNumberOfSubscriptions());
        addedSubscription = service1.addSubscription(createSubscription("Test#2", ALL, null, "NSR:Quay:1", "NSR:Quay:2", (String) null));
        assertNotNull(addedSubscription.getId());
        assertEquals(2, service1.getNumberOfSubscriptions());
        addedSubscription = service1.addSubscription(createSubscription("Test#3", ALL, null, null,null, "NSB:Line:Test1"));
        assertNotNull(addedSubscription.getId());
        assertEquals(3, service1.getNumberOfSubscriptions());

        logger.info("Creates second DataStorageService");
        DataStorageService service2 = new DataStorageService(datastore, null, subscriptionTopic);
        service2.logger = LoggerFactory.getLogger(DataStorageService.class.getName()+"#2");
        assertEquals(0, service2.getNumberOfSubscriptions());
        service2.populateSubscriptionCacheFromDatastore(); //postconstruct...
        assertEquals(3, service2.getNumberOfSubscriptions());

        logger.info("Add subscription to second DataStorageService");
        addedSubscription = service2.addSubscription(createSubscription("Test#4", ET, null, null,null, "NSB:Line:Test1"));
        assertNotNull(addedSubscription.getId());
        assertNumberOfSubscriptionsWithWait(4, service2);
        assertNumberOfSubscriptionsWithWait(4, service1);

        logger.info("Update subscription to second DataStorageService");
        addedSubscription.setName("Test#4-updated");
        service1.updateSubscription(addedSubscription);
        assertNumberOfSubscriptionsWithWait(4, service2);
        assertNumberOfSubscriptionsWithWait(4, service1);
        Subscription subFrom1 = getSubscriptionByNameWithWait(service1, "Test#4-updated");
        logger.info("Service1 subscriptions: {}", getAllSubscriptionNames(service1));
        assertNotNull(subFrom1);
        assertEquals(addedSubscription.getId(), subFrom1.getId());
        Subscription subFrom2 = getSubscriptionByNameWithWait(service2, "Test#4-updated");
        logger.info("Service2 subscriptions: {}", getAllSubscriptionNames(service2));
        assertNotNull(subFrom2);
        assertEquals(addedSubscription.getId(), subFrom2.getId());

        logger.info("Deletes subscription from first DataStorageService");
        service2.removeSubscription(addedSubscription.getId());
        assertNumberOfSubscriptionsWithWait(3, service2);
        assertNumberOfSubscriptionsWithWait(3, service1);

        logger.info("Adds 50 subscriptions to each DataStorageService...");
        for (int i=0; i<50; i++) {
            service1.addSubscription(createSubscription("TestManyService1#"+i, SX, null, "NSR:Quay:"+(i+1), "NSR:Quay:"+(i+2), (String) null));
            service2.addSubscription(createSubscription("TestManyService2#"+i, ET, null, "NSR:Quay:"+(i+1), "NSR:Quay:"+(i+2), (String) null));
        }
        assertNumberOfSubscriptionsWithWait(103, service2);
        assertNumberOfSubscriptionsWithWait(103, service1);
    }

    private String getAllSubscriptionNames(DataStorageService service1) {
        return service1.getSubscriptions().stream().map(Subscription::getName).collect(Collectors.joining(", "));
    }

    private Subscription getSubscriptionByNameWithWait(DataStorageService service, String name) throws InterruptedException {
        long startWait = System.currentTimeMillis();
        while (service.getSubscriptionByName(name) == null && (System.currentTimeMillis() - startWait) < 5000) {
            Thread.sleep(1);
        }
        return service.getSubscriptionByName(name);
    }

    private void assertNumberOfSubscriptionsWithWait(int expected, DataStorageService service) throws InterruptedException {
        long startWait = System.currentTimeMillis();
        while (service.getNumberOfSubscriptions() != expected && (System.currentTimeMillis() - startWait) < 5000) {
            Thread.sleep(1);
        }
        assertEquals(expected, service.getNumberOfSubscriptions());
    }

    private Subscription createSubscription(String name, SubscriptionTypeEnum type, String codespace, String fromStopPoint, String toStopPoint, String... linerefs) {
        Subscription subscription = new Subscription();
        subscription.setPushAddress("http://somehost/test");
        subscription.setName(name);
        if (codespace != null) subscription.addCodespace(codespace);
        if (fromStopPoint != null) subscription.addFromStopPoint(fromStopPoint);
        if (toStopPoint != null) subscription.addToStopPoint(toStopPoint);
        if (linerefs != null) {
            for (String lineref : linerefs) {
                subscription.addLineRef(lineref);
            }
        }
        subscription.setType(type);
        return subscription;
    }
}