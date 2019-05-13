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

import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import static org.entur.ukur.subscription.SubscriptionTypeEnum.ET;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionManagerTest {

    @Test
    public void testQuayToStopPlaceMapping() throws JAXBException {
        DataStorageService storageMock = mock(DataStorageService.class);
        QuayAndStopPlaceMappingService mappingMock = mock(QuayAndStopPlaceMappingService.class);
        SubscriptionManager subscriptionManager = new SubscriptionManager(storageMock,
                new SiriMarshaller(), new MetricsService(), new HashMap<>(), new HashMap<>(), mappingMock);

        when(mappingMock.mapQuayToStopPlace("NSR:Quay:1")).thenReturn("NSR:StopPlace:1");
        Subscription s1 = new Subscription();
        s1.setId("s1");
        Subscription s2 = new Subscription();
        s2.setId("s2");
        when(storageMock.getSubscriptionsForStopPoint("NSR:Quay:1", ET)).thenReturn(Collections.singleton(s1));
        when(storageMock.getSubscriptionsForStopPoint("NSR:StopPlace:1", ET)).thenReturn(Collections.singleton(s2));

        Set<Subscription> subscriptionsForStopPlace = subscriptionManager.getSubscriptionsForStopPoint("NSR:StopPlace:1", ET);
        assertEquals(1, subscriptionsForStopPlace.size());
        assertThat(subscriptionsForStopPlace, hasItem(s2));

        Set<Subscription> subscriptionsForQuay = subscriptionManager.getSubscriptionsForStopPoint("NSR:Quay:1", ET);
        assertEquals(2, subscriptionsForQuay.size());
        assertThat(subscriptionsForQuay, hasItem(s1));
        assertThat(subscriptionsForQuay, hasItem(s2));

    }
}