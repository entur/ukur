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

import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.subscription.Subscription;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Set;

public interface DataStorageService {

    Collection<Subscription> getSubscriptions();
    Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef);
    Set<Subscription> getSubscriptionsForLineRefAndNoStops(String lineRef);
    Set<Subscription> getSubscriptionsForvehicleRefAndNoStops(String vehicleJourneyRef);
    Subscription addSubscription(Subscription subscription);
    void removeSubscription(String subscriptionId);
    void updateSubscription(Subscription subscription);
    long getNumberOfSubscriptions();

    void putCurrentJourney(LiveJourney liveJourney);
    Collection<LiveJourney> getCurrentJourneys();
    int getNumberOfCurrentJourneys();
    void removeJourneysOlderThan(ZonedDateTime now);

}
