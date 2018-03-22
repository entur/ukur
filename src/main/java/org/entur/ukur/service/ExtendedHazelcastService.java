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

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.subscription.Subscription;
import org.rutebanken.hazelcasthelper.service.HazelCastService;
import org.rutebanken.hazelcasthelper.service.KubernetesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class ExtendedHazelcastService extends HazelCastService {

    /** Evict cache when free heap percentage is below this value */
    private static final int EVICT_WHEN_FREE_HEAP_PERCENTAGE_BELOW = 25;

    public ExtendedHazelcastService(@Autowired KubernetesService kubernetesService, @Autowired UkurConfiguration cfg) {
        super(kubernetesService, cfg.getHazelcastManagementUrl());
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }

    public IMap<String, Set<String>> subscriptionIdsPerStopPoint() {
        return hazelcast.getMap("ukur.subscriptionIdsPerStop");
    }

    public IMap<String, Subscription> subscriptions() {
        return hazelcast.getMap("ukur.subscriptions");
    }

    @Bean
    public IMap<Object, Long> alreadySentCache() {
        return hazelcast.getMap("ukur.alreadySentCache");
    }

    @Bean
    public IMap<String, String> sharedProperties() {
        return hazelcast.getMap("ukur.sharedProperties");
    }

    public IMap<String, LiveJourney> currentJourneys() {
        return hazelcast.getMap("ukur.currentJourneys");
    }

    public Map<String, Collection<String>> stopPlaceIdToQaysId() {
        return hazelcast.getMap("ukur.stopPlaceIdToQaysId");
    }

    public Map<String, String> quayIdToStopPlaceId() {
        return hazelcast.getMap("ukur.quayIdToStopPlaceId");
    }

    @Override
    public List<MapConfig> getAdditionalMapConfigurations() {
        List<MapConfig> mapConfigs = super.getAdditionalMapConfigurations();

        mapConfigs.add(
                new MapConfig()
                        .setName("ukur.alreadySentCache")
                        .setMaxIdleSeconds(300)
                        .setEvictionPolicy(EvictionPolicy.LFU)
                        .setMaxSizeConfig(
                                new MaxSizeConfig(EVICT_WHEN_FREE_HEAP_PERCENTAGE_BELOW, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE)));

        return mapConfigs;

    }
}
