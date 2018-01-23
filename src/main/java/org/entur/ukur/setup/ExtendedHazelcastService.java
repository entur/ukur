package org.entur.ukur.setup;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.entur.ukur.subscription.PushMessage;
import org.entur.ukur.subscription.Subscription;
import org.rutebanken.hazelcasthelper.service.HazelCastService;
import org.rutebanken.hazelcasthelper.service.KubernetesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

@Service
public class ExtendedHazelcastService extends HazelCastService {

    /**
     * Evict cache when free heap percentage is below this value
     */
    private static final int EVICT_WHEN_FREE_HEAP_PERCENTAGE_BELOW = 25;

    public ExtendedHazelcastService(@Autowired KubernetesService kubernetesService, @Autowired UkurConfiguration cfg) {
        super(kubernetesService, cfg.getHazelcastManagementUrl());
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }

    @Bean
    public IMap<String, Set<Subscription>> subscriptionsPerStopPoint() {
        return hazelcast.getMap("ukur.subscriptionsPerStop");
    }

    @Bean
    public IMap<String, List<PushMessage>> pushMessagesMemoryStore() {
        //TODO: Denne skal fjernes når vi får på plass skikkelig push over http!
        return hazelcast.getMap("ukur.pushMessagesMemoryStore");
    }

    @Bean
    public IMap<String, Long> alreadySentCache() {
        return hazelcast.getMap("ukur.alreadySentCache");
    }

    @Bean
    public IMap<String, String> sharedProperties() {
        return hazelcast.getMap("ukur.sharedProperties");
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
