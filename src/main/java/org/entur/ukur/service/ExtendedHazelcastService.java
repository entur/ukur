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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.setup.UkurConfiguration;
import org.rutebanken.hazelcasthelper.service.HazelCastService;
import org.rutebanken.hazelcasthelper.service.KubernetesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

@Service
public class ExtendedHazelcastService extends HazelCastService {

    private static final String NODE_NAME_SETTER_LOCK = "nodeNameSetter";
    static final String NODENUMBER_PREFIX = "nodenumber.";
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public ExtendedHazelcastService(@Autowired KubernetesService kubernetesService, @Autowired UkurConfiguration cfg) {
        super(kubernetesService, cfg.getHazelcastManagementUrl());
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }

    @Bean
    public Map<Object, Long> alreadySentCache() {
        return hazelcast.getMap("ukur.alreadySentCache");
    }

    @Bean
    public IMap<String, String> sharedProperties() {
        return hazelcast.getMap("ukur.sharedProperties");
    }

    @Bean
    public IMap<String, LiveJourney> currentJourneys() {
        return hazelcast.getMap("ukur.currentJourneys");
    }

    @Override
    public List<MapConfig> getAdditionalMapConfigurations() {
        List<MapConfig> mapConfigs = super.getAdditionalMapConfigurations();

        mapConfigs.add(
                new MapConfig()
                        .setName("ukur.alreadySentCache")
                        .setMaxIdleSeconds(3600));
        return mapConfigs;

    }

    /**
     * Calculates the node name of the current hazelcast member. The idea is that there always
     * is a node0, node1 etc. But a given "nodeN" can change over time, as one node goes down
     * a new one will take its place. A members node-name will never change during its lifetime,
     * so there can be gaps in the node lists.
     */
    public String getMyNodeName() {
        Cluster cluster = hazelcast.getCluster();
        if (cluster != null) {
            IMap<String, String> sharedProperties = sharedProperties();
            //collect these before we continue to avoid us accidentally remove the key of a node starting:
            HashSet<String> nodenameKeys = sharedProperties.keySet().stream().filter(s -> s.startsWith(NODENUMBER_PREFIX)).collect(Collectors.toCollection(HashSet::new));
            try {
                Member localMember = cluster.getLocalMember();
                if (localMember == null || localMember.getAddress() == null) {
                    logger.warn("Does not have localMember's address - returns hardcoded node0");
                    return "node0";
                }
                String localhost = localMember.getAddress().getHost();

                String localNameKey = getNodeNameKey(localhost);
                String localNodeName = sharedProperties.get(localNameKey);
                if (localNodeName != null) {
                    logger.debug("localMember already has a node name: {}", localNodeName);
                    return localNodeName;
                } else {
                    hazelcast.getLifecycleService().addLifecycleListener(event -> {
                        if (SHUTTING_DOWN.equals(event.getState())) {
                            sharedProperties().remove(localNameKey);
                            logger.info("Hazelcast LifecycleListener: Removed value for nodeNameKey = {} from sharedProperties", localNameKey);
                        }
                    });
                }

                ArrayList<String> takenNodeNames = getTakenNodeNames(cluster, sharedProperties);
                for (int i = 0; i < 1000; i++) {
                    String nameToCheck = "node" + i;
                    if (!takenNodeNames.contains(nameToCheck)) {
                        logger.debug("Attempts to allocate node number '{}'", nameToCheck);
                        if (sharedProperties.tryLock(NODE_NAME_SETTER_LOCK)) {
                            try {
                                sharedProperties.put(localNameKey, nameToCheck);
                                List<String> sameNames = getTakenNodeNames(cluster, sharedProperties).stream().filter(s -> s.equals(nameToCheck)).collect(Collectors.toList());
                                if (sameNames.size() > 1) {
                                    logger.warn("Some other node has already taken the name '{}' - this node finds a new name", nameToCheck);
                                    sharedProperties.remove(localNameKey);
                                } else {
                                    logger.info("Returns node name {}", nameToCheck);
                                    return nameToCheck;
                                }
                            } finally {
                                sharedProperties.unlock(NODE_NAME_SETTER_LOCK);
                            }
                        } else {
                            logger.warn("Did not get lock - runs method again");
                            return getMyNodeName();
                        }
                    }
                }
            } finally {
                cleanOldNodeNames(cluster, nodenameKeys, sharedProperties);
            }

        }
        logger.warn("Not member of any hazelcast cluster - returns hardcoded node0");
        return "node0";
    }

    private String getNodeNameKey(String host) {
        return NODENUMBER_PREFIX + host;
    }

    private ArrayList<String> getTakenNodeNames(Cluster cluster, IMap<String, String> sharedProperties) {
        ArrayList<String> taken = new ArrayList<>();
        Set<Member> members = cluster.getMembers();
        Member localMember = cluster.getLocalMember();
        if (members != null) {
            for (Member member : members) {
                String host = member.getAddress().getHost();
                String hostNodeName = sharedProperties.get(getNodeNameKey(host));
                if (hostNodeName == null) {
                    if (!localMember.equals(member)) {
                        logger.info("Other member ({}) also lack node name", host);
                    }
                } else {
                    logger.debug("member '{}' has node name '{}'", host, hostNodeName);
                    taken.add(hostNodeName);
                }
            }
        }
        return taken;
    }

    private void cleanOldNodeNames(Cluster cluster, Set<String> nodenameKeys, IMap<String, String> sharedProperties) {
        Set<Member> members = cluster.getMembers();
        if (members != null) {
            for (Member member : members) {
                String host = member.getAddress().getHost();
                nodenameKeys.remove(getNodeNameKey(host));
            }
            if (nodenameKeys.isEmpty()) {
                logger.debug("No unused node names in shared properties");
            } else {
                logger.info("There are {} unused node names in shared properties - removes these keys: {}", nodenameKeys.size(), nodenameKeys);
                for (String key : nodenameKeys) {
                    sharedProperties.remove(key);
                }
            }
        }

    }
}
