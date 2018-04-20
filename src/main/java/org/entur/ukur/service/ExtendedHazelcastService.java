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
import com.hazelcast.core.*;
import org.entur.ukur.routedata.LiveJourney;
import org.entur.ukur.setup.UkurConfiguration;
import org.rutebanken.hazelcasthelper.service.HazelCastService;
import org.rutebanken.hazelcasthelper.service.KubernetesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

@Service
public class ExtendedHazelcastService extends HazelCastService {

    private static final String NODE_NAME_SETTER_LOCK = "nodeNameSetter";
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
            Member localMember = cluster.getLocalMember();
            if (localMember == null || localMember.getAddress() == null) {
                logger.warn("Does not have localMember's address - returns node0");
                return "node0";
            }
            String localhost = localMember.getAddress().getHost();
            IMap<String, String> sharedProperties = sharedProperties();
            String localNameKey = getNodeNameKey(localhost);
            String localNodeNumber = sharedProperties.get(localNameKey);
            if (localNodeNumber != null) {
                logger.debug("localMember already has a nodeNumber: {}", localNodeNumber);
                return localNodeNumber;
            } else {
                hazelcast.getLifecycleService().addLifecycleListener(event -> {
                    if (SHUTTING_DOWN.equals(event.getState())) {
                        sharedProperties().remove(localNameKey);
                        logger.info("Removed value for nodeNameKey = {} from sharedProperties", localNameKey);
                    }
                });
            }

            ArrayList<String> takenNodeNumbers = getTakenNodeNames(cluster, sharedProperties);
            for (int i = 0; i < 1000; i++) {
                String nameToCheck = "node" + i;
                if (!takenNodeNumbers.contains(nameToCheck)) {
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

            //TODO: add some mechanism to clean up old node names (the LifecycleListener is not guaranteed to run/succeed)
        }
        logger.debug("Not member of any hazelcast cluster - returns node0");
        return "node0";
    }

    private String getNodeNameKey(String host) {
        return "nodenumber." + host;
    }

    private ArrayList<String> getTakenNodeNames(Cluster cluster, IMap<String, String> sharedProperties) {
        ArrayList<String> taken = new ArrayList<>();
        Set<Member> members = cluster.getMembers(); //ordered by age - oldest first
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
}
