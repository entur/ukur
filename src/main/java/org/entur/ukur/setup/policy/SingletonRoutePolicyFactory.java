package org.entur.ukur.setup.policy;

import org.apache.camel.CamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spi.RoutePolicyFactory;
import org.entur.ukur.setup.ExtendedHazelcastService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * Create policies for enforcing that routes are started as singleton, using Hazelcast for cluster  sync.
 */
@Service
public class SingletonRoutePolicyFactory implements RoutePolicyFactory {

    public static final String SINGLETON_ROUTE_DEFINITION_GROUP_NAME = "UkurSingletonRoute";
    private static final Logger log = LoggerFactory.getLogger(SingletonRoutePolicyFactory.class);

    @Autowired
    ExtendedHazelcastService hazelcastService;

    /**
     * Create policy ensuring only one route with 'key' is started in cluster.
     */
    private RoutePolicy build(String key) {
        InterruptibleHazelcastRoutePolicy hazelcastRoutePolicy = new InterruptibleHazelcastRoutePolicy(hazelcastService.getHazelcastInstance());
        hazelcastRoutePolicy.setLockMapName("ukurRouteLockMap");
        hazelcastRoutePolicy.setLockKey(key);
        hazelcastRoutePolicy.setLockValue("lockValue");
        hazelcastRoutePolicy.setShouldStopConsumer(false);

        log.info("RoutePolicy: Created HazelcastPolicy for key {}", key);
        return hazelcastRoutePolicy;
    }

    @Override
    public RoutePolicy createRoutePolicy(CamelContext camelContext, String routeId, RouteDefinition routeDefinition) {
        try {
            if (SINGLETON_ROUTE_DEFINITION_GROUP_NAME.equals(routeDefinition.getGroup())) {
                return build(routeId);
            }
        } catch (Exception e) {
            log.warn("Failed to create singleton route policy", e);
        }
        return null;
    }


}
