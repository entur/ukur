package org.entur.ukur.route;

import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spring.SpringRouteBuilder;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.setup.policy.InterruptibleHazelcastRoutePolicy;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.entur.ukur.setup.policy.SingletonRoutePolicyFactory.SINGLETON_ROUTE_DEFINITION_GROUP_NAME;

/**
 * Defines common route behavior.
 */
public abstract class AbstractClusterRouteBuilder extends SpringRouteBuilder {


    protected UkurConfiguration config;

    protected AbstractClusterRouteBuilder(UkurConfiguration config) {
        this.config = config;
    }

    @Override
    public void configure() {
        errorHandler(transactionErrorHandler()
                .logExhausted(true)
                .logRetryStackTrace(true));
    }
    /**
     * Create a new singleton route definition from URI. Only one such route should be active throughout the cluster at any time.
     */
    protected RouteDefinition singletonFrom(String uri, String routeId) {
        return this.from(uri)
                .group(SINGLETON_ROUTE_DEFINITION_GROUP_NAME)
                .routeId(routeId)
                .autoStartup(true);
    }


    protected boolean isLeader(String routeId) {
        RouteContext routeContext = getContext().getRoute(routeId).getRouteContext();
        List<RoutePolicy> routePolicyList = routeContext.getRoutePolicyList();
        if (routePolicyList != null) {
            for (RoutePolicy routePolicy : routePolicyList) {
                if (routePolicy instanceof InterruptibleHazelcastRoutePolicy) {
                    return ((InterruptibleHazelcastRoutePolicy) (routePolicy)).isLeader();
                }
            }
        }
        return false;
    }

    protected void releaseLeadership(String routeId) {
        RouteContext routeContext = getContext().getRoute(routeId).getRouteContext();
        List<RoutePolicy> routePolicyList = routeContext.getRoutePolicyList();
        if (routePolicyList != null) {
            for (RoutePolicy routePolicy : routePolicyList) {
                if (routePolicy instanceof InterruptibleHazelcastRoutePolicy) {
                    ((InterruptibleHazelcastRoutePolicy) routePolicy).releaseLeadership();
                    log.info("Leadership released: {}", routeId);
                }
            }
        }
    }

}
