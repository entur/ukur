package org.entur.ukur.route;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotRunningPredicate implements Predicate {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String routeId;

    public NotRunningPredicate(String routeId) {
        this.routeId = routeId;
    }

    @Override
    public boolean matches(Exchange exchange) {
        int size = exchange.getContext().getInflightRepository().size(routeId);
        boolean notRunning = size == 0;
        logger.debug("Number of running instances of route '{}' is {} - returns {}", routeId, size, notRunning);
        return notRunning;
    }
}
