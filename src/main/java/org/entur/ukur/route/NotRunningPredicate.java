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
