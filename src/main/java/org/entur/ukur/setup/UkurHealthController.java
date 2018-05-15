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

package org.entur.ukur.setup;

import org.apache.camel.CamelContext;
import org.apache.camel.StartupListener;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.context.annotation.ApplicationScope;

import javax.annotation.PostConstruct;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@ApplicationScope
public class UkurHealthController implements StartupListener {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;
    private final CamelContext camelContext;
    private boolean camelContextStarted = false;

    @Autowired
    public UkurHealthController(QuayAndStopPlaceMappingService quayAndStopPlaceMappingService, CamelContext camelContext) {
        this.quayAndStopPlaceMappingService = quayAndStopPlaceMappingService;
        this.camelContext = camelContext;
    }

    @PostConstruct
    public void configureListener() {
        logger.debug("Registers camel context startup listener");
        try {
            camelContext.addStartupListener(this);
        } catch (Exception e) {
            logger.error("Got error while registering a StartupListener on the camel context", e);
        }
        //TODO: have single periodic async background thread do more extensive checking and have this controllers live-method simply return the last result of that check
    }

    @RequestMapping(value = "/live", method = GET)
    public String live() {
        logger.info("live called");
        if (camelContextStarted) {
            return "OK";
        } else {
            throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "The Camel context has not started yet");
        }
    }

    @RequestMapping(value = "/ready", method = GET)
    public String ready() {
        logger.info("ready called");
        if (quayAndStopPlaceMappingService.getNumberOfStopPlaces() > 0) {
            return "OK";
        } else {
            throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "No quay and stopplace mapping ready yet");
        }
    }

    @Override
    public void onCamelContextStarted(CamelContext context, boolean alreadyStarted) {
        camelContextStarted = true;
        logger.debug("Notified about camel context started (alreadyStarted={})", alreadyStarted);
    }
}
