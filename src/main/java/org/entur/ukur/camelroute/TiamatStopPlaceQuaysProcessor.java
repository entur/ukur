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

package org.entur.ukur.camelroute;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Service
public class TiamatStopPlaceQuaysProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private QuayAndStopPlaceMappingService dataStorageService;
    boolean hasRun = false;

    public TiamatStopPlaceQuaysProcessor(QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {
        this.dataStorageService = quayAndStopPlaceMappingService;
    }

    @Override
    public void process(Exchange exchange) throws IOException {
        readFileFromInputStream(exchange.getIn().getBody(InputStream.class));
    }

    void readFileFromInputStream(InputStream json) throws IOException {
        logger.debug("Received inputstream with size {} bytes", String.format("%,d", json.available()));

        ObjectMapper mapper = new ObjectMapper();
        //noinspection unchecked
        HashMap<String, Collection<String>> result = mapper.readValue(json, HashMap.class);
        logger.info("Got {} stopplaces: ", result.size());
        if (logger.isTraceEnabled()) {
            for (Map.Entry<String, Collection<String>> entry : result.entrySet()) {
                logger.trace(" {} -> {}", entry.getKey(), entry.getValue());
            }
        }
        dataStorageService.updateStopsAndQuaysMap(result);
        hasRun = true;
    }

    boolean hasRun() {
        return hasRun;
    }
}
