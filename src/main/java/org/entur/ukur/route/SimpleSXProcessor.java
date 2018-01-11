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

import org.entur.ukur.xml.SiriObjectToFileWriter;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.rutebanken.siri20.util.SiriXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.siri.siri20.*;

import java.io.File;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;

class SimpleSXProcessor implements Processor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private HashSet<String> uniqueSituations = new HashSet<>();
    private SiriObjectToFileWriter writer;


    public SimpleSXProcessor(String outfolder) {
        File sxFolder = new File(outfolder, "sx");
        writer = new SiriObjectToFileWriter(sxFolder);
        logger.info("Writes xml files to {}", sxFolder.getAbsolutePath());
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        InputStream xml = exchange.getIn().getBody(InputStream.class);
        logger.debug("Reveived XML with size {} bytes", String.format("%,d", xml.available()));
        Siri siri = SiriXml.parseXml(xml);
        if (siri == null || siri.getServiceDelivery() == null) {
            throw new IllegalArgumentException("No ServiceDelivery element...");
        }
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        exchange.getIn().setHeader(TestRoute.MORE_DATA_HEADER, serviceDelivery.isMoreData());
        List<SituationExchangeDeliveryStructure> situationExchangeDeliveries = serviceDelivery.getSituationExchangeDeliveries();
        logger.debug(".situationExchangeDeliveries: {}", situationExchangeDeliveries.size());
        HashSet<String> situationsInThisExchange = new HashSet<>();
        for (SituationExchangeDeliveryStructure situationExchangeDelivery : situationExchangeDeliveries) {
            SituationExchangeDeliveryStructure.Situations situations = situationExchangeDelivery.getSituations();
            List<PtSituationElement> ptSituationElements = situations.getPtSituationElements();
            logger.debug("..situation.ptSituationElements: {}", ptSituationElements.size());
            for (PtSituationElement ptSituationElement : ptSituationElements) {
                SituationNumber situationNumber = ptSituationElement.getSituationNumber();
                String name;
                if (situationNumber != null) {
                    name = situationNumber.getValue();
                    situationsInThisExchange.add(name);
                } else {
                    name = "null";
                }
                logger.debug("PtSituationElement: {}, ", name);
                writer.printToFile(ptSituationElement, name);
            }
        }
        uniqueSituations.addAll(situationsInThisExchange);
        logger.debug("SX data summary:");
        logger.debug("  unique situations ..............: {}", uniqueSituations.size());
        logger.debug("  situations in this exchange ....: {}", situationsInThisExchange.size());
    }

}

