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

class SimpleETProcessor implements Processor {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private HashSet<String> uniqueDatedVehicleJourneyRef = new HashSet<>();
    private HashSet<String> uniqueLines = new HashSet<>();
    private HashSet<String> uniqueLineDirections = new HashSet<>();
    private SiriObjectToFileWriter writer;


    public SimpleETProcessor(String outfolder) {
        File sxFolder = new File(outfolder, "et");
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
        List<EstimatedTimetableDeliveryStructure> estimatedTimetableDeliveries = serviceDelivery.getEstimatedTimetableDeliveries();
        exchange.getIn().setHeader(TestRoute.MORE_DATA_HEADER, serviceDelivery.isMoreData());
//            logger.debug("estimatedTimetableDeliveries: {} (isMoreData={})", estimatedTimetableDeliveries.size(), serviceDelivery.isMoreData());
        for (EstimatedTimetableDeliveryStructure estimatedTimetableDelivery : estimatedTimetableDeliveries) {
            List<EstimatedVersionFrameStructure> estimatedJourneyVersionFrames = estimatedTimetableDelivery.getEstimatedJourneyVersionFrames();
//                logger.debug(".estimatedJourneyVersionFrames: {}", estimatedJourneyVersionFrames.size());
            for (EstimatedVersionFrameStructure estimatedJourneyVersionFrame : estimatedJourneyVersionFrames) {
                List<EstimatedVehicleJourney> estimatedVehicleJourneies = estimatedJourneyVersionFrame.getEstimatedVehicleJourneies();
//                    logger.debug("..estimatedVehicleJourneies: {}", estimatedVehicleJourneies.size());
                for (EstimatedVehicleJourney estimatedVehicleJourney : estimatedVehicleJourneies) {

                    StringBuilder filenameBuilder = new StringBuilder();
                    LineRef lineRef = estimatedVehicleJourney.getLineRef();
                    if (lineRef != null) {
                        uniqueLines.add(lineRef.getValue());
                        filenameBuilder.append(lineRef.getValue());
                        DirectionRefStructure directionRef = estimatedVehicleJourney.getDirectionRef();
                        if (directionRef != null) {
                            uniqueLineDirections.add(lineRef.getValue() + "-" + directionRef.getValue());
                            filenameBuilder.append("-").append(directionRef.getValue());
                        }
                    }
                    DatedVehicleJourneyRef datedVehicleJourneyRef = estimatedVehicleJourney.getDatedVehicleJourneyRef();
                    if (datedVehicleJourneyRef != null) {
                        uniqueDatedVehicleJourneyRef.add(datedVehicleJourneyRef.getValue());
                        filenameBuilder.append("-").append(datedVehicleJourneyRef.getValue());
                    }

                    writer.printToFile(estimatedVehicleJourney, filenameBuilder.toString());

                    //TODO: Få inn et tidselement så gamle data kan fjernes (validity/ankomst->variant av expiring map)
                    //TODO: estimatedVehicleJourney.getEstimatedCalls()
                }
            }
        }
        logger.debug("ET data summary:");
        logger.debug("  unique Line..................: {}", uniqueLines.size());
        logger.debug("  unique DatedVehicleJourney...: {}", uniqueDatedVehicleJourneyRef.size());
        logger.debug("  unique LineRef og Direction..: {}", uniqueLineDirections.size());
    }
}
