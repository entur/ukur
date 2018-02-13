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

import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.LineRef;
import uk.org.siri.siri20.PtSituationElement;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class FileStorageService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final File folder;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private final SiriMarshaller siriMarshaller;

    @Autowired
    public FileStorageService(SiriMarshaller siriMarshaller,
                              @Value("${ukur.storage.folder}")String parentFolder ) {
        this.siriMarshaller = siriMarshaller;
        folder = new File(parentFolder);
        //noinspection ResultOfMethodCallIgnored
        folder.mkdirs();
        logger.debug("Will store files to {} (if configured to do so)", folder.getAbsolutePath());
    }

    public void writeToFile(EstimatedVehicleJourney estimatedVehicleJourney) {
        String filename = getPushMessageFilename(estimatedVehicleJourney);
        logger.trace("Writes EstimatedVehicleJourney to file {}", filename);
        String xmlString = toXMLString(estimatedVehicleJourney);
        writeMessageToFile(xmlString, filename);
    }

    public void writeToFile(PtSituationElement ptSituationElement) {
        String filename = getPushMessageFilename(ptSituationElement);
        logger.trace("Writes PtSituationElement to file {}", filename);
        String xmlString = toXMLString(ptSituationElement);
        writeMessageToFile(xmlString, filename);
    }

    private void writeMessageToFile(String xml, String pushMessageFilename) {
        try {
            FileWriter fw = new FileWriter(new File(folder, pushMessageFilename));
            fw.write(xml);
            fw.close();
        } catch (IOException e) {
            logger.error("Could not write pushmessage file", e);
        }
    }

    private String getPushMessageFilename(EstimatedVehicleJourney estimatedVehicleJourney) {
        String vehicleJourney = estimatedVehicleJourney.getVehicleRef() == null ? "null" : estimatedVehicleJourney.getVehicleRef().getValue();
        LineRef lineRef = estimatedVehicleJourney.getLineRef();
        String line = "null";
        if (lineRef != null && StringUtils.containsIgnoreCase(lineRef.getValue(), ":Line:")) {
            line = StringUtils.substringAfterLast(lineRef.getValue(), ":");
        }
        String filename = LocalDateTime.now().format(formatter) + "_ET_" + line + "_" + vehicleJourney + ".xml";
        return filename.replaceAll(" ", "");
    }

    private String getPushMessageFilename(PtSituationElement ptSituationElement) {
        String situationNumber = ptSituationElement.getSituationNumber() == null ? "null" : ptSituationElement.getSituationNumber().getValue();
        return LocalDateTime.now().format(formatter) + "_SX_" + situationNumber + ".xml";
    }

    private String toXMLString(Object element) {
        try {
            return siriMarshaller.marshall(element);
        } catch (JAXBException e) {
            logger.warn("Error marshalling object", e);
            return "ERROR: " + e.getMessage();
        }
    }
}
