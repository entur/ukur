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

import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.PtSituationElement;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;

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
        String xmlString = toXMLString(estimatedVehicleJourney);
        String[] lineRefArray = StringUtils.split(getStringValue(estimatedVehicleJourney.getLineRef()), ':');
        String subfolder;
        if (lineRefArray == null || lineRefArray.length < 1 || StringUtils.isBlank(lineRefArray[0]) || "-".equals(lineRefArray[0])) {
            subfolder = "empty";
        } else {
            subfolder = StringUtils.lowerCase(lineRefArray[0]);
        }
        logger.trace("Writes EstimatedVehicleJourney to file {} in subfolder {}", filename, subfolder);
        writeMessageToFile(xmlString, subfolder, filename);
    }

    public void writeToFile(PtSituationElement ptSituationElement) {
        String filename = getPushMessageFilename(ptSituationElement);
        String xmlString = toXMLString(ptSituationElement);
        String subfolder = StringUtils.lowerCase(getStringValue(ptSituationElement.getParticipantRef()));
        if (StringUtils.isBlank(subfolder) || "-".equals(subfolder)) {
            subfolder = "empty";
        }
        logger.trace("Writes PtSituationElement to file {} in subfolder {}", filename, subfolder);
        writeMessageToFile(xmlString, subfolder, filename);
    }

    private void writeMessageToFile(String xml, String subfolder, String pushMessageFilename) {
        try {
            File targetFolder = new File(folder, subfolder);
            //noinspection ResultOfMethodCallIgnored
            targetFolder.mkdir();
            FileWriter fw = new FileWriter(new File(targetFolder, pushMessageFilename));
            fw.write(xml);
            fw.close();
        } catch (IOException e) {
            logger.error("Could not write pushmessage file", e);
        }
    }

    private String getPushMessageFilename(EstimatedVehicleJourney estimatedVehicleJourney) {
        String vehicleJourney = getStringValue(estimatedVehicleJourney.getVehicleRef());
        String line = getStringValue(estimatedVehicleJourney.getLineRef());
        if (StringUtils.containsIgnoreCase(line, ":Line:")) {
            line = StringUtils.substringAfterLast(line, ":");
        }
        String filename = LocalDateTime.now().format(formatter) + "_ET_" + line + "_" + vehicleJourney + ".xml";
        return filename.replaceAll(" ", "");
    }

    private String getPushMessageFilename(PtSituationElement ptSituationElement) {
        String situationNumber = getStringValue(ptSituationElement.getSituationNumber());
        if (StringUtils.contains(situationNumber, ":")) {
            situationNumber = StringUtils.substringAfterLast(situationNumber, ":");
        }
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
