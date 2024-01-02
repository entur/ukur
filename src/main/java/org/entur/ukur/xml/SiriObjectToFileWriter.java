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

package org.entur.ukur.xml;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.siri.siri21.Siri;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SiriObjectToFileWriter {

    private JAXBContext jaxbContext;
    private File parentFolder;
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public SiriObjectToFileWriter(File parentFolder) {
        try {
            jaxbContext = JAXBContext.newInstance(Siri.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Problem initializing JAXBContext", e);
        }
        this.parentFolder = parentFolder;
        parentFolder.mkdirs();
    }

    public void printToFile(Object element, String name) {
        try {
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            String filename = name.replaceAll("[^a-zA-Z0-9.-]", "_") + ".xml";
            FileWriter fileWriter = new FileWriter(new File(parentFolder, filename));
            XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(fileWriter);
            jaxbMarshaller.marshal(element, new NoNamespaceIndentingXMLStreamWriter(writer));
            fileWriter.close();
        } catch (JAXBException|IOException |XMLStreamException e) {
            logger.warn("Error writing to file", e);
        }
    }

}
