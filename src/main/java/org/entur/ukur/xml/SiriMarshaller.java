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
import jakarta.xml.bind.Unmarshaller;
import org.springframework.stereotype.Component;
import uk.org.siri.siri21.Siri;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;

@Component
public class SiriMarshaller {

    private final JAXBContext jaxbContext;

    public SiriMarshaller() throws JAXBException {
        jaxbContext = JAXBContext.newInstance(Siri.class);
    }

    public <T> T unmarshall(String xml, Class<T>resultingClass) throws JAXBException, XMLStreamException {
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        XMLInputFactory xmlif = XMLInputFactory.newInstance();
        XMLStreamReader xmlsr = xmlif.createXMLStreamReader(new StringReader(xml));
        return resultingClass.cast(jaxbUnmarshaller.unmarshal(xmlsr));
    }

    public String prettyPrintNoNamespaces(Object element) throws JAXBException, XMLStreamException {
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        jaxbMarshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
        StringWriter stringWriter = new StringWriter();
        XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(stringWriter);
        jaxbMarshaller.marshal(element, new NoNamespaceIndentingXMLStreamWriter(writer));
        writer.close();
        return stringWriter.getBuffer().toString();
    }

    public String marshall(Object element) throws JAXBException {
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        StringWriter stringWriter = new StringWriter();
        jaxbMarshaller.marshal(element, stringWriter);
        return stringWriter.getBuffer().toString();
    }

}
