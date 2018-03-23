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

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TiamatStopPlaceQuaysProcessorTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @SuppressWarnings("unchecked")
    @Test
    public void name() throws IOException {
        String json =
                "{\n" +
                "  \"NSR:StopPlace:26291\": [\n" +
                "    \"NSR:Quay:45505\"\n" +
                "  ],\n" +
                "  \"NSR:StopPlace:26292\": [\n" +
                "    \"NSR:Quay:45506\",\n" +
                "    \"NSR:Quay:45507\"\n" +
                "  ],\n" +
                "  \"NSR:StopPlace:26293\": [\n" +
                "    \"NSR:Quay:45509\",\n" +
                "    \"NSR:Quay:45508\"\n" +
                "  ],\n" +
                "  \"NSR:StopPlace:26294\": [\n" +
                "    \"NSR:Quay:45510\",\n" +
                "    \"NSR:Quay:45511\",\n" +
                "    \"NSR:Quay:45512\",\n" +
                "    \"NSR:Quay:45513\",\n" +
                "    \"NSR:Quay:45514\"\n" +
                "  ]\n" +
                "}";

        InputStream stream = IOUtils.toInputStream(json, "UTF-8");
        QuayAndStopPlaceMappingService quayAndStopPlaceMappingService = new QuayAndStopPlaceMappingService(new MetricsService(null, 0));
        TiamatStopPlaceQuaysProcessor processor = new TiamatStopPlaceQuaysProcessor(quayAndStopPlaceMappingService);
        Exchange exchangeMock = createExchangeMock(stream);

        assertEquals(0, quayAndStopPlaceMappingService.getNumberOfStopPlaces());
        processor.process(exchangeMock);
        assertEquals(4, quayAndStopPlaceMappingService.getNumberOfStopPlaces());

        assertThat(quayAndStopPlaceMappingService.mapStopPlaceToQuays("NSR:StopPlace:26291"), hasItems("NSR:Quay:45505"));
        assertThat(quayAndStopPlaceMappingService.mapStopPlaceToQuays("NSR:StopPlace:26292"), hasItems("NSR:Quay:45506", "NSR:Quay:45507"));
        assertThat(quayAndStopPlaceMappingService.mapStopPlaceToQuays("NSR:StopPlace:26293"), hasItems("NSR:Quay:45509", "NSR:Quay:45508"));
        assertThat(quayAndStopPlaceMappingService.mapStopPlaceToQuays("NSR:StopPlace:26294"), hasItems("NSR:Quay:45510", "NSR:Quay:45511", "NSR:Quay:45512", "NSR:Quay:45513", "NSR:Quay:45514"));
        assertEquals("NSR:StopPlace:26291", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45505"));
        assertEquals("NSR:StopPlace:26292", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45506"));
        assertEquals("NSR:StopPlace:26292", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45507"));
        assertEquals("NSR:StopPlace:26293", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45509"));
        assertEquals("NSR:StopPlace:26293", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45508"));
        assertEquals("NSR:StopPlace:26294", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45510"));
        assertEquals("NSR:StopPlace:26294", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45511"));
        assertEquals("NSR:StopPlace:26294", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45512"));
        assertEquals("NSR:StopPlace:26294", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45513"));
        assertEquals("NSR:StopPlace:26294", quayAndStopPlaceMappingService.mapQuayToStopPlace("NSR:Quay:45514"));
    }

    @SuppressWarnings("unchecked")
    @Test
    @Ignore
    public void verifyActualTiamatData() throws IOException {
        QuayAndStopPlaceMappingService quayAndStopPlaceMappingService = new QuayAndStopPlaceMappingService(new MetricsService(null, 0));
        TiamatStopPlaceQuaysProcessor processor = new TiamatStopPlaceQuaysProcessor(quayAndStopPlaceMappingService);
        Exchange exchangeMock = createExchangeMock(new FileInputStream("/home/jon/Documents/Entur/StopPlacesAndQuays.json"));

        assertEquals(0, quayAndStopPlaceMappingService.getNumberOfStopPlaces());
        processor.process(exchangeMock);
        assertTrue( quayAndStopPlaceMappingService.getNumberOfStopPlaces() > 10000);


        HashMap<String, Collection<String>> captured = quayAndStopPlaceMappingService.getAllStopPlaces();
        assertTrue( captured.size() > 10000);

        logger.info("Got {} different stopplaces", captured.size());
        boolean foundQuayOnSeveralStops = false;
        HashSet<String> uniqueQuays = new HashSet<>();
        for (Collection<String> quayIds : captured.values()) {
            for (String id : quayIds) {
                if (!uniqueQuays.add(id)) { {
                    foundQuayOnSeveralStops = true;
                    logger.warn("QuayId {} is already found", id);
                }}
            }
        }
        logger.info("and {} unique quays", uniqueQuays.size());
        assertFalse("Did not expect same quay on several stops", foundQuayOnSeveralStops);

    }

    private Exchange createExchangeMock(InputStream stream) {
        Exchange exchangeMock = mock(Exchange.class);
        Message messageMock = mock(Message.class);
        when(exchangeMock.getIn()).thenReturn(messageMock);
        when(messageMock.getBody(InputStream.class)).thenReturn(stream);
        return exchangeMock;
    }
}