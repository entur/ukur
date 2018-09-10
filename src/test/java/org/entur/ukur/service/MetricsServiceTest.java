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

import com.codahale.metrics.Histogram;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

import static org.junit.Assert.*;

public class MetricsServiceTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testHistogramWithTextDelay() {
        MetricsService ms = new MetricsService();

        SortedMap<String, Histogram> histograms = ms.getHistograms();
        assertTrue(histograms.isEmpty());

        ms.registerMessageDelay("test", "2018-01-17T10:18:51.537+01:00");
        histograms = ms.getHistograms();
        assertFalse(histograms.isEmpty());
        assertTrue(histograms.containsKey("test"));
        Histogram histogram = histograms.get("test");
        assertEquals(1, histogram.getCount());
        long max = histogram.getSnapshot().getMax();
        logger.info("max = {}", max);
        assertTrue(16070400000L < max); //minimum 6 months ago

    }
}