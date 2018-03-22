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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UkurConfiguration {

    private static final String QUEUE_PREFIX = "ukur";
    public static final String ET_QUEUE = QUEUE_PREFIX + ".et";
    public static final String SX_QUEUE = QUEUE_PREFIX + ".sx";

    @Value("${rutebanken.kubernetes.url:}")
    private String kubernetesUrl;

    @Value("${rutebanken.kubernetes.enabled:true}")
    private boolean kubernetesEnabled;

    @Value("${rutebanken.kubernetes.namespace:default}")
    private String namespace;

    @Value("${rutebanken.hazelcast.management.url:}")
    private String hazelcastManagementUrl;

    @Value("${ukur.camel.anshar.et.url}")
    private String etURL;

    @Value("${ukur.camel.anshar.sx.url}")
    private String sxURL;

    @Value("${ukur.camel.et.polling.enabled}")
    private boolean etPollingEnabled;

    @Value("${ukur.camel.sx.polling.enabled}")
    private boolean sxPollingEnabled;

    @Value("${ukur.camel.rest.port}")
    private int restPort;

    @Value("${ukur.camel.polling.interval}")
    private int pollingInterval;

    @Value("${ukur.camel.tiamat.stop_place_quays.url}")
    private String tiamatStopPlaceQuaysURL;

    @Value("${ukur.camel.tiamat.stop_place_quays.interval}")
    private int tiamatStopPlaceQuaysInterval;

    @Value("${ukur.camel.tiamat.stop_place_quays.enabled}")
    private boolean tiamatStopPlaceQuaysEnabled;

    public String getHazelcastManagementUrl() {
        return hazelcastManagementUrl;
    }

    public String getKubernetesUrl() {
        return kubernetesUrl;
    }

    public boolean isKubernetesEnabled() {
        return kubernetesEnabled;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getAnsharETCamelUrl(String requestorId) {
        return etURL + "?requestorId=" + requestorId + "&maxSize=500";
    }

    public String getAnsharSXCamelUrl(String requestorId) {
        return sxURL + "?requestorId=" + requestorId + "&maxSize=500";
    }

    public boolean isEtPollingEnabled() {
        return etPollingEnabled;
    }

    public boolean isSxPollingEnabled() {
        return sxPollingEnabled;
    }

    public int getRestPort() {
        return restPort;
    }

    public int getPollingInterval() {
        return pollingInterval;
    }

    public String getTiamatStopPlaceQuaysURL() {
        return tiamatStopPlaceQuaysURL;
    }

    public int getTiamatStopPlaceQuaysInterval() {
        return tiamatStopPlaceQuaysInterval;
    }

    public boolean isTiamatStopPlaceQuaysEnabled() {
        return tiamatStopPlaceQuaysEnabled;
    }
}
