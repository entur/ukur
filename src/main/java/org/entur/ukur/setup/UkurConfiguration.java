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

    @Value("${rutebanken.kubernetes.url:}")
    private String kubernetesUrl;

    @Value("${rutebanken.kubernetes.enabled:true}")
    private boolean kubernetesEnabled;

    @Value("${rutebanken.kubernetes.namespace:default}")
    private String namespace;

    @Value("${rutebanken.hazelcast.management.url:}")
    private String hazelcastManagementUrl;

    @Value("${ukur.camel.rest.port:8080}")
    private int restPort;

    @Value("${ukur.camel.tiamat.stop_place_quays.url:http4://tiamat/services/stop_places/list/stop_place_quays/}")
    private String tiamatStopPlaceQuaysURL;

    @Value("${ukur.camel.tiamat.stop_place_quays.interval:3600000}")
    private int tiamatStopPlaceQuaysInterval;

    @Value("${ukur.camel.tiamat.stop_place_quays.enabled:true}")
    private boolean tiamatStopPlaceQuaysEnabled;

    @Value("${ukur.camel.pubsub.et}")
    private String etPubsubQueue;

    @Value("${ukur.camel.pubsub.sx}")
    private String sxPubsubQueue;

    public String getEtPubsubQueue() {
        return etPubsubQueue;
    }

    public String getSxPubsubQueue() {
        return sxPubsubQueue;
    }

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

    public int getRestPort() {
        return restPort;
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
