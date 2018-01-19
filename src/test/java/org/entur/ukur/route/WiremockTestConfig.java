package org.entur.ukur.route;

import org.entur.ukur.setup.UkurConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.UUID;

@Configuration
@Primary
public class WiremockTestConfig extends UkurConfiguration {

    @Value("${wiremock.server.port}")
    private String wiremockPort;

    @Override
    public String getAnsharETCamelUrl(UUID uuid) {
        return "http4://localhost:" + wiremockPort + "/et";
    }

    @Override
    public String getAnsharSXCamelUrl(UUID uuid) {
        return "http4://localhost:" + wiremockPort + "/sx";
    }

    @Override
    public boolean isQuartzRoutesEnabled() {
        return false;
    }
}
