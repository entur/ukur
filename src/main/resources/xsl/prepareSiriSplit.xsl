<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:siri="http://www.siri.org.uk/siri"
                xmlns:ns2="http://www.ifopt.org.uk/acsb"
                version="1.0">

    <xsl:output method="xml" indent="yes" />

    <xsl:template match="/">
        <root>
            <xsl:call-template name="Siri" />
        </root>
    </xsl:template>

    <xsl:template name="Siri" match="/siri:Siri">

        <xsl:for-each select="/siri:Siri/siri:ServiceDelivery/siri:EstimatedTimetableDelivery/siri:EstimatedJourneyVersionFrame/siri:EstimatedVehicleJourney[not(ns2:ServiceFeatureRef/text()='freightTrain')]">
            <siri:Siri>
                <siri:ServiceDelivery>
                    <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:ResponseTimestamp"></xsl:copy-of>
                    <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:ProducerRef"></xsl:copy-of>
                    <siri:EstimatedTimetableDelivery>
                        <siri:EstimatedJourneyVersionFrame>
                            <xsl:copy-of select="."></xsl:copy-of>
                        </siri:EstimatedJourneyVersionFrame>
                    </siri:EstimatedTimetableDelivery>
                </siri:ServiceDelivery>
            </siri:Siri>
        </xsl:for-each>

        <xsl:for-each select="/siri:Siri/siri:ServiceDelivery/siri:SituationExchangeDelivery/siri:Situations/siri:PtSituationElement">
            <siri:Siri>
                <siri:ServiceDelivery>
                    <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:ResponseTimestamp"></xsl:copy-of>
                    <xsl:copy-of select="/siri:Siri/siri:ServiceDelivery/siri:ProducerRef"></xsl:copy-of>
                    <siri:SituationExchangeDelivery>
                        <siri:Situations>
                            <xsl:copy-of select="."></xsl:copy-of>
                        </siri:Situations>
                    </siri:SituationExchangeDelivery>
                </siri:ServiceDelivery>
            </siri:Siri>
        </xsl:for-each>

    </xsl:template>

</xsl:stylesheet>
