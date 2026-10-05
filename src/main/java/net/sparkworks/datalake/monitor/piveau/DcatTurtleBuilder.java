package net.sparkworks.datalake.monitor.piveau;

import java.util.List;

/**
 * Builds the DCAT-AP / 6G-DALI Turtle bodies sent to Piveau Hub Repo, compliant with the
 * 6G-DALI Metadata Application Profile.
 */
public final class DcatTurtleBuilder {

    private DcatTurtleBuilder() {
    }

    /** Turtle for the dataset described by a {@code metadata.json}. */
    public static String buildDataset(String apiUrl, String datasetId, DatasetMetadata metadata,
                                      String issuedDate, String modifiedDate) {
        StringBuilder turtle = new StringBuilder();

        // Prefixes (alphabetical)
        turtle.append("@prefix adms:   <http://www.w3.org/ns/adms#> .\n");
        turtle.append("@prefix dali:   <https://dali-project.eu/ns#> .\n");
        turtle.append("@prefix dcat:   <http://www.w3.org/ns/dcat#> .\n");
        turtle.append("@prefix dct:    <http://purl.org/dc/terms/> .\n");
        turtle.append("@prefix foaf:   <http://xmlns.com/foaf/0.1/> .\n");
        turtle.append("@prefix gax:    <https://registry.lab.gaia-x.eu/v1/api/trusted-shape-registry/v1/shapes/jsonld/trustframework#> .\n");
        turtle.append("@prefix prov:   <http://www.w3.org/ns/prov#> .\n");
        turtle.append("@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
        turtle.append("@prefix schema: <https://schema.org/> .\n");
        turtle.append("@prefix skos:   <http://www.w3.org/2004/02/skos/core#> .\n");
        turtle.append("@prefix vcard:  <http://www.w3.org/2006/vcard/ns#> .\n");
        turtle.append("@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .\n\n");

        turtle.append("<").append(apiUrl).append("/").append(datasetId).append(">\n");

        // ── Types ──────────────────────────────────────────────────────────────
        turtle.append("    a                         dcat:Dataset, gax:DataResource ;\n");

        // ── Core identity (Mandatory) ──────────────────────────────────────────
        turtle.append("    dct:title                 \"").append(escape(metadata.getTitle())).append("\"@en ;\n");
        turtle.append("    dct:description           \"").append(escape(metadata.getDescription())).append("\"@en ;\n");
        turtle.append("    dct:identifier            \"").append(escape(metadata.getIdentifier())).append("\" ;\n");
        turtle.append("    dct:issued                \"").append(issuedDate).append("\"^^xsd:date ;\n");
        turtle.append("    dct:modified              \"").append(modifiedDate).append("\"^^xsd:date ;\n");
        turtle.append("    dct:language              <http://publications.europa.eu/resource/authority/language/")
              .append(metadata.getLanguage()).append("> ;\n");

        // Version (Recommended, optional)
        if (hasText(metadata.getVersion())) {
            turtle.append("    adms:version              \"").append(escape(metadata.getVersion())).append("\" ;\n");
        }

        // ── Coverage (Recommended) ─────────────────────────────────────────────
        if (hasText(metadata.getSpatial())) {
            turtle.append("    dct:spatial               \"").append(escape(metadata.getSpatial())).append("\" ;\n");
        }
        if (hasText(metadata.getTemporalStart()) && hasText(metadata.getTemporalEnd())) {
            turtle.append("    dct:temporal              [ a dct:PeriodOfTime ; dcat:startDate \"")
                  .append(metadata.getTemporalStart()).append("\"^^xsd:date ; dcat:endDate \"")
                  .append(metadata.getTemporalEnd()).append("\"^^xsd:date ] ;\n");
        }

        // ── Rights & License (Mandatory) ───────────────────────────────────────
        turtle.append("    dct:accessRights          <http://publications.europa.eu/resource/authority/access-right/")
              .append(metadata.getAccessRights()).append("> ;\n");
        turtle.append("    dct:license               <").append(metadata.getLicense()).append("> ;\n");
        turtle.append("    dct:conformsTo            <https://www.go-fair.org/fair-principles/> ;\n");

        // ── SNS-JU / DALI (Mandatory) ──────────────────────────────────────────
        turtle.append("    dali:snsProjectName       \"").append(escape(metadata.getSnsProjectName())).append("\" ;\n");
        turtle.append("    dali:gdprCompliant        ").append(metadata.isGdprCompliant()).append(" ;\n");
        turtle.append("    dali:fairCompliant        ").append(metadata.isFairCompliant()).append(" ;\n");

        // ── GAIA-X (Mandatory) ─────────────────────────────────────────────────
        turtle.append("    gax:containsPII           ").append(metadata.isContainsPii()).append(" ;\n");
        if (hasText(metadata.getProducedBy())) {
            turtle.append("    gax:producedBy            <").append(metadata.getProducedBy()).append("> ;\n");
            turtle.append("    prov:wasAttributedTo      <").append(metadata.getProducedBy()).append("> ;\n");
        }
        if (hasText(metadata.getExposedThrough())) {
            turtle.append("    gax:exposedThrough        <").append(metadata.getExposedThrough()).append("> ;\n");
        }

        // ── Classification (Recommended) ───────────────────────────────────────
        turtle.append("    dcat:theme                <http://publications.europa.eu/resource/authority/data-theme/")
              .append(metadata.getTheme()).append("> ;\n");

        String keywords = formatQuotedList(metadata.getKeywords());
        if (!keywords.isEmpty()) {
            turtle.append("    dcat:keyword              ").append(keywords).append(" ;\n");
        }

        // ── Agents (Recommended) ───────────────────────────────────────────────
        if (hasText(metadata.getPublisher())) {
            turtle.append("    dct:publisher             [ a foaf:Organization ; foaf:name \"")
                  .append(escape(metadata.getPublisher())).append("\" ] ;\n");
        }

        if (hasText(metadata.getCreatorName())) {
            // foaf:Person for named researchers, foaf:Organization when the institution
            // itself is credited — the MAP uses both. ORCID goes in schema:identifier as
            // an IRI, so a bare "0000-0002-..." is expanded to its orcid.org form.
            String nodeType = "Organization".equalsIgnoreCase(metadata.getCreatorKind()) ? "foaf:Organization" : "foaf:Person";
            turtle.append("    dct:creator               [ a ").append(nodeType)
                  .append(" ; foaf:name \"").append(escape(metadata.getCreatorName())).append("\"");
            if (hasText(metadata.getCreatorEmail())) {
                turtle.append(" ; foaf:mbox <mailto:").append(metadata.getCreatorEmail()).append(">");
            }
            if (hasText(metadata.getCreatorOrcid())) {
                String orcid = metadata.getCreatorOrcid().trim();
                if (!orcid.startsWith("http")) {
                    orcid = "https://orcid.org/" + orcid;
                }
                turtle.append(" ; schema:identifier <").append(orcid).append(">");
            }
            if (hasText(metadata.getCreatorAffiliation())) {
                turtle.append(" ; schema:affiliation \"").append(escape(metadata.getCreatorAffiliation())).append("\"");
            }
            turtle.append(" ] ;\n");
        }

        for (String contributor : metadata.getContributors()) {
            if (!hasText(contributor)) continue;
            turtle.append("    dct:contributor           [ a foaf:Agent ; foaf:name \"")
                  .append(escape(contributor)).append("\" ] ;\n");
        }

        for (String publication : metadata.getRelatedPublications()) {
            if (!hasText(publication)) continue;
            turtle.append("    dct:relation              <").append(publication.trim()).append("> ;\n");
        }

        if (hasText(metadata.getContactEmail())) {
            turtle.append("    dcat:contactPoint         [ a vcard:Organization ; vcard:hasEmail <mailto:")
                  .append(metadata.getContactEmail()).append("> ] ;\n");
        }

        // Note: schema:variableMeasured / schema:measurementTechnique describe a specific
        // file's columns, not the dataset as a whole (a dataset can have distributions with
        // different columns) — per the 6G-DALI MAP (§5.3.E) these belong on the
        // dcat:Distribution instead (see buildDistribution), not here.

        // ── 5G/6G Testbed Context (Recommended) ───────────────────────────────
        TestbedContext tc = metadata.getTestbedContext();
        if (tc != null) {
            turtle.append("    dali:testbedContext       [\n");
            turtle.append("        a                     dali:TestbedContext ;\n");
            appendTcString(turtle, "dali:underlayPlatform",          tc.getUnderlayPlatform(), true);
            appendTcString(turtle, "dali:environment",               tc.getEnvironment(), false);
            appendTcString(turtle, "dali:networkDomain",             tc.getNetworkDomain(), false);
            appendTcString(turtle, "dali:ran3gppRelease",            tc.getRan3gppRelease(), false);
            appendTcString(turtle, "dali:ranNewRadioType",           tc.getRanNewRadioType(), false);
            appendTcString(turtle, "dali:ranSplit",                  tc.getRanSplit(), false);
            appendTcString(turtle, "dali:ranFocusedTechnology",      tc.getRanFocusedTechnology(), false);
            appendTcString(turtle, "dali:ranCoverageType",           tc.getRanCoverageType(), false);
            appendTcList(turtle,   "dali:ranFrequencyBand",          tc.getRanFrequencyBand());
            if (tc.getRanBandwidthMHz() != null) {
                turtle.append("        dali:ranBandwidthMHz      ").append(tc.getRanBandwidthMHz()).append(" ;\n");
            }
            if (tc.getRanMaxEndDevices() != null) {
                turtle.append("        dali:ranMaxEndDevices     ").append(tc.getRanMaxEndDevices()).append(" ;\n");
            }
            appendTcString(turtle, "dali:ranMobilityModel",          tc.getRanMobilityModel(), false);
            appendTcString(turtle, "dali:coreRelease",               tc.getCoreRelease(), false);
            appendTcString(turtle, "dali:coreSolution",              tc.getCoreSolution(), false);
            appendTcString(turtle, "dali:transportType",             tc.getTransportType(), false);
            appendTcString(turtle, "dali:computeOrchestratorType",   tc.getComputeOrchestratorType(), false);
            if (tc.getComputeGpuUse() != null) {
                turtle.append("        dali:computeGpuUse        ")
                      .append(tc.getComputeGpuUse()).append("^^xsd:boolean ;\n");
            }
            appendTcString(turtle, "dali:computeVirtualizationType", tc.getComputeVirtualizationType(), false);
            appendTcString(turtle, "dali:computeInfrastructureType", tc.getComputeInfrastructureType(), false);
            appendTcString(turtle, "dali:trafficOrigin",             tc.getTrafficOrigin(), false);
            appendTcString(turtle, "dali:trafficPattern",            tc.getTrafficPattern(), false);
            appendTcString(turtle, "dali:sliceType",                 tc.getSliceType(), false);
            appendTcString(turtle, "dali:referencePlane",            tc.getReferencePlane(), false);
            appendTcString(turtle, "dali:relatedVertical",           tc.getRelatedVertical(), false);
            appendTcString(turtle, "dali:observationPointHorizontal", tc.getObservationPointHorizontal(), false);
            appendTcString(turtle, "dali:observationPointVertical",  tc.getObservationPointVertical(), false);
            appendTcList(turtle,   "dali:measurementFamily",         tc.getMeasurementFamily());
            appendTcList(turtle,   "dali:measurementTool",           tc.getMeasurementTool());
            turtle.append("    ] ;\n");
        }

        // Replace the final " ;\n" with " .\n" to close the subject block
        String result = turtle.toString();
        int lastSemi = result.lastIndexOf(" ;\n");
        if (lastSemi >= 0) {
            result = result.substring(0, lastSemi) + " .\n" + result.substring(lastSemi + 3);
        }
        return result;
    }

    /** Turtle for one distribution (one data file) of a dataset. */
    public static String buildDistribution(String apiUrl, String datasetId, String distributionId, String assetId,
                                           String fileName, String accessUrl, String downloadUrl,
                                           List<String> columns, String measurementTechnique,
                                           String license, String issuedDate) {
        StringBuilder turtle = new StringBuilder();

        turtle.append("@prefix dali:   <https://dali-project.eu/ns#> .\n");
        turtle.append("@prefix dcat:   <http://www.w3.org/ns/dcat#> .\n");
        turtle.append("@prefix dcatap: <http://data.europa.eu/r5r/> .\n");
        turtle.append("@prefix dct:    <http://purl.org/dc/terms/> .\n");
        turtle.append("@prefix schema: <https://schema.org/> .\n");
        turtle.append("@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .\n\n");

        turtle.append("<").append(apiUrl).append("/").append(datasetId)
              .append("/distributions/").append(distributionId).append(">\n");
        turtle.append("    a                       dcat:Distribution ;\n");
        turtle.append("    dct:title               \"").append(escape(fileName)).append("\"@en ;\n");
        turtle.append("    dct:description         \"Data distribution for ").append(escape(fileName)).append("\"@en ;\n");
        // accessURL is the EDC connector's negotiation entrypoint (this distribution is
        // registered there under dali:assetId — see EdcAssetRegistrationService), not the
        // raw file — that's downloadURL below.
        if (hasText(accessUrl)) {
            turtle.append("    dcat:accessURL          <").append(accessUrl).append("> ;\n");
        }
        if (hasText(downloadUrl)) {
            turtle.append("    dcat:downloadURL        <").append(downloadUrl).append("> ;\n");
        }
        turtle.append("    dali:assetId            \"").append(escape(assetId)).append("\" ;\n");
        turtle.append("    dali:connectorType      \"dspaceconnector\" ;\n");
        turtle.append("    dct:format              \"").append(FileTypes.format(fileName)).append("\" ;\n");
        turtle.append("    dcat:mediaType          \"").append(FileTypes.mediaType(fileName)).append("\" ;\n");
        // schema:variableMeasured / schema:measurementTechnique describe this specific
        // file's columns, not the dataset as a whole — per 6G-DALI MAP §5.3.E they belong
        // here, on the distribution.
        if (columns != null && !columns.isEmpty()) {
            turtle.append("    schema:variableMeasured ").append(formatQuotedList(columns)).append(" ;\n");
        }
        if (hasText(measurementTechnique)) {
            turtle.append("    schema:measurementTechnique \"")
                  .append(escape(measurementTechnique)).append("\"@en ;\n");
        }
        turtle.append("    dct:license             <").append(license).append("> ;\n");
        turtle.append("    dct:issued              \"").append(issuedDate).append("\"^^xsd:date ;\n");
        turtle.append("    dcatap:availability     <http://data.europa.eu/r5r/availability/STABLE> .\n");

        return turtle.toString();
    }

    /** Slug for the distribution's own URI: lowercase, extension dropped, runs of non-alphanumerics as '-'. */
    public static String distributionId(String name) {
        int lastDot = name.lastIndexOf('.');
        String baseName = lastDot > 0 ? name.substring(0, lastDot) : name;
        return baseName.toLowerCase()
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("^-+|-+$", "");
    }

    private static boolean hasText(String s) {
        return s != null && !s.isEmpty();
    }

    private static void appendTcString(StringBuilder sb, String predicate, String value, boolean isUri) {
        if (!hasText(value)) return;
        sb.append("        ").append(predicate).append("  ");
        if (isUri) {
            sb.append("<").append(value).append(">");
        } else {
            sb.append("\"").append(escape(value)).append("\"");
        }
        sb.append(" ;\n");
    }

    /** One triple per value; blank entries are skipped so an unfilled list row is not published. */
    private static void appendTcList(StringBuilder sb, String predicate, List<String> values) {
        if (values == null) return;
        for (String value : values) {
            if (value == null || value.isBlank()) continue;
            sb.append("        ").append(predicate).append("  \"")
              .append(escape(value.trim())).append("\" ;\n");
        }
    }

    private static String formatQuotedList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(escape(values.get(i))).append("\"");
        }
        return sb.toString();
    }

    /** Escape special characters in strings for Turtle. */
    static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
