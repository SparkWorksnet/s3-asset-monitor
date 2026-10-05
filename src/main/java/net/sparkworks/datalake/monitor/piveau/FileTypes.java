package net.sparkworks.datalake.monitor.piveau;

/**
 * Format and media type for a file, by extension. Kept in sync with the canonical extension
 * mappings used elsewhere in this project (dataops-orchestrator/piveau_dataset_client.py's
 * CANONICAL_MEDIA_TYPE_BY_EXTENSION, and EdcAssetRegistrationService.detectContentType).
 */
final class FileTypes {

    private FileTypes() {
    }

    static String extension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        return lastDot > 0 ? fileName.substring(lastDot + 1).toLowerCase() : "";
    }

    static String format(String fileName) {
        String ext = extension(fileName);
        return switch (ext) {
            case "csv" -> "CSV";
            case "tsv" -> "TSV";
            case "json" -> "JSON";
            case "jsonld" -> "JSON-LD";
            case "jsonl", "ndjson" -> "JSONL";
            case "txt" -> "TXT";
            case "xml" -> "XML";
            case "parquet" -> "PARQUET";
            case "xlsx", "xls" -> "XLSX";
            case "pdf" -> "PDF";
            default -> ext.toUpperCase();
        };
    }

    static String mediaType(String fileName) {
        return switch (extension(fileName)) {
            case "csv" -> "text/csv";
            case "tsv" -> "text/tab-separated-values";
            case "jsonl" -> "application/jsonl";
            case "ndjson" -> "application/x-ndjson";
            case "json" -> "application/json";
            case "jsonld" -> "application/ld+json";
            case "txt" -> "text/plain";
            case "xml" -> "application/xml";
            case "parquet" -> "application/parquet";
            case "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "xls" -> "application/vnd.ms-excel";
            case "pdf" -> "application/pdf";
            default -> "application/octet-stream";
        };
    }
}
