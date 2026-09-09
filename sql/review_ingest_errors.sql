-- Review aggregate provider ingestion failures without exposing raw payloads.
SELECT
    source,
    stage,
    error_code,
    error_message,
    COUNT(*) AS record_count,
    MIN(detected_at) AS first_detected_at,
    MAX(detected_at) AS last_detected_at
FROM ingest_errors
GROUP BY source, stage, error_code, error_message
ORDER BY source, stage, record_count DESC, error_code;
