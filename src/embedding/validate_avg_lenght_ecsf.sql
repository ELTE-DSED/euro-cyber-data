SELECT
    MAX(LENGTH(description)) AS max_chars,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LENGTH(description)) AS p95_chars
FROM ecsf_tks
WHERE description IS NOT NULL;