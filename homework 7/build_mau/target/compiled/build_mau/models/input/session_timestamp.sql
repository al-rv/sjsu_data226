SELECT
    sessionId,
    ts
FROM USER_DB_BLUEJAY.raw.session_timestamp
WHERE sessionId IS NOT NULL