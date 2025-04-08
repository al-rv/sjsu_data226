SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_BLUEJAY.raw.user_session_channel
WHERE sessionID IS NOT NULL