SELECT
    userId,
    sessionId,
    channel
FROM {{ source('raw', 'user_session_channel') }}
WHERE sessionID IS NOT NULL
