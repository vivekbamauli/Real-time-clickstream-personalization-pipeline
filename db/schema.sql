CREATE TABLE content (
    content_id TEXT PRIMARY KEY,
    title TEXT,
    category TEXT,
    media_url TEXT,
    like_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_interest (
    user_id TEXT,
    category TEXT,
    score FLOAT DEFAULT 0,
    PRIMARY KEY(user_id, category)
);
