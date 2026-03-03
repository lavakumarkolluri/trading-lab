CREATE TABLE IF NOT EXISTS learning.journal
(
    id              UUID DEFAULT generateUUIDv4(),
    created_at      DateTime DEFAULT now(),
    session_date    Date,
    topic           String,
    category        String,
    what_i_built    String DEFAULT '',
    concept         String DEFAULT '',
    why_it_matters  String DEFAULT '',
    code_snippet    String DEFAULT '',
    mistake_made    String DEFAULT '',
    how_i_fixed_it  String DEFAULT '',
    key_takeaway    String DEFAULT '',
    linkedin_post   String DEFAULT '',
    tags            Array(String),
    published       UInt8 DEFAULT 0
)
ENGINE = MergeTree()
ORDER BY (session_date, category)