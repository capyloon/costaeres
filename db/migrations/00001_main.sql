
CREATE TABLE IF NOT EXISTS resources
(
    id       INTEGER  PRIMARY KEY ASC NOT NULL,
    parent   INTEGER  KEY NOT NULL,
    kind     INTEGER  NOT NULL,
    name     TEXT     KEY NOT NULL,
    created  DATETIME NOT NULL,
    modified DATETIME NOT NULL,
    scorer   BLOB     NOT NULL, -- bincode encoded representation of the scorer.
-- Enforce unique names under a container.
    UNIQUE(parent , name)
);

CREATE INDEX IF NOT EXISTS idx_res_name ON resources(name);

CREATE TABLE IF NOT EXISTS tags
(
    id  INTEGER KEY NOT NULL,
    tag TEXT    NOT NULL,
    FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS variants
(
    id       INTEGER KEY NOT NULL,
    name     TEXT    NOT NULL,
    mimeType TEXT    NOT NULL,
    size     INTEGER NOT NULL,
    FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tag_name ON tags(tag);

CREATE TABLE IF NOT EXISTS fts
(
    id    INTEGER KEY NOT NULL,
    ngram TEXT    NOT NULL,
    FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
    UNIQUE(id, ngram)
);

CREATE INDEX IF NOT EXISTS idx_ngram ON fts(ngram);
