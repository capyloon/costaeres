
CREATE TABLE IF NOT EXISTS resources
(
    id       TEXT     PRIMARY KEY ASC NOT NULL, -- a uuid to identify resources.
    parent   TEXT     KEY NOT NULL,
    kind     INTEGER  NOT NULL,
    name     TEXT     KEY NOT NULL,
    created  DATETIME NOT NULL,
    modified DATETIME NOT NULL,
    scorer   BLOB     NOT NULL, -- bincode encoded representation of the scorer.
-- Enforce unique names under a container.
    UNIQUE(parent , name)
);

CREATE TABLE IF NOT EXISTS tags
(
    id  TEXT KEY NOT NULL,
    tag TEXT NOT NULL,
    FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS variants
(
    id       TEXT    KEY NOT NULL,
    name     TEXT    NOT NULL,
    mimeType TEXT    NOT NULL,
    size     INTEGER NOT NULL,
    FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tag_name ON tags(tag);

CREATE TABLE IF NOT EXISTS fts
(
    id    TEXT KEY NOT NULL,
    ngram0 TEXT NOT NULL,
    ngram1 TEXT NOT NULL DEFAULT '',
    ngram2 TEXT NOT NULL DEFAULT '',
    ngram3 TEXT NOT NULL DEFAULT '',
    ngram4 TEXT NOT NULL DEFAULT '',
    ngram5 TEXT NOT NULL DEFAULT '',
    ngram6 TEXT NOT NULL DEFAULT '',
    ngram7 TEXT NOT NULL DEFAULT '',
    ngram8 TEXT NOT NULL DEFAULT '',
    ngram9 TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(id) REFERENCES resources(id) ON DELETE CASCADE
    UNIQUE(id, ngram0)
    UNIQUE(id, ngram1)
    UNIQUE(id, ngram2)
    UNIQUE(id, ngram3)
    UNIQUE(id, ngram4)
    UNIQUE(id, ngram5)
    UNIQUE(id, ngram6)
    UNIQUE(id, ngram7)
    UNIQUE(id, ngram8)
    UNIQUE(id, ngram9)
);

CREATE INDEX IF NOT EXISTS idx_ngram0 ON fts(ngram0);
CREATE INDEX IF NOT EXISTS idx_ngram1 ON fts(ngram1);
CREATE INDEX IF NOT EXISTS idx_ngram2 ON fts(ngram2);
CREATE INDEX IF NOT EXISTS idx_ngram3 ON fts(ngram3);
CREATE INDEX IF NOT EXISTS idx_ngram4 ON fts(ngram4);
CREATE INDEX IF NOT EXISTS idx_ngram5 ON fts(ngram5);
CREATE INDEX IF NOT EXISTS idx_ngram6 ON fts(ngram6);
CREATE INDEX IF NOT EXISTS idx_ngram7 ON fts(ngram7);
CREATE INDEX IF NOT EXISTS idx_ngram8 ON fts(ngram8);
CREATE INDEX IF NOT EXISTS idx_ngram9 ON fts(ngram9);
