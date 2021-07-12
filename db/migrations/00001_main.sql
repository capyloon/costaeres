
CREATE TABLE IF NOT EXISTS objects
(
    id       INTEGER  PRIMARY KEY ASC NOT NULL,
    parent   INTEGER  KEY NOT NULL,
    kind     INTEGER  NOT NULL,
    name     TEXT     NOT NULL,
    mimeType TEXT     NOT NULL,
    size     INTEGER  NOT NULL,
    created  DATETIME NOT NULL,
    modified DATETIME NOT NULL,
    score    TEXT     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_obj_mime ON objects(mimeType);
CREATE INDEX IF NOT EXISTS idx_obj_name ON objects(name);

CREATE TABLE IF NOT EXISTS tags
(
    id  INTEGER KEY NOT NULL,
    tag TEXT    NOT NULL,
    FOREIGN KEY(id) REFERENCES objects(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tag_name ON tags(tag);

CREATE TABLE IF NOT EXISTS fts
(
    id    INTEGER KEY NOT NULL,
    ngram TEXT    NOT NULL,
    FOREIGN KEY(id) REFERENCES objects(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ngram ON fts(ngram);
