
CREATE TABLE IF NOT EXISTS objects
(
    id       INTEGER PRIMARY KEY ASC NOT NULL,
    parent   INTEGER NOT NULL,
    kind     INTEGER NOT NULL,
    name     TEXT    NOT NULL,
    mimeType TEXT    NOT NULL,
    size     INTEGER NOT NULL,
    created  DATETIME NOT NULL,
    modified DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS tags
(
    id  INTEGER KEY NOT NULL,
    tag TEXT    NOT NULL,
    FOREIGN KEY(id) REFERENCES objects(id) ON DELETE CASCADE
);
