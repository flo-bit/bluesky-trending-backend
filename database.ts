// database.ts

import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';

// Determine the LiteFS directory based on the environment
const litefsDir = process.env.NODE_ENV === 'production' ? '/var/lib/litefs' : './litefs';
const litefsPath = join(litefsDir, 'db.sqlite');

// Ensure the LiteFS directory exists
if (!existsSync(litefsDir)) {
  console.error('Unable to reach LiteFS directory at', litefsDir);
  process.exit(1);
}

// Initialize SQLite database
const db = new Database(litefsPath);

// Initialize the database schema
db.run(`
  CREATE TABLE IF NOT EXISTS hashtags (
    timestamp BIGINT NOT NULL,
    tag TEXT NOT NULL,
    lang TEXT,
    count INTEGER NOT NULL
  );
`);

db.run(`
  CREATE INDEX IF NOT EXISTS idx_timestamp ON hashtags (timestamp);
`);

db.run(`
  CREATE INDEX IF NOT EXISTS idx_tag_lang ON hashtags (tag, lang);
`);

// Export the database instance for use in other modules
export { db };
