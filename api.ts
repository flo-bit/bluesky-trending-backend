// api.ts

import { db } from './database'; // Import the SQLite database instance

// Function to get all languages in the database
export function getAllLanguages(): string[] {
    const result = db.query('SELECT DISTINCT lang FROM hashtags WHERE lang IS NOT NULL;').all();
    return result.map(row => row.lang);
}

// Helper function to get top hashtags within a specified time frame
export function getTopHashtags(hours: number, lang: string | null = null): { tag: string; total_count: number }[] {
    const timestampThreshold = Date.now() * 1000 - hours * 3600 * 1_000_000; // Convert hours to microseconds
    let sql = `
        SELECT tag, SUM(count) AS total_count
        FROM hashtags
        WHERE timestamp >= ?
    `;
    const args: (number | string)[] = [timestampThreshold];

    if (lang) {
        if (lang === 'en') {
            sql += ` AND (lang = ? OR lang = '')`;
            args.push(lang);
        } else {
            sql += ` AND lang = ?`;
            args.push(lang);
        }
    }
    sql += `
        GROUP BY tag
        ORDER BY total_count DESC
        LIMIT 50;
    `;

    const result = db.query(sql).all(...args);
    return result.map(row => ({ tag: row.tag, total_count: row.total_count }));
}

// Function to get top 100 hashtags of the last hour
export function getTopHashtagsLastHour(lang: string | null = null): { tag: string; total_count: number }[] {
    return getTopHashtags(1, lang);
}

// Function to get top 100 hashtags of the last 24 hours
export function getTopHashtagsLast24Hours(lang: string | null = null): { tag: string; total_count: number }[] {
    return getTopHashtags(24, lang);
}

// Function to get the last update timestamp
export function getLastUpdateTimestamp(): number | null {
    const row = db.query('SELECT MAX(timestamp) AS last_timestamp FROM hashtags;').get();
    return row?.last_timestamp || null;
}
