import { createClient } from '@libsql/client';

if (
    !process.env.TURSO_DATABASE_URL ||
    (!process.env.TURSO_AUTH_TOKEN && process.env.TURSO_DATABASE_URL !== 'file:hashtags.db')
) {
    throw new Error('TURSO_DATABASE_URL and TURSO_AUTH_TOKEN must be set');
}

const client = createClient({
    url: process.env.TURSO_DATABASE_URL, // Your Turso database URL
    authToken: process.env.TURSO_AUTH_TOKEN, // Your Turso authentication token
});

console.log('Connected to Turso:', process.env.TURSO_DATABASE_URL);

// Function to initialize the database
async function initializeDatabase() {
    await client.batch(
        [
            {
                sql: `
                    CREATE TABLE IF NOT EXISTS hashtags (
                        timestamp BIGINT NOT NULL,
                        tag TEXT NOT NULL,
                        lang TEXT,
                        count INTEGER NOT NULL
                    );
                `,
                args: [],
            },
            {
                sql: `CREATE INDEX IF NOT EXISTS idx_timestamp ON hashtags (timestamp);`,
                args: [],
            },
            {
                sql: `CREATE INDEX IF NOT EXISTS idx_tag_lang ON hashtags (tag, lang);`,
                args: [],
            },
        ],
        'write'
    );
}

// Function to get the last timestamp from the database
async function getLastTimestamp(): Promise<number | null> {
    const result = await client.execute(
        'SELECT MAX(timestamp) AS last_timestamp FROM hashtags'
    );
    return (result.rows[0]?.last_timestamp as number) || null;
}

function printTimestamp(timestamp: number) {
    let time = new Date(timestamp / 1_000).toISOString();
    console.log('Timestamp:', time);
}

// Function to calculate microseconds
function secondsToMicroseconds(seconds: number) {
    return Math.floor(seconds * 1_000_000);
}

type HashtagCounts = Map<string, Map<string, number>>;

let currentHourStartTime: number;
const oneHourMicroseconds = secondsToMicroseconds(60 * 60);
let hashtagCounts: HashtagCounts = new Map<string, Map<string, number>>(); // Map<lang, Map<tag, count>>


let currentTimestamp: number;
let lastPrintedTimestamp: number;

async function startWebSocket(cursor: number) {
    const url = `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&cursor=${cursor}`;

    const ws = new WebSocket(url);

    ws.onopen = () => {
        console.log('Connected to WebSocket');
    };

    ws.onmessage = async (event) => {
        const data = event.data;
        const json = JSON.parse(data.toString());

        if (
            json.kind === 'commit' &&
            json.commit.collection === 'app.bsky.feed.post' &&
            json.commit.operation === 'create' &&
            json.commit.record.text
        ) {
            const timestamp = json.time_us;

            currentTimestamp = timestamp;

            if(!lastPrintedTimestamp) lastPrintedTimestamp = timestamp;

            if(lastPrintedTimestamp + secondsToMicroseconds(60) < currentTimestamp) {
                printTimestamp(timestamp);
                lastPrintedTimestamp = timestamp;
            }

            // Check if we've crossed into the next hour
            if (timestamp >= currentHourStartTime + oneHourMicroseconds) {
                // clone hashtagCounts

                const uploadTimestamp =
                    currentHourStartTime + oneHourMicroseconds - secondsToMicroseconds(5);

                uploadHashtagCounts(new Map(hashtagCounts), uploadTimestamp);

                hashtagCounts = new Map<string, Map<string, number>>();
                currentHourStartTime += oneHourMicroseconds;
            }

            const text: string = json.commit.record.text;
            let lang = json.commit.record.langs?.[0] || ''; // First language or 'unknown'

            // remove everything after - in lang and trim and lowercase
            lang = lang.split('-')[0].trim().toLowerCase();

            // Save hashtags in the map
            if (text.includes('#')) {
                const hashtags = text.match(/#(\w+)/g);

                if (hashtags) {
                    for (const hashtag of hashtags) {
                        const tag = hashtag.toLowerCase().replace('#', ''); // Normalize tag
                        let langCounts = hashtagCounts.get(lang);
                        if (!langCounts) {
                            langCounts = new Map();
                            hashtagCounts.set(lang, langCounts);
                        }
                        let count = langCounts.get(tag) ?? 0;
                        langCounts.set(tag, count + 1);
                    }
                }
            }
        }
    };

    ws.onerror = (event) => {
        console.error('WebSocket error:', event.message);
        reconnectWebSocket();
    };

    ws.onclose = () => {
        console.log('WebSocket connection closed');
        reconnectWebSocket();
    };
}

// Function to upload hashtag counts to the database
async function uploadHashtagCounts(hashtags: HashtagCounts, uploadTimestamp: number) {
    const insertQueries = [];

    for (const [lang, langCounts] of hashtags) {
        // Convert langCounts Map to an array of [tag, count] pairs
        const countsArray = Array.from(langCounts.entries());
        // Sort by count descending
        countsArray.sort((a, b) => b[1] - a[1]);
        // Take top 500
        const topCounts = countsArray.slice(0, 500);

        for (const [tag, count] of topCounts) {
            insertQueries.push({
                sql: 'INSERT INTO hashtags (timestamp, tag, lang, count) VALUES (?, ?, ?, ?)',
                args: [uploadTimestamp, tag, lang, count],
            });
        }
    }

    try {
        await client.batch(insertQueries, 'write');
        console.log(
            `Uploaded counts for hour`
        );
        printTimestamp(uploadTimestamp);
    } catch (err) {
        console.error('Failed to upload hashtag counts:', err.message);
    }
}

// Function to reconnect WebSocket with an optional delay
async function reconnectWebSocket(delay = 5000) {
    console.log(`Reconnecting WebSocket in ${delay / 1000} seconds...`);

    printTimestamp(currentTimestamp);

    setTimeout(() => startWebSocket(currentTimestamp), delay);
}

// Main logic
(async function main() {
    await initializeDatabase();

    const nowMicroseconds = Date.now() * 1_000;
    const twentyFourHoursAgoMicroseconds =
        nowMicroseconds - secondsToMicroseconds(24 * 60 * 60);

    let lastTimestamp = await getLastTimestamp();
    if (!lastTimestamp) {
        lastTimestamp = twentyFourHoursAgoMicroseconds;
    }

    // Use the later of lastTimestamp or 24 hours ago
    let startingTimestamp = Math.max(
        lastTimestamp,
        twentyFourHoursAgoMicroseconds
    );

    // Round up to the next full hour
    let startingDate = new Date(startingTimestamp / 1_000); // in milliseconds
    startingDate.setMinutes(0, 0, 0); // set to the top of the hour
    startingDate.setHours(startingDate.getHours() + 1); // add 1 hour

    if (startingDate.getTime() * 1_000 < startingTimestamp) {
        // If startingDate is before startingTimestamp, move to next hour
        startingDate.setHours(startingDate.getHours() + 1);
    }

    currentHourStartTime = startingDate.getTime() * 1_000; // in microseconds

    console.log('Using timestamp');
    printTimestamp(currentHourStartTime);

    await startWebSocket(currentHourStartTime);
})();
