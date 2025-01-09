// app.ts

import WebSocket from 'ws';
import { db } from './database';
import { serve } from 'bun';
import { handler } from './server';

// Function to get the last timestamp from the database
async function getLastTimestamp(): Promise<number | null> {
    const result = db.query('SELECT MAX(timestamp) AS last_timestamp FROM hashtags').get()
    // @ts-ignore
    return result.last_timestamp || null;
}

function printTimestamp(timestamp: number) {
    const time = new Date(timestamp / 1_000).toISOString();
    console.log('Timestamp:', time);
}

// Function to calculate microseconds
function secondsToMicroseconds(seconds: number) {
    return Math.floor(seconds * 1_000_000);
}

type HashtagCounts = Map<string, Map<string, number>>;

let currentHourStartTime: number;
const oneHourMicroseconds = secondsToMicroseconds(60 * 60);
let hashtagCounts: HashtagCounts = new Map(); // Map<lang, Map<tag, count>>

let currentTimestamp: number = 0;
let lastPrintedTimestamp: number = 0;

// Function to start the WebSocket connection
function startWebSocket(cursor: number) {
    const url = `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&cursor=${cursor}`;

    const ws = new WebSocket(url);

    ws.onopen = () => {
        console.log('Connected to WebSocket');
    };

    ws.onmessage = (event) => {
        const data = event.data as string;
        const json = JSON.parse(data.toString());

        if (
            json.kind === 'commit' &&
            json.commit.collection === 'app.bsky.feed.post' &&
            json.commit.operation === 'create' &&
            json.commit.record.text
        ) {
            const timestamp = json.time_us;

            currentTimestamp = timestamp;

            if (!lastPrintedTimestamp) lastPrintedTimestamp = timestamp;

            if (lastPrintedTimestamp + secondsToMicroseconds(60) < currentTimestamp) {
                printTimestamp(timestamp);
                lastPrintedTimestamp = timestamp;
            }

            // Check if we've crossed into the next hour
            if (timestamp >= currentHourStartTime + oneHourMicroseconds) {
                const uploadTimestamp =
                    currentHourStartTime + oneHourMicroseconds - secondsToMicroseconds(5);

                uploadHashtagCounts(new Map(hashtagCounts), uploadTimestamp);

                hashtagCounts = new Map();
                currentHourStartTime += oneHourMicroseconds;
            }

            const text: string = json.commit.record.text;
            let lang = json.commit.record.langs?.[0] || ''; // First language or 'unknown'

            // Remove everything after '-' in lang, trim, and lowercase
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

    ws.onerror = (event: any) => {
        console.error('WebSocket error:', event.message);
        reconnectWebSocket();
    };

    ws.onclose = () => {
        console.log('WebSocket connection closed');
        reconnectWebSocket();
    };
}

// Function to upload hashtag counts to the database
function uploadHashtagCounts(hashtags: HashtagCounts, uploadTimestamp: number) {
    const insertQueries: { tag: string; lang: string; count: number; timestamp: number }[] = [];

    for (const [lang, langCounts] of hashtags) {
        // Convert langCounts Map to an array of [tag, count] pairs
        const countsArray = Array.from(langCounts.entries());
        // Sort by count descending
        countsArray.sort((a, b) => b[1] - a[1]);
        // Take top 500
        const topCounts = countsArray.slice(0, 500);

        for (const [tag, count] of topCounts) {
            insertQueries.push({ tag, lang, count, timestamp: uploadTimestamp });
        }
    }

    try {
        const transaction = db.transaction(() => {
            const stmt = db.prepare('INSERT INTO hashtags (timestamp, tag, lang, count) VALUES (?, ?, ?, ?)');
            for (const { tag, lang, count, timestamp } of insertQueries) {
                stmt.run(timestamp, tag, lang, count);
            }
            stmt.finalize();
        });

        transaction();

        console.log(`Uploaded counts for hour`);
        printTimestamp(uploadTimestamp);
    } catch (err: any) {
        console.error('Failed to upload hashtag counts:', err.message);
    }
}

// Function to reconnect WebSocket with an optional delay
function reconnectWebSocket(delay = 5000) {
    console.log(`Reconnecting WebSocket in ${delay / 1000} seconds...`);
    printTimestamp(currentTimestamp);

    setTimeout(() => startWebSocket(currentTimestamp), delay);
}

(async function main() {
    // console.log('Starting main');
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

    startWebSocket(currentHourStartTime);

    // Start the API server
    serve({
        fetch: handler,
        port: 3001, // Use a different port if needed
        hostname: "0.0.0.0",
    });
})();