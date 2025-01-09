// server.ts
import { getAllLanguages, getTopHashtagsLastHour, getTopHashtagsLast24Hours, getLastUpdateTimestamp } from './api';

// Define your API routes and handlers
export const handler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname;
    const method = req.method;

    if (method === "GET") {
        if (path === "/api/languages") {
            const languages = getAllLanguages();
            return new Response(JSON.stringify(languages), { status: 200, headers: { "Content-Type": "application/json" } });
        }

        if (path === "/api/top-hashtags/last-hour") {
            const lang = url.searchParams.get("lang");
            const hashtags = getTopHashtagsLastHour(lang);
            return new Response(JSON.stringify(hashtags), { status: 200, headers: { "Content-Type": "application/json" } });
        }

        if (path === "/api/top-hashtags/last-24-hours") {
            const lang = url.searchParams.get("lang");
            const hashtags = getTopHashtagsLast24Hours(lang);
            return new Response(JSON.stringify(hashtags), { status: 200, headers: { "Content-Type": "application/json" } });
        }

        if (path === "/api/last-update-timestamp") {
            const lastTimestamp = getLastUpdateTimestamp();
            return new Response(JSON.stringify({ last_timestamp: lastTimestamp }), { status: 200, headers: { "Content-Type": "application/json" } });
        }
    }

    // Handle 404 Not Found
    return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers: { "Content-Type": "application/json" } });
};
