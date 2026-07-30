# Render Exit Checklist

Vigil's persistent database is Supabase PostgreSQL and its replacement API is:

`https://vigil-api.vercel.app`

Do not disable Render until every external sender below uses that URL. Requests sent to the old Render URL cannot be recovered automatically.

## External senders

1. Update the tracking scripts on every funnel, landing, checkout, and thank-you page:

```html
<script src="https://vigil-api.vercel.app/t/hyros.js"
        data-token="YOUR_SITE_TOKEN"
        data-endpoint="https://vigil-api.vercel.app"
        data-stape-endpoint="https://wnczugry.usv.stape.io"></script>
<script src="https://vigil-api.vercel.app/t/hyros-ghl.js"></script>
```

2. In the Google Ads spend script, set:

```javascript
var MINI_HYROS_ENDPOINT = "https://vigil-api.vercel.app";
```

3. In Stape, replace any Mini Hyros callback or forwarding URL that begins with `https://mini-hyros.onrender.com` with `https://vigil-api.vercel.app`, preserving the path.

4. In Meta and TikTok developer settings, replace Render OAuth redirect URLs with the matching `https://vigil-api.vercel.app` callback URL shown by Vigil's connection screen.

5. Set the dashboard production variable to:

```text
NEXT_PUBLIC_API_URL=https://vigil-api.vercel.app
```

## Verification before shutdown

- `https://vigil-api.vercel.app/health` returns HTTP 200 and reports PostgreSQL.
- `https://vigil-api.vercel.app/t/hyros.js` returns JavaScript.
- The dashboard loads reports through `vigil-api.vercel.app` with no Render requests in DevTools.
- A real lead and a real purchase appear in Vigil after being submitted.
- The Google Ads script completes successfully against the Vercel endpoint.
- Meta and TikTok reconnect and sync without callback errors.

After all checks pass, suspend the Render service for 24 hours. If no traffic or missing data is observed, delete the Render service and its disk.
