# Deploy Mini Hyros for Free

## What You Need
- A **GitHub** account (free)
- A **Render** account (free) — for the backend API
- A **Vercel** account (free) — for the dashboard

---

## Step 1: Push to GitHub

```powershell
cd c:\Users\SPECIAL\Desktop\hyros
git init
git add .
git commit -m "Mini Hyros - self-hosted attribution"
```

Then create a new repo on https://github.com/new (call it `hyros` or whatever you want) and:

```powershell
git remote add origin https://github.com/YOUR_USERNAME/hyros.git
git branch -M main
git push -u origin main
```

---

## Step 2: Deploy Backend on Render

1. Go to https://render.com and sign up (free)
2. Click **New → Web Service**
3. Connect your GitHub repo
4. Configure:
   - **Name**: `mini-hyros-api`
   - **Root Directory**: leave empty (uses project root)
   - **Runtime**: **Docker**
   - **Instance Type**: **Free**
5. Add these **Environment Variables**:
   - `ATTRIBUTIONOPS_DB_PATH` = `/app/data/dummy/attributionops_demo.sqlite`
   - `TRACKING_DOMAIN` = `https://mini-hyros-api.onrender.com` (your Render URL)
   - `SITE_TOKEN` = `your-site-token` (pick any string)
6. Click **Create Web Service**
7. Wait for build to complete (~3-5 min)
8. Your backend URL will be: `https://mini-hyros-api.onrender.com`

**Test it:** Visit `https://mini-hyros-api.onrender.com/api/health`

---

## Step 3: Deploy Dashboard on Vercel

1. Go to https://vercel.com and sign up (free, use GitHub)
2. Click **Add New → Project**
3. Import your GitHub repo
4. Configure:
   - **Framework Preset**: Next.js
   - **Root Directory**: `dashboard`
5. Add this **Environment Variable**:
   - `NEXT_PUBLIC_API_URL` = `https://mini-hyros-api.onrender.com` (your Render URL from Step 2)
6. Click **Deploy**
7. Wait for build (~1-2 min)
8. Your dashboard URL will be something like: `https://hyros-dashboard.vercel.app`

---

## Step 4: Update Tracking Domain

Go back to Render → your web service → Environment:
- Set `TRACKING_DOMAIN` = `https://mini-hyros-api.onrender.com`

This makes the setup page (`/t/setup`) show the correct script URLs.

---

## Step 5: Install Tracking Pixel

Visit `https://mini-hyros-api.onrender.com/t/setup` for copy-paste snippets.

Your main tracking script will be:
```html
<script src="https://mini-hyros-api.onrender.com/t/hyros.js"
        data-token="your-site-token"
        data-endpoint="https://mini-hyros-api.onrender.com"></script>
```

---

## Your Final URLs

| What | URL |
|---|---|
| Dashboard | `https://your-app.vercel.app` |
| Backend API | `https://mini-hyros-api.onrender.com` |
| Tracking Script | `https://mini-hyros-api.onrender.com/t/hyros.js` |
| Setup Guide | `https://mini-hyros-api.onrender.com/t/setup` |
| Shopify Webhook | `https://mini-hyros-api.onrender.com/api/webhooks/shopify` |
| Stripe Webhook | `https://mini-hyros-api.onrender.com/api/webhooks/stripe` |

---

## Notes

- **Render free tier** sleeps after 15 min of inactivity. First request after sleep takes ~30s. To keep it awake, use a free cron pinger like https://cron-job.org to hit `/api/health` every 14 min.
- **Data resets** on Render free tier when you redeploy (SQLite is on ephemeral disk). For persistent data, either upgrade to Render's $7/mo plan or switch to Turso (free SQLite cloud DB).
- **Auto-deploy**: Both Vercel and Render auto-deploy when you push to GitHub.
