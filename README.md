# Develeap BDR Job Monitor — Automated Pipeline

Automatically scans Israeli job boards for DevOps, AI Engineering, and Agentic Developer listings, updates an interactive dashboard, deploys to Netlify, and posts new listings to Slack.

**Live dashboard:** https://develeap-bdr-jobs.netlify.app

## Schedule

| Period | Frequency | Cron (UTC) | Israel Time |
|--------|-----------|------------|-------------|
| Daytime | Every 15 min | `*/15 6-16 * * *` | 08:00–18:59 |
| Nighttime | Every 60 min | `0 17-23,0-5 * * *` | 19:00–07:59 |

## Architecture

```
Search (SerpAPI / DuckDuckGo)
        ↓
   Parse & categorize jobs
        ↓
   Match Develeap customers (123 companies)
        ↓
   Merge with existing dashboard data
        ↓
   Deploy to Netlify + Post to Slack
```

## Files

- `update_jobs.py` — Main automation script
- `dashboard/index.html` — Self-contained HTML dashboard
- `.github/workflows/job-monitor.yml` — GitHub Actions workflow
- `requirements.txt` — Python dependencies

