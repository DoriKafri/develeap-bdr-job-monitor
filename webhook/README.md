# Apollo Phone Webhook

Cloudflare Worker that receives async phone number callbacks from Apollo.io's
`reveal_phone_number` API and commits them to `apollo_data.json` in the repo.

## Setup (one-time, ~2 minutes)

1. Install Wrangler (Cloudflare's CLI):
   ```bash
   npm install -g wrangler
   ```

2. Login to Cloudflare (free account):
   ```bash
   npx wrangler login
   ```

3. Set the GitHub token secret:
   ```bash
   cd webhook/
   npx wrangler secret put GH_TOKEN
   # Paste your GitHub token when prompted
   ```

4. Deploy:
   ```bash
   npx wrangler deploy
   ```

5. Copy the deployed URL (e.g. `https://apollo-phone-webhook.<your-subdomain>.workers.dev`)

6. Set the URL as a GitHub secret so the enrichment script can use it:
   - Go to repo Settings → Secrets → Actions
   - Add `APOLLO_WEBHOOK_URL` with the Worker URL

## How it works

1. `enrich_apollo.py` calls Apollo People Match with `reveal_phone_number: true`
   and `webhook_url` pointing to this Worker
2. Apollo processes the phone lookup asynchronously
3. When ready, Apollo POSTs the phone data to this Worker
4. The Worker reads `apollo_data.json` from GitHub, merges the phone numbers,
   and commits the updated file back

## Testing

```bash
curl -X POST https://your-worker.workers.dev \
  -H "Content-Type: application/json" \
  -d '{"person": {"id": "test", "email": "test@example.com", "phone_numbers": [{"sanitized_number": "+1234567890", "type": "mobile"}]}}'
```
