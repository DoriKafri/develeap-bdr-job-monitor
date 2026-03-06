/**
 * Apollo.io Phone Webhook Receiver — Cloudflare Worker
 *
 * Receives async phone number callbacks from Apollo's reveal_phone_number API,
 * and commits the data to apollo_data.json in the GitHub repo.
 *
 * Environment variables (set via wrangler secret):
 *   GH_TOKEN  — GitHub personal access token with repo write access
 *   GH_REPO   — e.g. "DoriKafri/develeap-bdr-job-monitor"
 *   WEBHOOK_SECRET — shared secret to validate Apollo callbacks (optional)
 */

const APOLLO_DATA_PATH = "apollo_data.json";

export default {
  async fetch(request, env) {
    // Only accept POST
    if (request.method !== "POST") {
      return new Response("OK", { status: 200 });
    }

    // Optional: validate webhook secret
    const url = new URL(request.url);
    if (env.WEBHOOK_SECRET) {
      const token = url.searchParams.get("token") || request.headers.get("x-webhook-secret");
      if (token !== env.WEBHOOK_SECRET) {
        return new Response("Unauthorized", { status: 401 });
      }
    }

    try {
      const body = await request.json();
      console.log("Received Apollo webhook:", JSON.stringify(body).slice(0, 500));

      // Apollo sends phone data in various formats; extract what we need
      const phoneData = extractPhoneData(body);
      if (!phoneData || phoneData.length === 0) {
        return new Response(JSON.stringify({ status: "no_phone_data" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Read current apollo_data.json from GitHub
      const currentData = await readGitHubFile(env);
      if (!currentData) {
        return new Response(JSON.stringify({ error: "Could not read apollo_data.json" }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Merge phone numbers into contacts
      let updated = 0;
      const contacts = currentData.content.contacts || {};

      for (const entry of phoneData) {
        // Find matching contact by apolloId or email
        for (const [key, contact] of Object.entries(contacts)) {
          const match =
            (entry.apolloId && contact.apolloId === entry.apolloId) ||
            (entry.email && contact.email === entry.email);

          if (match) {
            // Update phone fields
            if (entry.phone) {
              contact.phone = entry.phone;
              contact.phoneType = entry.phoneType || "other";
            }
            if (entry.allPhones && entry.allPhones.length > 0) {
              contact.allPhones = entry.allPhones;
              // Prefer mobile for WhatsApp
              const mobile = entry.allPhones.find((p) => p.type === "mobile");
              if (mobile) {
                contact.phone = mobile.number;
                contact.phoneType = "mobile";
              } else if (!contact.phone && entry.allPhones[0]) {
                contact.phone = entry.allPhones[0].number;
                contact.phoneType = entry.allPhones[0].type || "other";
              }
            }
            updated++;
            break;
          }
        }
      }

      if (updated === 0) {
        return new Response(JSON.stringify({ status: "no_matches", received: phoneData.length }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Update sync metadata
      currentData.content.lastPhoneUpdate = new Date().toISOString();
      currentData.content.contacts = contacts;

      // Commit back to GitHub
      const committed = await writeGitHubFile(env, currentData.content, currentData.sha);

      return new Response(
        JSON.stringify({
          status: "ok",
          updated,
          total_received: phoneData.length,
          committed,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    } catch (err) {
      console.error("Webhook error:", err);
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};

/**
 * Extract phone data from Apollo webhook payload.
 * Apollo may send different formats depending on the endpoint.
 */
function extractPhoneData(body) {
  const results = [];

  // Format 1: Single person object with phone_numbers
  if (body.person) {
    const p = body.person;
    const phones = (p.phone_numbers || []).map((ph) => ({
      number: ph.sanitized_number || ph.number || "",
      type: (ph.type || "").toLowerCase(),
    })).filter((ph) => ph.number);

    if (phones.length > 0) {
      const mobile = phones.find((ph) => ph.type === "mobile");
      results.push({
        apolloId: p.id || "",
        email: p.email || "",
        phone: mobile ? mobile.number : phones[0].number,
        phoneType: mobile ? "mobile" : phones[0].type || "other",
        allPhones: phones,
      });
    }
  }

  // Format 2: Array of people
  if (Array.isArray(body.people)) {
    for (const p of body.people) {
      const phones = (p.phone_numbers || []).map((ph) => ({
        number: ph.sanitized_number || ph.number || "",
        type: (ph.type || "").toLowerCase(),
      })).filter((ph) => ph.number);

      if (phones.length > 0) {
        const mobile = phones.find((ph) => ph.type === "mobile");
        results.push({
          apolloId: p.id || "",
          email: p.email || "",
          phone: mobile ? mobile.number : phones[0].number,
          phoneType: mobile ? "mobile" : phones[0].type || "other",
          allPhones: phones,
        });
      }
    }
  }

  // Format 3: Waterfall phone result
  if (body.phone_number || body.sanitized_number) {
    results.push({
      apolloId: body.person_id || body.id || "",
      email: body.email || "",
      phone: body.sanitized_number || body.phone_number || "",
      phoneType: (body.phone_type || "other").toLowerCase(),
      allPhones: [
        {
          number: body.sanitized_number || body.phone_number || "",
          type: (body.phone_type || "other").toLowerCase(),
        },
      ],
    });
  }

  return results;
}

/**
 * Read apollo_data.json from GitHub.
 */
async function readGitHubFile(env) {
  const resp = await fetch(
    `https://api.github.com/repos/${env.GH_REPO}/contents/${APOLLO_DATA_PATH}`,
    {
      headers: {
        Authorization: `token ${env.GH_TOKEN}`,
        Accept: "application/vnd.github.v3+json",
        "User-Agent": "apollo-phone-webhook",
      },
    }
  );

  if (!resp.ok) {
    console.error("GitHub read failed:", resp.status, await resp.text());
    return null;
  }

  const data = await resp.json();
  const content = JSON.parse(atob(data.content.replace(/\n/g, "")));
  return { content, sha: data.sha };
}

/**
 * Write updated apollo_data.json back to GitHub.
 */
async function writeGitHubFile(env, content, sha) {
  const encoded = btoa(unescape(encodeURIComponent(JSON.stringify(content, null, 2))));

  const resp = await fetch(
    `https://api.github.com/repos/${env.GH_REPO}/contents/${APOLLO_DATA_PATH}`,
    {
      method: "PUT",
      headers: {
        Authorization: `token ${env.GH_TOKEN}`,
        Accept: "application/vnd.github.v3+json",
        "Content-Type": "application/json",
        "User-Agent": "apollo-phone-webhook",
      },
      body: JSON.stringify({
        message: "chore: update phone numbers from Apollo webhook",
        content: encoded,
        sha: sha,
      }),
    }
  );

  if (!resp.ok) {
    const err = await resp.text();
    console.error("GitHub write failed:", resp.status, err);
    return false;
  }
  return true;
}
