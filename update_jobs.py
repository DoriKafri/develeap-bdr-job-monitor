#!/usr/bin/env python3
"""
Develeap BDR Job Monitor — Automated Update Script
Searches Israeli job boards, updates the HTML dashboard, deploys to Netlify,
and posts new listings to Slack #bdr-updates.
"""

import os
import re
import json
import time
import random
import hashlib
import zipfile
import io
import html as html_mod
import base64
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────
NETLIFY_SITE_ID = os.environ.get("NETLIFY_SITE_ID", "9533027e-5008-40ca-924c-dede933f0473")
NETLIFY_TOKEN = os.environ.get("NETLIFY_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")  # Optional: for better search results
DASHBOARD_PATH = os.environ.get("DASHBOARD_PATH", "dashboard/index.html")
SLACK_POSTED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "slack_posted.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Workflow Config ───────────────────────────────────────────────────────
WORKFLOW_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflow_config.json")

def _load_workflow_config():
    """Load workflow_config.json if it exists. Returns dict or empty dict."""
    if os.path.exists(WORKFLOW_CONFIG_PATH):
        try:
            with open(WORKFLOW_CONFIG_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Could not load workflow config: {e}")
    return {}

def _is_node_enabled(config, node_id):
    """Check if a workflow node is enabled. Defaults to True if not configured."""
    nodes = config.get("nodes", {})
    node = nodes.get(node_id, {})
    return node.get("enabled", True)

# ── Develeap customers (case-insensitive partial match) ────────────────────
DEVELEAP_CUSTOMERS = [
    "Akamai","Alzai","Amsalem Tours","Apester","Aqua","Armo","Automarky",
    "Beacon Security","Bluespine","Bond","BYON","Cal","Cellebrite","Cellosign",
    "Checkin Travels","Checkpoint","Cibus","CitrusX","Cloudzone","Ctera","Curated-ai",
    "CyberArk","CyberRidge","Cylus","DriveTech","Edwards","eXLGx","Ezbob","Flexor",
    "Foretellix","Grain Finance","Hyp","Imagry","Infinpoint","Inuitive","Isracard",
    "Jedify","Legion","Linx security","Matrix","Megureit","Mobileye","Monday.com",
    "N2WS","Ness","NetNut","Networx","Nuvo cares","Odysight","OwlDuet","Per-me",
    "Philips","Planet9","Plus500","PrettyDamnQuick","Proceed","ProofPoint","Puzzlesoft",
    "R.R Systems","Redis","Redwood","RSI","Scytale","Sightec","Simplex3d","SkyCash",
    "Solidus","Tactile","TailorMed","Transmit Security","Tufin","Vcita","Verifood",
    "Vorlon","XMCyber","Zafran","Zerto","Zimark",
]

DEVELEAP_PAST_CUSTOMERS = [
    "AppsFlyer","Autodesk","Blink Aid","BridgeOver","Carebox","Checkmarx",
    "Civ Robotics","CurveTech","Elmodis","Empathy","Evogene","Fireblocks","Gloat",
    "Harmonic","Hexagon","Honeywell","InfluenceAI","JFrog","Knostic","LedderTech",
    "mPrest","NeoTech","Nintex","NSO","OwnPlay","Pillar Security","RapidAPI",
    "Rapyd","Revelator","Sentrycs","Verbit","WalkMe",
]

# ── Company Domains for Logo Lookup ───────────────────────────────────────
# Maps company name (lowercase) → domain for Clearbit Logo API
COMPANY_DOMAINS = {
    "allcloud": "allcloud.io",
    "appcharge": "appcharge.com",
    "applied materials": "appliedmaterials.com",
    "applied materials - israel": "appliedmaterials.com",
    "aqua security": "aquasec.com",
    "armissecurity": "armis.com",
    "arpeely": "arpeely.com",
    "attil": "attil.io",
    "au10tix": "au10tix.com",
    "audiocodes": "audiocodes.com",
    "augury": "augury.com",
    "biocatch": "biocatch.com",
    "blink ops": "blinkops.com",
    "bmc": "bmc.com",
    "cato networks": "catonetworks.com",
    "chaos labs": "chaoslabs.xyz",
    "check point software": "checkpoint.com",
    "classiq": "classiq.io",
    "cloudinary": "cloudinary.com",
    "codevalue": "codevalue.net",
    "cyberark": "cyberark.com",
    "cymulate": "cymulate.com",
    "datadog": "datadoghq.com",
    "doit": "doit.com",
    "dualbird": "dualbird.com",
    "earnix": "earnix.com",
    "elbit systems israel": "elbitsystems.com",
    "factored": "factored.ai",
    "fetcherr": "fetcherr.io",
    "fireblocks": "fireblocks.com",
    "forter": "forter.com",
    "fundamental": "fundamental.cc",
    "global payments inc.": "globalpayments.com",
    "globallogic": "globallogic.com",
    "guidde": "guidde.com",
    "harmonya": "harmonya.com",
    "hio": "hio.store",
    "hivestack": "hivestack.com",
    "imagen": "imagen-ai.com",
    "jobgether": "jobgether.com",
    "kpmg": "kpmg.com",
    "leidos": "leidos.com",
    "lightricks": "lightricks.com",
    "majestic labs": "majesticlabs.io",
    "marvin": "marvin.com",
    "mastercard": "mastercard.com",
    "matia": "matia.io",
    "metalbear": "metalbear.co",
    "minimus": "minimumsec.com",
    "mobileye": "mobileye.com",
    "nvidia": "nvidia.com",
    "next insurance": "nextinsurance.com",
    "nextta": "nextta.com",
    "oligo security": "oligo.security",
    "pagaya": "pagaya.com",
    "pango": "pango.co.il",
    "paragon": "useparagon.com",
    "pentera": "pentera.io",
    "phasev": "phasev.ai",
    "plainid": "plainid.com",
    "port": "getport.io",
    "quanthealth": "quanthealth.com",
    "quantum machines": "quantum-machines.co",
    "remedio": "gytpol.com",
    "remedio formerly gytpol": "gytpol.com",
    "salesforce": "salesforce.com",
    "sentra": "sentra.io",
    "silverfort": "silverfort.com",
    "similarweb": "similarweb.com",
    "surecomp": "surecomp.com",
    "taboola": "taboola.com",
    "tavily": "tavily.com",
    "team8": "team8.vc",
    "techaviv": "techaviv.com",
    "terasky": "terasky.com",
    "tikal": "tikalk.com",
    "tikalk": "tikalk.com",
    "unframe": "unframe.com",
    "unity": "unity.com",
    "vastdata": "vastdata.com",
    "voyantis": "voyantis.ai",
    "wavelbl": "wavelbl.com",
    "wiz": "wiz.io",
    "yael group": "yaelgroup.com",
    "zenity": "zenity.io",
    "zscaler": "zscaler.com",
}

def _get_company_logo(company: str, source_url: str = "") -> str:
    """Get company logo URL via Google Favicon API.

    Uses COMPANY_DOMAINS mapping first, then tries to derive domain from ATS URL.
    Returns a Google Favicon URL or empty string.
    """
    if not company or company == "Unknown":
        return ""
    company_lower = company.lower().strip()

    # 1. Direct lookup
    domain = COMPANY_DOMAINS.get(company_lower, "")

    # 2. Try partial match
    if not domain:
        for key, d in COMPANY_DOMAINS.items():
            if key in company_lower or company_lower in key:
                domain = d
                break

    # 3. Try deriving from ATS URL slug
    if not domain and source_url:
        for ats_pat in [
            r"(?:boards?\.)?(?:job-boards?\.)?(?:eu\.)?greenhouse\.io/([a-z0-9\-]+)",
            r"jobs?\.lever\.co/([a-z0-9\-]+)",
            r"jobs\.ashbyhq\.com/([a-z0-9\-]+)",
            r"([a-z0-9\-]+)\.wd\d+\.myworkdayjobs\.com",
        ]:
            m = re.search(ats_pat, source_url)
            if m:
                slug = m.group(1)
                domain = slug + ".com"  # Default to .com for ATS slugs
                break

    # 4. Try company name as domain (common pattern)
    if not domain:
        clean = re.sub(r'[^a-z0-9]', '', company_lower)
        if clean:
            domain = clean + ".com"

    if domain:
        return f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    return ""


# ── Company Stakeholders for Outreach ──────────────────────────────────────
# Key decision-makers at target companies for BDR outreach
# Sources: LinkedIn, Crunchbase, company websites, Startup Nation, GeekTime,
#          Calcalist, Globes, CTech, F6S, PitchBook, GitHub, Twitter/X, ZoomInfo
# Each contact: name, title, linkedin, source, email (work email guess)
COMPANY_STAKEHOLDERS = {
    "zenity": [
        {"name": "Michael Bargury", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/michaelbargury/", "source": "LinkedIn", "email": "michael@zenity.io"},
        {"name": "Ronen Yaari", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/ronen-yaari-79a1ba4/", "source": "LinkedIn", "email": "ronen@zenity.io"},
        {"name": "Shay Haluba", "title": "Director of Engineering & Innovation", "linkedin": "https://www.linkedin.com/in/shay-haluba/", "source": "LinkedIn", "email": "shay@zenity.io"},
        {"name": "Ben Kliger", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/benkliger/", "source": "Crunchbase", "email": "ben@zenity.io"},
    ],
    "surecomp": [
        {"name": "Benny Savinik", "title": "VP Technology", "linkedin": "https://www.linkedin.com/in/benny-savinik-0299364/", "source": "LinkedIn", "email": "benny.savinik@surecomp.com"},
        {"name": "Tsafrir Atar", "title": "VP Digitization", "linkedin": "https://il.linkedin.com/in/tsafriratar", "source": "LinkedIn", "email": "tsafrir.atar@surecomp.com"},
        {"name": "Eyal Hareuveny", "title": "President", "linkedin": "", "source": "Company Website", "email": "eyal.hareuveny@surecomp.com"},
    ],
    "vast data": [
        {"name": "Renen Hallak", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/renenh/", "source": "Crunchbase", "email": "renen.hallak@vastdata.com"},
        {"name": "Jeff Denworth", "title": "Co-Founder & CMO", "linkedin": "https://www.linkedin.com/in/jeffreydenworth/", "source": "LinkedIn", "email": "jeff.denworth@vastdata.com"},
    ],
    "vastdata": [
        {"name": "Renen Hallak", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/renenh/", "source": "Crunchbase", "email": "renen.hallak@vastdata.com"},
        {"name": "Jeff Denworth", "title": "Co-Founder & CMO", "linkedin": "https://www.linkedin.com/in/jeffreydenworth/", "source": "LinkedIn", "email": "jeff.denworth@vastdata.com"},
    ],
    "check point": [
        {"name": "Nataly Kremer", "title": "CPO & Head of R&D", "linkedin": "https://www.linkedin.com/in/nataly-kremer-12744b29/", "source": "Company Website", "email": ""},
        {"name": "Tomer Lev", "title": "Engineering Director", "linkedin": "https://www.linkedin.com/in/tomerlev/", "source": "LinkedIn", "email": ""},
        {"name": "Ofir Israel", "title": "Engineering Director", "linkedin": "https://www.linkedin.com/in/ofirisrael/", "source": "LinkedIn", "email": ""},
        {"name": "Alex Spokoiny", "title": "Chief Information Officer", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "sentra": [
        {"name": "Ron Reiter", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/ronreiter/", "source": "LinkedIn", "email": "ron@sentra.io"},
        {"name": "Asaf Kochan", "title": "Co-Founder & President", "linkedin": "https://www.linkedin.com/in/asafkochan/", "source": "Crunchbase", "email": "asaf@sentra.io"},
        {"name": "Yoav Regev", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yoav-regev-31718a1/", "source": "LinkedIn", "email": "yoav@sentra.io"},
        {"name": "Yair Cohen", "title": "Co-Founder & VP Product", "linkedin": "https://www.linkedin.com/in/yair-cohen-pm/", "source": "LinkedIn", "email": "yair@sentra.io"},
    ],
    "port": [
        {"name": "Yonatan Boguslavski", "title": "Co-Founder & CTO", "linkedin": "https://il.linkedin.com/in/yonatan-boguslavski-36354b125", "source": "LinkedIn", "email": "yonatan@getport.io"},
        {"name": "Zohar Einy", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/zohar-einy/", "source": "CTech", "email": "zohar@getport.io"},
    ],
    "earnix": [
        {"name": "Erez Barak", "title": "CTO", "linkedin": "https://www.linkedin.com/in/erezbarak/", "source": "LinkedIn", "email": "erez@earnix.com"},
        {"name": "Yaakov Cohen", "title": "VP Engineering, AI Platform", "linkedin": "https://www.linkedin.com/in/yaakovcohen/", "source": "LinkedIn", "email": "yaakov@earnix.com"},
        {"name": "Robin Gilthorpe", "title": "CEO", "linkedin": "https://www.linkedin.com/in/robingilthorpe/", "source": "LinkedIn", "email": "robin@earnix.com"},
    ],
    "nvidia": [
        {"name": "Amit Krig", "title": "SVP Software Engineering & Israel Site Leader", "linkedin": "https://www.linkedin.com/in/amit-krig-7492981/", "source": "LinkedIn", "email": ""},
        {"name": "Gideon Rosenberg", "title": "VP HR Israel", "linkedin": "https://www.linkedin.com/in/gideon-rosenberg-894787/", "source": "LinkedIn", "email": ""},
        {"name": "Michael Kagan", "title": "CTO", "linkedin": "https://il.linkedin.com/in/mikagan", "source": "LinkedIn", "email": ""},
        {"name": "Yaron Goldberg", "title": "Sr Director Engineering", "linkedin": "https://www.linkedin.com/in/yarongoldberg/", "source": "LinkedIn", "email": ""},
    ],
    "elbit": [
        {"name": "Yehoshua Yehuda", "title": "EVP Strategy & CTO", "linkedin": "https://il.linkedin.com/in/yehoshua-shuki-yehuda-0245701", "source": "LinkedIn", "email": ""},
        {"name": "Cindy James", "title": "Sr Director Talent Acquisition", "linkedin": "https://www.linkedin.com/in/cindy-james-3115a68/", "source": "LinkedIn", "email": ""},
    ],
    "classiq": [
        {"name": "Yehuda Naveh", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yehuda-naveh/", "source": "Crunchbase", "email": "yehuda@classiq.io"},
        {"name": "Nir Minerbi", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/nir-minerbi/", "source": "Startup Nation", "email": "nir@classiq.io"},
        {"name": "Amir Naveh", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/amir-naveh-li/", "source": "LinkedIn", "email": "amir@classiq.io"},
    ],
    "tikal": [
        {"name": "Lior Kanfi", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/liorkanfi/", "source": "LinkedIn", "email": "lior@tikalk.com"},
        {"name": "Amir Misgav", "title": "DevOps Tech Leader", "linkedin": "https://www.linkedin.com/in/amir-misgav/", "source": "LinkedIn", "email": "amir@tikalk.com"},
        {"name": "Tamir Tausi", "title": "Head of Sales", "linkedin": "https://il.linkedin.com/in/tamirtausi", "source": "LinkedIn", "email": "tamir@tikalk.com"},
    ],
    "hio": [
        {"name": "Golan Agmon", "title": "Founder", "linkedin": "https://www.linkedin.com/in/golan-agmon-27484b6/", "source": "CTech", "email": ""},
    ],
    "augury": [
        {"name": "Gal Shaul", "title": "Co-Founder & CPTO", "linkedin": "https://il.linkedin.com/in/gal-shaul-427a5a38", "source": "LinkedIn", "email": "gshaul@augury.com"},
        {"name": "Saar Yoskovitz", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/saaryoskovitz/", "source": "GeekTime", "email": "syoskovitz@augury.com"},
    ],
    "pagaya": [
        {"name": "Avital Pardo", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/avital-pardo-257408b7/", "source": "Calcalist", "email": "avital.pardo@pagaya.com"},
        {"name": "Dina Leventol Nimrodi", "title": "Director of Research Engineering", "linkedin": "https://www.linkedin.com/in/dina-leventol-nimrodi-309a8395/", "source": "LinkedIn", "email": ""},
    ],
    "forter": [
        {"name": "Eran Vanounou", "title": "CTO", "linkedin": "https://www.linkedin.com/in/eran-vanounou-983684a/", "source": "LinkedIn", "email": "eran.vanounou@forter.com"},
        {"name": "Oren Ellenbogen", "title": "SVP Engineering", "linkedin": "https://il.linkedin.com/in/orenellenbogen", "source": "GitHub", "email": "oren.ellenbogen@forter.com"},
        {"name": "Jonathan Long", "title": "Sr Director Talent Acquisition", "linkedin": "https://www.linkedin.com/in/jonathan-long-23215693/", "source": "LinkedIn", "email": "jonathan.long@forter.com"},
        {"name": "Michael Reitblat", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/reitblat/", "source": "LinkedIn", "email": ""},
    ],
    "lightricks": [
        {"name": "Yaron Inger", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yaroninger/", "source": "Crunchbase", "email": "yaron@lightricks.com"},
        {"name": "Alon Roth", "title": "Engineering Manager", "linkedin": "https://www.linkedin.com/in/alonroth/", "source": "LinkedIn", "email": "alon@lightricks.com"},
        {"name": "Noa Lichtenstein", "title": "Engineering Manager, AI Photo Tools", "linkedin": "https://www.linkedin.com/in/noa-licht/", "source": "LinkedIn", "email": "noa@lightricks.com"},
    ],
    "cloudinary": [
        {"name": "Tal Lev-Ami", "title": "Co-Founder & CTO", "linkedin": "https://il.linkedin.com/in/tallevami", "source": "LinkedIn", "email": "tal.levami@cloudinary.com"},
        {"name": "Itai Lahan", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/itail/", "source": "LinkedIn", "email": "itai.lahan@cloudinary.com"},
        {"name": "Nadav Soferman", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/nadavsoferman/", "source": "LinkedIn", "email": "nadav.soferman@cloudinary.com"},
    ],
    "guidde": [
        {"name": "Yoav Einav", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yoav-einav-58354323/", "source": "LinkedIn", "email": "yoav.einav@guidde.com"},
        {"name": "Dan Sahar", "title": "CPO & Co-Founder", "linkedin": "https://www.linkedin.com/in/dansahar/", "source": "LinkedIn", "email": "dan.sahar@guidde.com"},
    ],
    "unframe": [
        {"name": "Shay Levi", "title": "Co-Founder & CEO", "linkedin": "https://il.linkedin.com/in/shaylevi2", "source": "Globes", "email": "shay.levi@unframe.ai"},
        {"name": "Adi Azarya", "title": "Co-Founder & VP R&D", "linkedin": "https://il.linkedin.com/in/adiazarya", "source": "LinkedIn", "email": "adi.azarya@unframe.ai"},
        {"name": "Larissa Schneider", "title": "COO & Co-Founder", "linkedin": "https://www.linkedin.com/in/schneiderlarissa/", "source": "LinkedIn", "email": "larissa.schneider@unframe.ai"},
    ],
    "fundamental": [
        {"name": "Jeremy Fraenkel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/jeremy-fraenkel/", "source": "CTech", "email": ""},
    ],
    "bmc": [
        {"name": "Ram Chakravarti", "title": "CTO", "linkedin": "https://www.linkedin.com/in/ramchak/", "source": "Company Website", "email": ""},
    ],
    "leidos": [
        {"name": "Jim Carlini", "title": "CTO", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "matia": [
        {"name": "Benjamin Segal", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/benjamin-segal/", "source": "LinkedIn", "email": "benjamin@matia.io"},
        {"name": "Geva Segal", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gevasegal/", "source": "LinkedIn", "email": "geva@matia.io"},
    ],
    "kpmg": [
        {"name": "Dina Pasca-Raz", "title": "Partner, Head of Technology", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "somekhchaikin": [
        {"name": "Dina Pasca-Raz", "title": "Partner, Head of Technology", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "taboola": [
        {"name": "Tal Sliwowicz", "title": "Senior VP R&D - Infrastructure", "linkedin": "https://www.linkedin.com/in/talsliwowicz/", "source": "LinkedIn", "email": "tal@taboola.com"},
        {"name": "Lior Golan", "title": "CTO", "linkedin": "https://www.linkedin.com/in/liorgolan/", "source": "LinkedIn", "email": "lior.golan@taboola.com"},
        {"name": "Anjali Oldfield", "title": "Head of HR, EMEA & APAC", "linkedin": "https://www.linkedin.com/in/anjalioldfield/", "source": "LinkedIn", "email": "anjali@taboola.com"},
    ],
    "cyberark": [
        {"name": "Udi Mokady", "title": "Founder & Executive Chairman", "linkedin": "https://www.linkedin.com/in/udimokady/", "source": "LinkedIn", "email": "udi.mokady@cyberark.com"},
        {"name": "Matt Cohen", "title": "CEO", "linkedin": "https://www.linkedin.com/in/mattjcohen/", "source": "LinkedIn", "email": "matt.cohen@cyberark.com"},
        {"name": "Kathy Cullen-Cote", "title": "Chief People Officer", "linkedin": "https://www.linkedin.com/in/kathy-cullen/", "source": "LinkedIn", "email": "kathy.cullen-cote@cyberark.com"},
    ],
    "wiz": [
        {"name": "Assaf Rappaport", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/assafrappaport/", "source": "LinkedIn", "email": "assaf@wiz.io"},
        {"name": "Roy Reznik", "title": "Co-Founder, VP R&D", "linkedin": "https://www.linkedin.com/in/roy-reznik-a8b822189/", "source": "LinkedIn", "email": "roy@wiz.io"},
        {"name": "Arik Nemtsov", "title": "Director of Engineering", "linkedin": "https://www.linkedin.com/in/arik-nemtsov-b9516578/", "source": "LinkedIn", "email": "arik@wiz.io"},
    ],
    "fireblocks": [
        {"name": "Michael Shaulov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/michaelsh/", "source": "LinkedIn", "email": "michael@fireblocks.com"},
        {"name": "Pavel Berengoltz", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/pavelberengoltz/", "source": "LinkedIn", "email": "pavel@fireblocks.com"},
        {"name": "Idan Ofrat", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/idanofrat/", "source": "LinkedIn", "email": "idan@fireblocks.com"},
    ],
    "mobileye": [
        {"name": "Amnon Shashua", "title": "President & CEO, Founder", "linkedin": "https://www.linkedin.com/in/amnon-shashua/", "source": "LinkedIn", "email": "amnon.shashua@mobileye.com"},
        {"name": "Shai Shalev-Shwartz", "title": "CTO", "linkedin": "https://www.linkedin.com/in/shai-shalev-shwartz/", "source": "LinkedIn", "email": "shai@mobileye.com"},
    ],
    "silverfort": [
        {"name": "Hed Kovetz", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/hedkovetz/", "source": "LinkedIn", "email": "hed@silverfort.com"},
        {"name": "Ben Livne", "title": "Senior VP R&D", "linkedin": "https://www.linkedin.com/in/benlivne/", "source": "LinkedIn", "email": "ben@silverfort.com"},
        {"name": "Yiftach Keshet", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yiftachkeshet/", "source": "LinkedIn", "email": "yiftach@silverfort.com"},
    ],
    "similarweb": [
        {"name": "Ron Asher", "title": "CTO", "linkedin": "https://www.linkedin.com/in/ronasher/", "source": "LinkedIn", "email": "ron.asher@similarweb.com"},
        {"name": "Or Offer", "title": "CEO", "linkedin": "https://www.linkedin.com/in/oroffer/", "source": "LinkedIn", "email": "or.offer@similarweb.com"},
    ],
    "pentera": [
        {"name": "Amitai Ratzon", "title": "CEO", "linkedin": "https://www.linkedin.com/in/amitairatzon/", "source": "LinkedIn", "email": "amitai@pentera.io"},
        {"name": "Arik Liberzon", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/arikliberzon/", "source": "LinkedIn", "email": "arik@pentera.io"},
    ],
    "au10tix": [
        {"name": "Dan Yerushalmi", "title": "CEO", "linkedin": "https://www.linkedin.com/in/danyerushalmi/", "source": "LinkedIn", "email": "dan@au10tix.com"},
    ],
    "audiocodes": [
        {"name": "Shabtai Adlersberg", "title": "President & CEO", "linkedin": "https://www.linkedin.com/in/shabtai-adlersberg/", "source": "LinkedIn", "email": "shabtai.adlersberg@audiocodes.com"},
        {"name": "Niran Baruch", "title": "VP Finance & CFO", "linkedin": "https://www.linkedin.com/in/niran-baruch/", "source": "LinkedIn", "email": "niran.baruch@audiocodes.com"},
    ],
    "biocatch": [
        {"name": "Gadi Mazor", "title": "CEO", "linkedin": "https://www.linkedin.com/in/gadimazor/", "source": "LinkedIn", "email": "gadi@biocatch.com"},
        {"name": "Avi Turgeman", "title": "Founder & CTO", "linkedin": "https://www.linkedin.com/in/aviturgeman/", "source": "LinkedIn", "email": "avi@biocatch.com"},
    ],
    "cymulate": [
        {"name": "Eyal Wachsman", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/eyalwachsman/", "source": "LinkedIn", "email": "eyal@cymulate.com"},
        {"name": "Avihai Ben-Yossef", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/avihai-ben-yossef/", "source": "LinkedIn", "email": "avihai@cymulate.com"},
    ],
    "doit": [
        {"name": "Vadim Solovey", "title": "CTO & Managing Director", "linkedin": "https://www.linkedin.com/in/vadimska/", "source": "LinkedIn", "email": "vadim@doit.com"},
        {"name": "Yoav Toussia-Cohen", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/yoavtc/", "source": "LinkedIn", "email": "yoav@doit.com"},
    ],
    "fetcherr": [
        {"name": "Roi Dover", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/roidover/", "source": "LinkedIn", "email": "roi@fetcherr.io"},
        {"name": "Roy Friedman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/royfriedman1/", "source": "LinkedIn", "email": "roy@fetcherr.io"},
    ],
    "zscaler": [
        {"name": "Jay Chaudhry", "title": "CEO, Chairman & Founder", "linkedin": "https://www.linkedin.com/in/jaychaudhry/", "source": "LinkedIn", "email": "jay@zscaler.com"},
    ],
    "allcloud": [
        {"name": "Roman Koterman", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/roman-koterman/", "source": "LinkedIn", "email": "rkoterman@allcloud.io"},
    ],
    "applied materials": [
        {"name": "Nir Yogev", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/nir-yogev-0a2a2618/", "source": "LinkedIn", "email": "nir.yogev@amat.com"},
        {"name": "Anat Tzur", "title": "Director of DevOps", "linkedin": "https://www.linkedin.com/in/anat-tzur/", "source": "LinkedIn", "email": "anat.tzur@amat.com"},
    ],
    "aquasec": [
        {"name": "Dror Davidoff", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/drordavidoff/", "source": "LinkedIn", "email": "dror@aquasec.com"},
        {"name": "Amir Jerbi", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/amirjerbi/", "source": "LinkedIn", "email": "amir@aquasec.com"},
    ],
    "codevalue": [
        {"name": "Lior Fridman", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/lior-fridman-8a07906/", "source": "LinkedIn", "email": "lior@codevalue.com"},
    ],
    "globallogic": [
        {"name": "Nitzan Shapira", "title": "Country Manager Israel", "linkedin": "https://www.linkedin.com/in/nitzanshapira/", "source": "LinkedIn", "email": "nitzan.shapira@globallogic.com"},
    ],
    "team8": [
        {"name": "Nadav Zafrir", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadavzafrir/", "source": "LinkedIn", "email": "nadav@team8.vc"},
        {"name": "Israel Grimberg", "title": "Partner", "linkedin": "https://www.linkedin.com/in/israelgrimberg/", "source": "LinkedIn", "email": "israel@team8.vc"},
    ],
    "paragon": [
        {"name": "Idan Nurick", "title": "CEO", "linkedin": "https://www.linkedin.com/in/idannurick/", "source": "LinkedIn", "email": ""},
    ],
    "plainid": [
        {"name": "Oren Ohayon Harel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/oren-ohayon-harel/", "source": "LinkedIn", "email": "oren@plainid.com"},
        {"name": "Gal Helemski", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/galhelemski/", "source": "LinkedIn", "email": "gal@plainid.com"},
    ],
    "pango": [
        {"name": "Hari Ravichandran", "title": "Founder & CEO (Aura)", "linkedin": "https://www.linkedin.com/in/hariravichandran/", "source": "LinkedIn", "email": ""},
    ],
    "terasky": [
        {"name": "Alon Barel", "title": "CEO & Founder", "linkedin": "https://www.linkedin.com/in/alonbarel/", "source": "LinkedIn", "email": "alon@terasky.com"},
    ],
    "voyantis": [
        {"name": "Ido Benmoshe", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/idobenmoshe/", "source": "LinkedIn", "email": "ido@voyantis.com"},
    ],
    "wavebl": [
        {"name": "Noam Ohana", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/noamohana/", "source": "LinkedIn", "email": "noam@wavebl.com"},
    ],
    "metalbear": [
        {"name": "Aviram Hassan", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/aviramhassan/", "source": "LinkedIn", "email": "aviram@metalbear.co"},
        {"name": "Eyal Bukchin", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/eyal-bukchin/", "source": "LinkedIn", "email": "eyal@metalbear.co"},
    ],
    "quantummachines": [
        {"name": "Itamar Sivan", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/itamarsivan/", "source": "LinkedIn", "email": "itamar@quantum-machines.co"},
        {"name": "Yonatan Cohen", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yonatancohen/", "source": "LinkedIn", "email": "yonatan@quantum-machines.co"},
    ],
    "chaoslabs": [
        {"name": "Omer Goldberg", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/omergoldberg/", "source": "LinkedIn", "email": "omer@chaoslabs.xyz"},
    ],
    "next insurance": [
        {"name": "Guy Goldstein", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/guygoldstein/", "source": "LinkedIn", "email": "guy@nextinsurance.com"},
        {"name": "Alon Huri", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/alonhuri/", "source": "LinkedIn", "email": "alon@nextinsurance.com"},
    ],
    "cato networks": [
        {"name": "Shlomo Kramer", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/shlomokramer/", "source": "LinkedIn", "email": "shlomo@catonetworks.com"},
        {"name": "Gur Shatz", "title": "Co-Founder & COO", "linkedin": "https://www.linkedin.com/in/gurshatz/", "source": "LinkedIn", "email": "gur@catonetworks.com"},
    ],
    "datadog": [
        {"name": "Olivier Pomel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/olivierpomel/", "source": "LinkedIn", "email": "olivier@datadoghq.com"},
        {"name": "Alexis Le-Quoc", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/alexislequoc/", "source": "LinkedIn", "email": "alexis@datadoghq.com"},
    ],
    "unity": [
        {"name": "Matt Bromberg", "title": "CEO", "linkedin": "https://www.linkedin.com/in/mattbromberg/", "source": "LinkedIn", "email": "matt.bromberg@unity3d.com"},
    ],
    "appcharge": [
        {"name": "Maor Sauron", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/maorsauron/", "source": "LinkedIn", "email": "maor@appcharge.com"},
    ],
    "blinkops": [
        {"name": "Gil Barak", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gilbarak/", "source": "LinkedIn", "email": "gil@blinkops.com"},
    ],
    "harmonya": [
        {"name": "Eran Lupo", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/eranlupo/", "source": "LinkedIn", "email": "eran@harmonya.com"},
    ],
    "oligosecurity": [
        {"name": "Nadav Czerninski", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadavczerninski/", "source": "LinkedIn", "email": "nadav@oligo.security"},
    ],
    "minimus": [
        {"name": "Matan Derman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/matanderman/", "source": "LinkedIn", "email": "matan@minimus.io"},
    ],
    "nextta": [
        {"name": "Oded Shopen", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/odedshopen/", "source": "LinkedIn", "email": "oded@nextta.com"},
    ],
    "remedio": [
        {"name": "Tal Peer", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/talpeer/", "source": "LinkedIn", "email": "tal@gytpol.com"},
    ],
    "imagen": [
        {"name": "Oron Branitzky", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/oronbranitzky/", "source": "LinkedIn", "email": "oron@imagen.io"},
    ],
    "marvin": [
        {"name": "Hila Qu", "title": "CEO", "linkedin": "https://www.linkedin.com/in/hilaqu/", "source": "LinkedIn", "email": "hila@marvin.com"},
    ],
    "aqua security": [
        {"name": "Dror Davidoff", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/drordavidoff/", "source": "LinkedIn", "email": "dror@aquasec.com"},
        {"name": "Amir Jerbi", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/amirjerbi/", "source": "LinkedIn", "email": "amir@aquasec.com"},
    ],
    "oligo security": [
        {"name": "Gal Elbaz", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gal-elbaz-2b70b214/", "source": "LinkedIn", "email": "gal@oligo.security"},
        {"name": "Nadav Czerninski", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadavcz/", "source": "CTech", "email": "nadav@oligo.security"},
    ],
    "quantum machines": [
        {"name": "Itamar Sivan", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/itamarsivan/", "source": "LinkedIn", "email": "itamar@quantum-machines.co"},
        {"name": "Yonatan Cohen", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yonatan-cohen-05684411/", "source": "Calcalist", "email": "yonatan@quantum-machines.co"},
    ],
    "mastercard": [
        {"name": "Jorn Lambert", "title": "Chief Product Officer", "linkedin": "https://www.linkedin.com/in/jornlambert/", "source": "LinkedIn", "email": ""},
    ],
    "salesforce": [
        {"name": "Oren Winter", "title": "SVP Engineering, Israel R&D", "linkedin": "https://www.linkedin.com/in/oren-winter-89571a/", "source": "LinkedIn", "email": ""},
    ],
    "blink ops": [
        {"name": "Gil Barak", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gilbarak/", "source": "LinkedIn", "email": "gil@blinkops.com"},
        {"name": "Raz Itzhakian", "title": "CTO", "linkedin": "https://www.linkedin.com/in/razitzhakian/", "source": "LinkedIn", "email": ""},
    ],
    "chaos labs": [
        {"name": "Omer Goldberg", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/omer-goldberg/", "source": "LinkedIn", "email": "omer@chaoslabs.xyz"},
    ],
    "tavily": [
        {"name": "Lior Gross", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/liorgross/", "source": "LinkedIn", "email": "lior@tavily.com"},
    ],
    "quanthealth": [
        {"name": "Orr Inbar", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/orrinbar/", "source": "LinkedIn", "email": "orr@quanthealth.com"},
    ],
    "cato networks": [
        {"name": "Shlomo Kramer", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/shlomokramer/", "source": "Globes", "email": ""},
        {"name": "Eyal Heiman", "title": "CTO", "linkedin": "https://www.linkedin.com/in/eyal-heiman-99233a98/", "source": "LinkedIn", "email": ""},
    ],
    "global payments": [
        {"name": "Josh Whipple", "title": "CFO & Senior EVP", "linkedin": "https://www.linkedin.com/in/joshwhipple/", "source": "LinkedIn", "email": ""},
    ],
    "yael group": [
        {"name": "Doron Gigi", "title": "CEO", "linkedin": "https://www.linkedin.com/in/doron-gigi-93123823/", "source": "LinkedIn", "email": ""},
        {"name": "Amit Dover", "title": "Deputy CEO & CTO", "linkedin": "https://www.linkedin.com/in/amitdover/", "source": "Company Website", "email": ""},
    ],
    "terasky": [
        {"name": "Or Yaacov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/oryaacov/", "source": "LinkedIn", "email": "or@terasky.com"},
    ],
    "torq": [
        {"name": "Leonid Belkind", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/leonidbelkind/", "source": "LinkedIn", "email": ""},
        {"name": "Ofer Smadari", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/ofersmadari/", "source": "LinkedIn", "email": ""},
    ],
    "axonius": [
        {"name": "Ofri Shur", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/ofri-shur-3683a13b/", "source": "LinkedIn", "email": ""},
        {"name": "Dean Sysman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/deansysman/", "source": "LinkedIn", "email": ""},
    ],
    "freightos": [
        {"name": "Enric Alventosa", "title": "CTO", "linkedin": "https://www.linkedin.com/in/enric-alventosa-04469180/", "source": "LinkedIn", "email": ""},
    ],
    "linearb": [
        {"name": "Yishai Beeri", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yishaibeeri/", "source": "LinkedIn", "email": ""},
        {"name": "Ori Keren", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/ori-keren-8254965/", "source": "LinkedIn", "email": ""},
    ],
    "upstream": [
        {"name": "Yonatan Appel", "title": "CTO", "linkedin": "https://www.linkedin.com/in/yonatan-appel-5895223/", "source": "LinkedIn", "email": ""},
        {"name": "Yoav Levy", "title": "CEO", "linkedin": "https://www.linkedin.com/in/yoav-levy-117b2b1/", "source": "LinkedIn", "email": ""},
    ],
    "armis": [
        {"name": "Nadir Izrael", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadiriz/", "source": "LinkedIn", "email": ""},
        {"name": "Yevgeny Dibrov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yevgenydibrov/", "source": "LinkedIn", "email": ""},
    ],
    "armissecurity": [
        {"name": "Nadir Izrael", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadiriz/", "source": "LinkedIn", "email": ""},
        {"name": "Yevgeny Dibrov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yevgenydibrov/", "source": "LinkedIn", "email": ""},
    ],
    "deloitte": [
        {"name": "Ronen Sigal", "title": "Managing Partner, Israel", "linkedin": "https://www.linkedin.com/in/ronensigal/", "source": "LinkedIn", "email": ""},
    ],
    "motorolasolutions": [
        {"name": "Benny Dvir", "title": "Sr. Director Solutions & Services, Israel", "linkedin": "https://www.linkedin.com/in/bennydvir/", "source": "LinkedIn", "email": ""},
    ],
    "motorola solutions": [
        {"name": "Benny Dvir", "title": "Sr. Director Solutions & Services, Israel", "linkedin": "https://www.linkedin.com/in/bennydvir/", "source": "LinkedIn", "email": ""},
    ],
    "ness technologies": [
        {"name": "Sudip Singh", "title": "CEO", "linkedin": "https://www.linkedin.com/in/sudipsingh/", "source": "LinkedIn", "email": ""},
    ],
    "samsung": [
        {"name": "Ilan Elias", "title": "VP & Head of Samsung Israel R&D Center", "linkedin": "https://www.linkedin.com/in/ilan-elias-6766b54/", "source": "LinkedIn", "email": ""},
    ],
    "sqlink": [
        {"name": "Tamir Goren", "title": "CEO", "linkedin": "https://www.linkedin.com/in/tamir-goren-8b666a4/", "source": "LinkedIn", "email": ""},
    ],
    "comm it": [
        {"name": "Ilan Sokolov", "title": "CTO", "linkedin": "https://www.linkedin.com/in/ilan-sokolov/", "source": "LinkedIn", "email": ""},
    ],
    "gett": [
        {"name": "Yaki Zakai", "title": "CTO", "linkedin": "https://il.linkedin.com/in/yaki-zakai-62847", "source": "LinkedIn", "email": ""},
        {"name": "Matteo de Renzi", "title": "CEO", "linkedin": "https://uk.linkedin.com/in/matteoderenzi", "source": "LinkedIn", "email": ""},
    ],
    "wix": [
        {"name": "Yoav Abrahami", "title": "CTO & Head of Wix Engineering", "linkedin": "https://www.linkedin.com/in/yoavabrahami/", "source": "LinkedIn", "email": ""},
        {"name": "Nir Zohar", "title": "President & COO", "linkedin": "https://www.linkedin.com/in/nirzohar/", "source": "LinkedIn", "email": ""},
    ],
    "cloudflare": [
        {"name": "John Graham-Cumming", "title": "CTO", "linkedin": "https://www.linkedin.com/in/jgrahamc/", "source": "LinkedIn", "email": ""},
    ],
    "intel": [
        {"name": "Greg Lavender", "title": "CTO & SVP", "linkedin": "https://www.linkedin.com/in/greg-lavender-9539724/", "source": "LinkedIn", "email": ""},
    ],
    "intel corporation": [
        {"name": "Greg Lavender", "title": "CTO & SVP", "linkedin": "https://www.linkedin.com/in/greg-lavender-9539724/", "source": "LinkedIn", "email": ""},
    ],
    "atera": [
        {"name": "Gil Pekelman", "title": "CEO", "linkedin": "https://www.linkedin.com/in/gilpekelman/", "source": "LinkedIn", "email": ""},
        {"name": "Oshri Moyal", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/oshrimoyal/", "source": "LinkedIn", "email": ""},
    ],
    "cloudera": [
        {"name": "Dipto Chakravarty", "title": "Chief Engineering Officer", "linkedin": "https://www.linkedin.com/in/diptochakravarty/", "source": "LinkedIn", "email": ""},
    ],
    "accenture": [
        {"name": "Karthik Narain", "title": "CEO, Accenture Technology", "linkedin": "https://www.linkedin.com/in/karthiknarain/", "source": "LinkedIn", "email": ""},
    ],
    "medtronic": [
        {"name": "Ken Washington", "title": "SVP & CTO", "linkedin": "https://www.linkedin.com/in/kenwashington/", "source": "LinkedIn", "email": ""},
    ],
    "carrier": [
        {"name": "James Pisz", "title": "VP Digital Solutions & CTO", "linkedin": "https://www.linkedin.com/in/jamespisz/", "source": "LinkedIn", "email": ""},
    ],
    "gsk": [
        {"name": "Karenann Terrell", "title": "Chief Digital & Technology Officer", "linkedin": "https://www.linkedin.com/in/karenannterrell/", "source": "LinkedIn", "email": ""},
    ],
    "xsolla": [
        {"name": "Chris Hewish", "title": "CEO", "linkedin": "https://www.linkedin.com/in/chrishewish/", "source": "LinkedIn", "email": ""},
    ],
    "cast ai": [
        {"name": "Laurent Gil", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/laurentgil/", "source": "LinkedIn", "email": ""},
        {"name": "Yuri Frayman", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yurifrayman/", "source": "LinkedIn", "email": ""},
    ],
    "cast.ai": [
        {"name": "Laurent Gil", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/laurentgil/", "source": "LinkedIn", "email": ""},
        {"name": "Yuri Frayman", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yurifrayman/", "source": "LinkedIn", "email": ""},
    ],
}

SEARCH_QUERIES = [
    # LinkedIn individual job listings (highest quality)
    "site:linkedin.com/jobs/view DevOps Engineer Israel",
    "site:linkedin.com/jobs/view Senior DevOps Engineer Israel",
    "site:linkedin.com/jobs/view AI Engineer Israel",
    "site:linkedin.com/jobs/view Machine Learning Engineer Israel",
    "site:linkedin.com/jobs/view Platform Engineer Israel",
    "site:linkedin.com/jobs/view MLOps Engineer Israel",
    "site:linkedin.com/jobs/view SRE Israel",
    "site:linkedin.com/jobs/view Cloud Engineer Israel",
    "site:linkedin.com/jobs/view Agentic AI Israel",
    "site:linkedin.com/jobs/view DevSecOps Israel",
    "site:linkedin.com/jobs/view Infrastructure Engineer Israel",
    "site:linkedin.com/jobs/view Data Engineer Israel",
    "site:linkedin.com/jobs/view Backend Engineer Israel",
    # Career sites and job boards
    "DevOps Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "AI Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    # Note: removed apple.com/microsoft.com/google.com — their SPA career pages
    # don't expose structured location data, causing false positives (e.g. India jobs on /en-il/ locale)
    "DevOps Engineer Israel site:workday.com OR site:myworkdayjobs.com",
    # Comeet (Israeli ATS with structured data)
    "site:comeet.com/jobs DevOps Engineer Israel",
    "site:comeet.com/jobs AI Engineer Israel",
    "site:comeet.com/jobs Cloud Engineer Israel",
    "site:comeet.com/jobs SRE Israel",
    "site:comeet.com/jobs Infrastructure Engineer Israel",
    # FinOps roles
    "site:linkedin.com/jobs/view FinOps Engineer Israel",
    "site:linkedin.com/jobs/view FinOps Analyst Israel",
    "site:linkedin.com/jobs/view Cloud Cost Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Financial Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Cost Optimization Israel",
    "FinOps Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "FinOps Israel site:comeet.com/jobs",
    "FinOps Israel site:workday.com OR site:myworkdayjobs.com",
    # General web searches
    "DevOps Engineer Israel hiring 2026",
    "AI Engineer Israel job 2026",
    "Agentic Developer Israel job",
    "Platform Engineer Israel hiring",
    "MLOps Engineer Israel job",
    "SRE Israel job 2026",
    "Cloud Engineer Israel job 2026",
    "Infrastructure Engineer Israel hiring",
    "FinOps Engineer Israel hiring 2026",
    "Cloud Cost Optimization Engineer Israel job",
    "Cloud Financial Management Israel job",
    # Solutions Architect / Sales Engineer roles (companies hiring these likely need DevOps help)
    "site:linkedin.com/jobs/view Solutions Architect Israel cloud OR kubernetes OR DevOps",
    "site:linkedin.com/jobs/view Sales Engineer Israel cloud OR DevOps OR infrastructure",
]

_DEFAULT_CATEGORY_KEYWORDS = {
    "agentic": ["agentic", "agent", "llm agent", "autonomous agent", "ai agent", "sales agent"],
    "ai": ["ai engineer", "machine learning", "ml engineer", "mlops", "data scientist",
            "deep learning", "nlp", "llm", "generative ai", "genai", "artificial intelligence",
            "ai ops", "large scale training"],
    "finops": ["finops", "fin ops", "cloud cost", "cloud financial", "cost optimization",
               "cloud economics", "cloud spend", "cost management", "cloud billing",
               "cost engineer", "cloud finance", "cost analyst"],
    "security": ["devsecops", "security engineer", "appsec", "application security",
                  "cybersecurity", "infosec", "information security", "cloud security",
                  "security architect", "penetration test", "soc analyst", "threat",
                  "vulnerability", "compliance engineer", "security operations"],
    "sre": ["sre", "site reliability", "reliability engineer", "production engineer",
             "availability engineer", "incident management", "on-call", "observability"],
    "platform": ["platform engineer", "platform team", "internal developer platform",
                  "developer experience", "developer platform", "idp ", "backstage",
                  "platform infrastructure", "developer productivity"],
    "data": ["data engineer", "data pipeline", "data platform", "etl", "elt ",
              "data warehouse", "data lake", "apache spark", "apache kafka",
              "data infrastructure", "analytics engineer", "dbt ", "airflow",
              "databricks", "snowflake engineer"],
    "cloud": ["cloud engineer", "cloud architect", "cloud infrastructure",
              "aws engineer", "azure engineer", "gcp engineer", "multi-cloud",
              "cloud migration", "cloud native", "cloud operations"],
    "devops": ["devops", "ci/cd", "kubernetes", "terraform", "docker",
               "infrastructure as code", "iac", "jenkins", "gitops", "argocd",
               "helm", "ansible", "puppet", "chef"],
}

def _load_category_keywords() -> dict:
    """Load category keywords from template_settings.json if available, else use defaults."""
    settings_path = os.path.join(os.path.dirname(__file__), "template_settings.json")
    try:
        with open(settings_path, "r") as f:
            settings = json.load(f)
        kw = settings.get("categories", {}).get("keywords")
        if kw and isinstance(kw, dict):
            logging.info(f"Loaded {len(kw)} category keyword sets from template_settings.json")
            return kw
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    logging.info("Using default category keywords")
    return _DEFAULT_CATEGORY_KEYWORDS

CATEGORY_KEYWORDS = _load_category_keywords()

# ── LinkedIn FTS (Free Text Search) ─────────────────────────────────────────
# Search LinkedIn posts for hiring announcements (e.g. "Hiring DevOps Israel")
# Uses search engines as proxy — never scrapes LinkedIn directly.
# Only 2-3 categories are searched per run (rotation) to stay under radar.
LINKEDIN_FTS_QUERIES_PER_CATEGORY = {
    "devops":   [
        'site:linkedin.com/posts "hiring" "devops" "Israel"',
        'site:linkedin.com/posts "hiring" "DevOps Engineer" "Israel"',
    ],
    "ai":       [
        'site:linkedin.com/posts "hiring" "AI Engineer" "Israel"',
        'site:linkedin.com/posts "hiring" "Machine Learning" "Israel"',
        'site:linkedin.com/posts "hiring" "MLOps" "Israel"',
    ],
    "cloud":    [
        'site:linkedin.com/posts "hiring" "Cloud Engineer" "Israel"',
        'site:linkedin.com/posts "hiring" "Cloud Architect" "Israel"',
    ],
    "platform": [
        'site:linkedin.com/posts "hiring" "Platform Engineer" "Israel"',
        'site:linkedin.com/posts "hiring" "Developer Platform" "Israel"',
    ],
    "sre":      [
        'site:linkedin.com/posts "hiring" "SRE" "Israel"',
        'site:linkedin.com/posts "hiring" "Site Reliability" "Israel"',
    ],
    "security": [
        'site:linkedin.com/posts "hiring" "Security Engineer" "Israel"',
        'site:linkedin.com/posts "hiring" "DevSecOps" "Israel"',
    ],
    "data":     [
        'site:linkedin.com/posts "hiring" "Data Engineer" "Israel"',
        'site:linkedin.com/posts "hiring" "Data Platform" "Israel"',
    ],
    "finops":   [
        'site:linkedin.com/posts "hiring" "FinOps" "Israel"',
        'site:linkedin.com/posts "hiring" "Cloud Cost" "Israel"',
    ],
    "agentic":  [
        'site:linkedin.com/posts "hiring" "Agentic" "Israel"',
        'site:linkedin.com/posts "hiring" "AI Agent" "Israel"',
    ],
}
# How many categories to search per run (rotation)
LINKEDIN_FTS_CATS_PER_RUN = 3
# Max queries per category per run
LINKEDIN_FTS_MAX_QUERIES_PER_CAT = 1
# File to track which categories were searched last, for round-robin rotation
LINKEDIN_FTS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "linkedin_fts_state.json")

SOURCE_MAP = {
    "linkedin.com": "linkedin",
    "glassdoor.com": "glassdoor",
    "alljobs.co.il": "alljobs",
    "drushim.co.il": "drushim",
    "builtin.com": "builtin",
    "facebook.com": "facebook",
    "t.me": "telegram",
    "goozali": "goozali",
    "greenhouse.io": "greenhouse",
    "lever.co": "lever",
    "ashbyhq.com": "ashby",
    "comeet.com": "comeet",
    "myworkdayjobs.com": "workday",
}


# ── Seed Jobs (manually curated listings that search engines don't index well) ─
SEED_JOBS = [
    {
        "title": "Senior FinOps Engineer",
        "snippet": "Check Point Software - Tel Aviv District, Israel (Hybrid). Managing and optimizing cloud costs across AWS, Azure, and GCP.",
        "url": "https://www.linkedin.com/jobs/view/senior-finops-engineer-at-check-point-software-technologies-ltd",
    },
    {
        "title": "Senior Cloud FinOps Engineer",
        "snippet": "Deloitte - Tel Aviv District, Israel (Hybrid). Cloud financial management and cost optimization consulting.",
        "url": "https://www.linkedin.com/jobs/view/senior-cloud-finops-engineer-at-deloitte",
    },
    {
        "title": "FinOps Engineer",
        "snippet": "Wix.com - Tel Aviv, Israel. Cloud cost management, optimization, and financial operations for large-scale cloud infrastructure.",
        "url": "https://www.linkedin.com/jobs/view/finops-engineer-at-wix",
    },
    {
        "title": "FinOps Analyst",
        "snippet": "IronSource (Unity) - Tel Aviv, Israel. Cloud cost analysis, budgeting, and forecasting for multi-cloud environments.",
        "url": "https://www.linkedin.com/jobs/view/finops-analyst-at-unity",
    },
    {
        "title": "Cloud Cost Optimization Engineer",
        "snippet": "CyberArk - Petah Tikva, Israel. FinOps practices, cloud spend optimization, and cost governance across AWS and Azure.",
        "url": "https://www.linkedin.com/jobs/view/cloud-cost-optimization-engineer-at-cyberark",
    },
    {
        "title": "FinOps Lead",
        "snippet": "Playtika - Herzliya, Israel. Leading FinOps practice, cloud cost management strategy, and financial reporting for cloud infrastructure.",
        "url": "https://www.linkedin.com/jobs/view/finops-lead-at-playtika",
    },
    {
        "title": "Senior FinOps Engineer",
        "snippet": "SolarEdge - Herzliya, Israel. Cloud financial operations, cost optimization, and cross-team cloud governance.",
        "url": "https://www.linkedin.com/jobs/view/senior-finops-engineer-at-solaredge",
    },
    {
        "title": "Cloud FinOps Specialist",
        "snippet": "NICE - Ra'anana, Israel. Cloud cost management, FinOps framework implementation, and cost optimization for SaaS platform.",
        "url": "https://www.linkedin.com/jobs/view/cloud-finops-specialist-at-nice",
    },
    {
        "title": "FinOps Engineer",
        "snippet": "Taboola - Tel Aviv, Israel. Cloud cost optimization, billing analysis, and financial governance for large-scale ad-tech infrastructure.",
        "url": "https://www.linkedin.com/jobs/view/finops-engineer-at-taboola",
    },
    {
        "title": "FinOps & Cloud Cost Analyst",
        "snippet": "Fiverr - Tel Aviv, Israel. Cloud financial management, cost analytics, and optimization recommendations across AWS.",
        "url": "https://www.linkedin.com/jobs/view/finops-cloud-cost-analyst-at-fiverr",
    },
]


# ── Search Functions ───────────────────────────────────────────────────────

def search_serpapi(query: str) -> list[dict]:
    """Search using SerpAPI (free tier: 100/month)."""
    if not SERPAPI_KEY:
        return []
    try:
        resp = requests.get("https://serpapi.com/search", params={
            "q": query, "api_key": SERPAPI_KEY, "gl": "il", "hl": "en", "num": 10
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for r in data.get("organic_results", []):
            # Combine snippet with rich_snippet text and date for better parsing
            snippet = r.get("snippet", "")
            rich = r.get("rich_snippet", {})
            if rich:
                # Rich snippets may contain additional text with dates
                for v in rich.values():
                    if isinstance(v, dict):
                        for sv in v.values():
                            if isinstance(sv, str) and sv not in snippet:
                                snippet = f"{snippet} {sv}"
            results.append({
                "title": r.get("title", ""),
                "snippet": snippet,
                "url": r.get("link", ""),
                "date": r.get("date", ""),  # SerpAPI sometimes returns date
            })
        return results
    except Exception as e:
        log.warning(f"SerpAPI search failed: {e}")
        return []


GOOGLE_JOBS_QUERIES = [
    ("FinOps Engineer", "Tel Aviv, Israel"),
    ("FinOps", "Israel"),
    ("Cloud FinOps", "Israel"),
    ("Cloud Cost Engineer", "Israel"),
    ("DevOps Engineer", "Israel"),
    ("Platform Engineer", "Israel"),
    ("SRE", "Israel"),
    ("AI Engineer", "Israel"),
    ("MLOps Engineer", "Israel"),
    ("Agentic AI Engineer", "Israel"),
]


def search_google_jobs() -> list[dict]:
    """Search using SerpAPI's Google Jobs engine for structured job listings."""
    if not SERPAPI_KEY:
        return []
    all_results = []
    for query, location in GOOGLE_JOBS_QUERIES:
        try:
            resp = requests.get("https://serpapi.com/search", params={
                "engine": "google_jobs",
                "q": query,
                "location": location,
                "api_key": SERPAPI_KEY,
                "hl": "en",
            }, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if "error" in data:
                log.warning(f"Google Jobs API error: {data['error']}")
                break  # Likely out of quota, stop trying
            for r in data.get("jobs_results", []):
                # Extract the best apply URL
                url = ""
                for opt in r.get("apply_options", []):
                    link = opt.get("link", "")
                    if link:
                        url = link
                        break
                if not url:
                    url = r.get("share_link", "")
                if not url:
                    continue
                title = r.get("title", "")
                company = r.get("company_name", "")
                location_str = r.get("location", "")
                description = r.get("description", "")[:500]
                snippet = f"{company} - {location_str}. {description}"
                all_results.append({
                    "title": f"{title} at {company}",
                    "snippet": snippet,
                    "url": url,
                    "date": "",
                })
            log.info(f"Google Jobs '{query}' in {location}: {len(data.get('jobs_results', []))} results")
        except Exception as e:
            log.warning(f"Google Jobs search failed for '{query}': {e}")
    return all_results


def search_duckduckgo(query: str) -> list[dict]:
    """Search using DuckDuckGo HTML (no API key needed)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(
            f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
            headers=headers, timeout=15
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for r in soup.select(".result"):
            title_el = r.select_one(".result__a")
            snippet_el = r.select_one(".result__snippet")
            if title_el:
                url = title_el.get("href", "")
                # DuckDuckGo wraps URLs in a redirect
                if "uddg=" in url:
                    from urllib.parse import parse_qs, urlparse
                    parsed = urlparse(url)
                    qs = parse_qs(parsed.query)
                    url = qs.get("uddg", [url])[0]
                results.append({
                    "title": title_el.get_text(strip=True),
                    "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                    "url": url,
                })
        return results[:10]
    except Exception as e:
        log.warning(f"DuckDuckGo search failed for '{query}': {e}")
        return []


def search_jobs(query: str) -> list[dict]:
    """Search with DuckDuckGo first, fall back to SerpAPI (conserve SerpAPI quota)."""
    results = search_duckduckgo(query)
    if not results:
        time.sleep(random.uniform(1.5, 3.0))  # Rate limiting
        results = search_serpapi(query)
    return results


def _load_linkedin_fts_state() -> dict:
    """Load LinkedIn FTS rotation state (which categories were searched last)."""
    if os.path.exists(LINKEDIN_FTS_STATE_PATH):
        try:
            with open(LINKEDIN_FTS_STATE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_cats": [], "seen_urls": []}


def _save_linkedin_fts_state(state: dict):
    """Save LinkedIn FTS rotation state."""
    try:
        with open(LINKEDIN_FTS_STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Could not save LinkedIn FTS state: {e}")


def _pick_fts_categories() -> list[str]:
    """Pick categories for this run using round-robin rotation."""
    all_cats = list(LINKEDIN_FTS_QUERIES_PER_CATEGORY.keys())
    state = _load_linkedin_fts_state()
    last_cats = set(state.get("last_cats", []))

    # Prefer categories NOT searched last time
    unsearched = [c for c in all_cats if c not in last_cats]
    if len(unsearched) < LINKEDIN_FTS_CATS_PER_RUN:
        # All categories were searched recently; reset and pick fresh
        unsearched = all_cats

    random.shuffle(unsearched)
    picked = unsearched[:LINKEDIN_FTS_CATS_PER_RUN]
    return picked


def _extract_fts_job_info(title: str, snippet: str, url: str) -> dict | None:
    """Extract job info from a LinkedIn post search result.

    LinkedIn posts are hiring announcements, not job listings. The title/snippet
    typically looks like:
      "John Smith on LinkedIn: We're hiring a DevOps Engineer in Tel Aviv!"
      "Acme Corp posted on LinkedIn: Join our team as a Cloud Engineer..."

    Returns a dict with title, company, snippet, url or None if not extractable.
    """
    title_lower = title.lower()
    snippet_lower = snippet.lower()
    combined = f"{title} {snippet}".lower()

    # Must be a LinkedIn post URL
    if "linkedin.com/posts/" not in url.lower() and "linkedin.com/feed/" not in url.lower():
        return None

    # Must contain hiring-related signals
    hiring_signals = ["hiring", "we're hiring", "we are hiring", "join our team",
                      "looking for", "open position", "open role", "new role",
                      "come join", "join us", "growing our team", "expanding our team",
                      "new opening", "hot job", "dream team", "seeking a"]
    if not any(sig in combined for sig in hiring_signals):
        return None

    # Extract company name from LinkedIn post title patterns
    # Pattern: "Name at Company: ..." or "Name | Company: ..."
    company = ""
    # "FirstName LastName on LinkedIn: ..." — company from snippet
    # "Company posted on LinkedIn: ..."
    company_match = re.search(r'^(.+?)\s+posted\s+on\s+LinkedIn', title)
    if company_match:
        company = company_match.group(1).strip()
    else:
        # Try "Name at Company" or "Name | Company" in title
        at_match = re.search(r'(?:at|@|\|)\s+([A-Z][^:|\-]+?)(?:\s*[-:|]|\s+on\s+LinkedIn)', title)
        if at_match:
            company = at_match.group(1).strip()
        else:
            # Try snippet: "Company is hiring..." or "At Company, we..."
            snip_match = re.search(r'^(?:at\s+)?([A-Z][A-Za-z0-9\s&.]+?)(?:\s*,\s*we|\s+is\s+(?:hiring|looking|growing))', snippet)
            if snip_match:
                company = snip_match.group(1).strip()

    # Clean company name
    if company:
        company = re.sub(r'\s+on\s+LinkedIn.*', '', company).strip()
        company = re.sub(r'\s*\|.*', '', company).strip()
        # Remove if it looks like a person's name (two words, both capitalized)
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', company):
            company = ""  # Likely a person name, not company

    # Extract job title from the post content
    job_title = ""
    # Look for common patterns: "hiring a DevOps Engineer", "looking for a Cloud Architect"
    role_match = re.search(
        r'(?:hiring\s+(?:a\s+)?|looking\s+for\s+(?:a\s+)?|open\s+(?:role|position)\s*[-:]\s*|'
        r'seeking\s+(?:a\s+)?|new\s+role\s*[-:]\s*)'
        r'([A-Z][A-Za-z/\s&]+?)(?:\s+in\s+|\s+at\s+|\s*[!.,\-]|\s+to\s+|\s+who\s+|$)',
        f"{title} {snippet}"
    )
    if role_match:
        job_title = role_match.group(1).strip()
        # Trim common trailing words
        job_title = re.sub(r'\s+(?:to|in|at|for|who|that|with)$', '', job_title, flags=re.IGNORECASE)
        # Reject if it looks like a person name (e.g. "Israel Zalmanov")
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', job_title):
            job_title = ""

    if not job_title:
        # Fall back: use a standard role title based on the category keyword found
        _cat_role_titles = {
            "devops": "DevOps Engineer", "ai": "AI Engineer", "cloud": "Cloud Engineer",
            "platform": "Platform Engineer", "sre": "Site Reliability Engineer",
            "security": "Security Engineer", "data": "Data Engineer",
            "finops": "FinOps Engineer", "agentic": "AI/Agentic Engineer",
        }
        for cat, keywords in CATEGORY_KEYWORDS.items():
            for kw in keywords:
                if kw.lower() in combined:
                    job_title = _cat_role_titles.get(cat, kw.title())
                    break
            if job_title:
                break

    if not job_title:
        return None  # Can't determine what role this is about

    # Build the display title
    display_title = job_title
    if company:
        display_title = f"{job_title} at {company}"

    # Use first 120 chars of snippet as description
    desc = snippet[:120] if snippet else title[:120]

    return {
        "title": display_title[:80],
        "snippet": desc,
        "url": url,
        "company": company or "Unknown",
        "_source_override": "linkedin_fts",
    }


def search_linkedin_fts() -> list[dict]:
    """Search LinkedIn posts for hiring announcements via DuckDuckGo/SerpAPI.

    Uses round-robin category rotation: only LINKEDIN_FTS_CATS_PER_RUN categories
    are searched each run. Results are LinkedIn post URLs with extracted job info.
    No LinkedIn pages are scraped directly.
    """
    state = _load_linkedin_fts_state()
    seen_urls = set(state.get("seen_urls", [])[-500:])  # Keep last 500 URLs for dedup
    picked_cats = _pick_fts_categories()
    log.info(f"LinkedIn FTS: searching categories {picked_cats}")

    all_results = []

    for cat in picked_cats:
        queries = LINKEDIN_FTS_QUERIES_PER_CATEGORY.get(cat, [])
        # Pick random subset of queries for this category
        random.shuffle(queries)
        selected_queries = queries[:LINKEDIN_FTS_MAX_QUERIES_PER_CAT]

        for query in selected_queries:
            log.info(f"  LinkedIn FTS query: {query}")
            results = search_duckduckgo(query)
            if not results:
                time.sleep(random.uniform(2.0, 4.0))
                results = search_serpapi(query)

            for r in results:
                url = r.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                job_info = _extract_fts_job_info(
                    r.get("title", ""),
                    r.get("snippet", ""),
                    url
                )
                if job_info:
                    all_results.append(job_info)
                    log.info(f"    Found: {job_info['title'][:60]}")

            # Random delay between queries (3-8 seconds)
            time.sleep(random.uniform(3.0, 8.0))

    # Save state for next run
    state["last_cats"] = picked_cats
    state["seen_urls"] = list(seen_urls)[-500:]
    _save_linkedin_fts_state(state)

    log.info(f"LinkedIn FTS: found {len(all_results)} hiring posts")
    return all_results


# ── Date Extraction ───────────────────────────────────────────────────────

def extract_posting_date(url: str) -> str:
    """Try to scrape the real posting date from a job listing page.
    Returns ISO date string (YYYY-MM-DD) or empty string if not found."""
    if not url:
        return ""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return ""
        text = resp.text[:100000]  # Limit to first 100KB

        # 1. JSON-LD structured data (most reliable — used by LinkedIn, many career sites)
        ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
        for ld_raw in ld_matches:
            try:
                ld = json.loads(ld_raw)
                # Handle both single object and array
                items = ld if isinstance(ld, list) else [ld]
                for item in items:
                    # JobPosting schema
                    if item.get("@type") == "JobPosting":
                        date_posted = item.get("datePosted", "")
                        if date_posted:
                            return _normalize_date(date_posted)
                    # Check nested items
                    if isinstance(item.get("@graph"), list):
                        for g in item["@graph"]:
                            if g.get("@type") == "JobPosting":
                                date_posted = g.get("datePosted", "")
                                if date_posted:
                                    return _normalize_date(date_posted)
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        # 2. "datePosted" anywhere in page (inline JSON, JS variables, etc.)
        m = re.search(r'"datePosted"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
        if m:
            return m.group(1)

        # 2b. Meta tags (og:article:published_time, datePublished, etc.)
        meta_patterns = [
            r'<meta[^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\']',
        ]
        for pat in meta_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return _normalize_date(m.group(1))

        # 2c. Any JSON field with "date" in key and ISO date value
        date_json = re.findall(r'"(?:date_?(?:posted|published|created|listed)?)"\s*:\s*"(\d{4}-\d{2}-\d{2}[T\s]?[^"]*)"', text, re.IGNORECASE)
        if date_json:
            return _normalize_date(date_json[0])

        # 2d. ISO dates near posting-related keywords in raw HTML/JS
        posting_date_ctx = re.findall(
            r'(?:post|publish|list|creat|updat)(?:ed|_at|At|Date|Time|_date|_time).{0,30}?(\d{4}-\d{2}-\d{2})',
            text, re.IGNORECASE
        )
        if posting_date_ctx:
            return posting_date_ctx[0]

        # 3. Relative date patterns in visible text ("Posted 3 days ago", "2 weeks ago")
        relative_patterns = [
            (r'(?:posted|published|listed)\s+(\d+)\s+day', "days"),
            (r'(?:posted|published|listed)\s+(\d+)\s+week', "weeks"),
            (r'(?:posted|published|listed)\s+(\d+)\s+month', "months"),
            (r'(?:posted|published|listed)\s+(\d+)\s+hour', "hours"),
            (r'(\d+)\s+days?\s+ago', "days"),
            (r'(\d+)\s+weeks?\s+ago', "weeks"),
            (r'(\d+)\s+months?\s+ago', "months"),
            (r'(\d+)\s+hours?\s+ago', "hours"),
        ]
        from datetime import timedelta
        for pat, unit in relative_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                n = int(m.group(1))
                now = datetime.now(timezone.utc)
                if unit == "hours":
                    dt = now - timedelta(hours=n)
                elif unit == "days":
                    dt = now - timedelta(days=n)
                elif unit == "weeks":
                    dt = now - timedelta(weeks=n)
                elif unit == "months":
                    dt = now - timedelta(days=n * 30)
                return dt.strftime("%Y-%m-%d")

    except Exception as e:
        log.debug(f"Date extraction failed for {url[:60]}: {e}")
    return ""


def scrape_job_page(url: str) -> dict:
    """Scrape a job listing page for date, company name, closed status, and location.
    Returns {"date": "YYYY-MM-DD" or "", "company": "name" or "", "closed": bool, "location_country": "", "is_career_page": bool}."""
    result = {"date": "", "company": "", "closed": False, "location_country": "", "is_career_page": False, "_http_status": 0}
    if not url:
        return result
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            log.info(f"  Scrape {url[:60]}: status={resp.status_code} (non-200, skipping)")
            result["_http_status"] = resp.status_code
            return result
        text = resp.text[:100000]  # Limit to first 100KB
        final_url = resp.url  # URL after redirects
        result["_http_status"] = 200
        log.info(f"  Scrape {url[:60]}: status={resp.status_code}, size={len(resp.text)}, truncated={len(text)}")

        # ── Detect career/multi-listing pages (e.g. expired Greenhouse job IDs redirect to careers page) ──
        # 1. Check <title> for career page patterns
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', text, re.IGNORECASE)
        if title_match:
            page_title = title_match.group(1).strip().lower()
            career_title_patterns = [
                r'^current\s+openings?\s+(at|@)\s+',
                r'^open\s+positions?\s+(at|@)\s+',
                r'^(careers|career\s+opportunities)\s+(at|@)\s+',
                r'^(all|current|available)\s+(open\s+)?(positions|jobs|roles|openings)\s',
                r'^jobs?\s+(at|@)\s+',
                r'^(join\s+us|join\s+our\s+team|we\'?re\s+hiring)',
                r'^[\w\s]+\s*[-\|]\s*careers?\s*$',
                r'\bcareer\s*(?:page|portal|site|hub)\b',
            ]
            for pat in career_title_patterns:
                if re.search(pat, page_title):
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE (title): '{page_title[:60]}' for {url[:60]}")
                    break

        # 2. For ATS URLs (Greenhouse, Lever, etc.): detect if redirected away from specific job
        if not result["is_career_page"]:
            url_lower = url.lower()
            final_lower = final_url.lower()
            # Greenhouse: original URL had /jobs/\d+ but final URL lost it
            if 'greenhouse.io' in url_lower and re.search(r'/jobs/\d+', url_lower):
                if not re.search(r'/jobs/\d+', final_lower):
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE (redirect lost job ID): {url[:60]} → {final_url[:60]}")
            # Lever: original URL had specific path but redirected to company root
            if 'lever.co' in url_lower and url_lower.count('/') > 4:
                if final_lower.rstrip('/').count('/') <= 3:
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE (lever redirect): {url[:60]} → {final_url[:60]}")

        # 3. Check for multiple job listing links on the page (strong signal of a career page)
        if not result["is_career_page"]:
            # Count distinct job links on the page (Greenhouse pattern: /jobs/\d+)
            if 'greenhouse.io' in (final_url or url).lower():
                job_links = set(re.findall(r'/jobs/(\d+)', text))
                if len(job_links) > 5:
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE ({len(job_links)} job links): {url[:60]}")

        if result["is_career_page"]:
            return result

        # ── Check if listing is closed ──
        closed_phrases = [
            "no longer accepting applications",
            "this job is no longer available",
            "this position has been filled",
            "this job has expired",
            "job closed",
            "listing has been removed",
            "application closed",
        ]
        text_lower_check = text.lower()
        for phrase in closed_phrases:
            if phrase in text_lower_check:
                result["closed"] = True
                log.info(f"  CLOSED: {url[:60]} — '{phrase}'")
                break

        # ── Check for stale time-ago indicators (e.g. "3 months ago") ──
        # For LinkedIn: only check "posted X ago" context, not any "X ago" on the page,
        # because LinkedIn sidebars/recommendations contain unrelated relative dates.
        if not result["closed"]:
            if "linkedin.com" in url:
                stale_match = re.search(
                    r'(?:posted|listed|published)\s+(\d+)\s+(month|year)s?\s+ago',
                    text_lower_check
                )
            else:
                stale_match = re.search(
                    r'(\d+)\s+(month|year)s?\s+ago',
                    text_lower_check
                )
            if stale_match:
                num = int(stale_match.group(1) if "linkedin.com" not in url else stale_match.group(1))
                unit = stale_match.group(2) if "linkedin.com" not in url else stale_match.group(2)
                if unit == "year" or (unit == "month" and num >= 1):
                    result["closed"] = True
                    log.info(f"  CLOSED (stale): {url[:60]} — '{stale_match.group(0)}'")

        # LinkedIn: check for JSON-LD (indicates active listing)
        if "linkedin.com" in url:
            has_job_ld = bool(re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>.*?"@type"\s*:\s*"JobPosting"',
                text, re.DOTALL
            ))
            result["_has_job_ld"] = has_job_ld  # pass this info downstream
            # Note: missing JSON-LD alone doesn't mean closed — LinkedIn often
            # blocks JSON-LD from data center IPs. Only explicit closed phrases count.

        # ── Extract company name (especially from LinkedIn) ──
        # LinkedIn: "companyName" in inline JSON
        cm = re.search(r'"companyName"\s*:\s*"([^"]{2,60})"', text)
        if cm:
            result["company"] = cm.group(1).strip()
        # LinkedIn: topcard org name
        if not result["company"]:
            cm = re.search(r'class="topcard__org-name[^"]*"[^>]*>([^<]{2,60})', text)
            if cm:
                result["company"] = cm.group(1).strip()
        # JSON-LD hiringOrganization
        if not result["company"]:
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
            for ld_raw in ld_matches:
                try:
                    ld = json.loads(ld_raw)
                    items = ld if isinstance(ld, list) else [ld]
                    for item in items:
                        if item.get("@type") == "JobPosting":
                            org = item.get("hiringOrganization", {})
                            if isinstance(org, dict) and org.get("name"):
                                result["company"] = org["name"].strip()
                                break
                        if isinstance(item.get("@graph"), list):
                            for g in item["@graph"]:
                                if g.get("@type") == "JobPosting":
                                    org = g.get("hiringOrganization", {})
                                    if isinstance(org, dict) and org.get("name"):
                                        result["company"] = org["name"].strip()
                                        break
                    if result["company"]:
                        break
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue

        # ── Extract location/country from page (for non-Israel filtering) ──
        # JSON-LD jobLocation → addressCountry
        ld_matches_loc = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
        for ld_raw in ld_matches_loc:
            try:
                ld = json.loads(ld_raw)
                items = ld if isinstance(ld, list) else [ld]
                for item in items:
                    jp = None
                    if item.get("@type") == "JobPosting":
                        jp = item
                    elif isinstance(item.get("@graph"), list):
                        for g in item["@graph"]:
                            if g.get("@type") == "JobPosting":
                                jp = g
                                break
                    if jp:
                        loc = jp.get("jobLocation", {})
                        if isinstance(loc, list):
                            loc = loc[0] if loc else {}
                        if isinstance(loc, dict):
                            addr = loc.get("address", {})
                            if isinstance(addr, dict):
                                country = addr.get("addressCountry", "")
                                if isinstance(country, dict):
                                    country = country.get("name", "")
                                if country:
                                    result["location_country"] = country.strip()
                                    log.info(f"  Location country: {result['location_country']} for {url[:60]}")
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        # LinkedIn: look for country in the page text
        if not result["location_country"] and "linkedin.com" in url:
            # LinkedIn often has "Location: City, Country" or "addressCountry":"XX"
            country_match = re.search(r'"addressCountry"\s*:\s*"([^"]+)"', text)
            if country_match:
                result["location_country"] = country_match.group(1).strip()
                log.info(f"  LinkedIn addressCountry: {result['location_country']} for {url[:60]}")

        # Apple careers: look for location in page
        if not result["location_country"] and "apple.com" in url:
            # Apple career pages often have location details
            loc_match = re.search(r'"location(?:Name)?"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
            if loc_match:
                loc_text = loc_match.group(1)
                result["location_country"] = loc_text.strip()
                log.info(f"  Apple location: {result['location_country']} for {url[:60]}")

        # ── Extract posting date ──
        # 0. Comeet "time_updated" in POSITION_DATA
        if "comeet.com" in url:
            cm = re.search(r'"time_updated"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
            if cm:
                result["date"] = cm.group(1)
                log.info(f"  Comeet time_updated: {result['date']} for {url[:60]}")
            # Company from POSITION_DATA
            if not result["company"]:
                pos_data = re.search(r'POSITION_DATA\s*=\s*(\{[^;]+)', text)
                if pos_data:
                    try:
                        pd = json.loads(pos_data.group(1))
                        # Company from the URL slug
                        cslug = re.search(r'comeet\.com/jobs/([^/]+)', url)
                        if cslug:
                            result["company"] = cslug.group(1).replace('-', ' ').title()
                    except (json.JSONDecodeError, TypeError):
                        pass

        # 0a. LinkedIn "listedAt" Unix timestamp in milliseconds (most precise for LinkedIn)
        if "linkedin.com" in url:
            listed_at = re.search(r'"listedAt"\s*:\s*(\d{13})', text)
            if listed_at:
                ts_ms = int(listed_at.group(1))
                result["date"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                log.info(f"  LinkedIn listedAt: {result['date']} for {url[:60]}")

        # NOTE: LinkedIn <time> tags are NOT reliable for posting dates.
        # They often belong to recommendation cards, sidebar content, etc.
        # Only listedAt JSON timestamp (extracted above) is reliable for LinkedIn.

        # 1. JSON-LD datePosted (most reliable for non-LinkedIn)
        if not result["date"]:
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
            for ld_raw in ld_matches:
                try:
                    ld = json.loads(ld_raw)
                    items = ld if isinstance(ld, list) else [ld]
                    for item in items:
                        if item.get("@type") == "JobPosting":
                            date_posted = item.get("datePosted", "")
                            if date_posted:
                                result["date"] = _normalize_date(date_posted)
                                break
                        if isinstance(item.get("@graph"), list):
                            for g in item["@graph"]:
                                if g.get("@type") == "JobPosting":
                                    date_posted = g.get("datePosted", "")
                                    if date_posted:
                                        result["date"] = _normalize_date(date_posted)
                                        break
                    if result["date"]:
                        break
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue

        # 2. "datePosted" anywhere in page (inline JSON / JS)
        if not result["date"]:
            m = re.search(r'"datePosted"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
            if m:
                result["date"] = m.group(1)

        # 2b. Meta tags
        if not result["date"]:
            meta_patterns = [
                r'<meta[^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\'][^>]*content=["\']([^"\']+)["\']',
                r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\']',
            ]
            for pat in meta_patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    result["date"] = _normalize_date(m.group(1))
                    break

        # 2c. Any JSON "date*" field with ISO date value
        if not result["date"]:
            date_json = re.findall(r'"(?:date_?(?:posted|published|created|listed)?)"\s*:\s*"(\d{4}-\d{2}-\d{2}[T\s]?[^"]*)"', text, re.IGNORECASE)
            if date_json:
                result["date"] = _normalize_date(date_json[0])

        # 2d. ISO dates near posting keywords
        if not result["date"]:
            posting_date_ctx = re.findall(
                r'(?:post|publish|list|creat|updat)(?:ed|_at|At|Date|Time|_date|_time).{0,30}?(\d{4}-\d{2}-\d{2})',
                text, re.IGNORECASE
            )
            if posting_date_ctx:
                result["date"] = posting_date_ctx[0]

        # 3. Relative date patterns (prefer "posted/published X ago" over raw "X ago")
        if not result["date"]:
            from datetime import timedelta
            relative_patterns = [
                (r'(?:posted|published|listed)\s+(\d+)\s+day', "days"),
                (r'(?:posted|published|listed)\s+(\d+)\s+week', "weeks"),
                (r'(?:posted|published|listed)\s+(\d+)\s+month', "months"),
                (r'(?:posted|published|listed)\s+(\d+)\s+hour', "hours"),
                (r'(\d+)\s+days?\s+ago', "days"),
                (r'(\d+)\s+weeks?\s+ago', "weeks"),
                (r'(\d+)\s+months?\s+ago', "months"),
                (r'(\d+)\s+hours?\s+ago', "hours"),
            ]
            for pat, unit in relative_patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    n = int(m.group(1))
                    now = datetime.now(timezone.utc)
                    if unit == "hours":
                        dt = now - timedelta(hours=n)
                    elif unit == "days":
                        dt = now - timedelta(days=n)
                    elif unit == "weeks":
                        dt = now - timedelta(weeks=n)
                    elif unit == "months":
                        dt = now - timedelta(days=n * 30)
                    result["date"] = dt.strftime("%Y-%m-%d")
                    break

    except Exception as e:
        log.debug(f"Page scrape failed for {url[:60]}: {e}")
    return result


def _normalize_date(raw: str) -> str:
    """Normalize various date formats to YYYY-MM-DD."""
    raw = raw.strip()
    # Already ISO format: 2026-03-01 or 2026-03-01T...
    m = re.match(r'(\d{4}-\d{2}-\d{2})', raw)
    if m:
        return m.group(1)
    # Formats like "March 1, 2026" or "1 March 2026"
    try:
        from datetime import datetime as dt_cls
        for fmt in ("%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d %b %Y",
                    "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return dt_cls.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return ""


# ── Parsing Functions ──────────────────────────────────────────────────────

def detect_source(url: str) -> str:
    """Detect job board source from URL."""
    url_lower = url.lower()
    for domain, source in SOURCE_MAP.items():
        if domain in url_lower:
            return source
    return "other"


def detect_category(title: str, snippet: str) -> str:
    """Detect job category from title and snippet.

    Title keywords are checked first (stronger signal) before description.
    This prevents a job titled 'Platform Engineer' from being classified as
    'sre' just because the description mentions SRE experience.
    """
    title_lower = title.lower()
    text = f"{title} {snippet}".lower()
    # Check most specific categories first, then broader ones
    # devops is always last (broadest catch-all), other categories in natural order
    all_cats = list(CATEGORY_KEYWORDS.keys())
    priority = [c for c in all_cats if c != "devops"] + (["devops"] if "devops" in all_cats else [])
    # Pass 1: Check TITLE only (strongest signal)
    for cat in priority:
        for kw in CATEGORY_KEYWORDS.get(cat, []):
            if kw in title_lower:
                return cat
    # Pass 2: Check full text (title + description)
    for cat in priority:
        for kw in CATEGORY_KEYWORDS.get(cat, []):
            if kw in text:
                return cat
    return "devops"  # Default


def _fetch_linkedin_photo(name: str, company: str, linkedin_url: str) -> str:
    """Find LinkedIn profile photo URL via SerpAPI Google Images.

    Returns a direct LinkedIn CDN URL (media.licdn.com) for the profile photo,
    or empty string if not found.
    """
    if not SERPAPI_KEY:
        return ""
    if not name:
        return ""
    try:
        # Search Google Images for the person's LinkedIn profile photo
        query = f'{name} {company} LinkedIn profile photo'
        resp = requests.get("https://serpapi.com/search.json", params={
            "engine": "google_images",
            "q": query,
            "api_key": SERPAPI_KEY,
            "num": 3,
        }, timeout=15)
        if resp.status_code != 200:
            return ""
        data = resp.json()
        # Look through image results for a LinkedIn CDN photo
        for r in data.get("images_results", [])[:5]:
            original = r.get("original", "")
            title = r.get("title", "").lower()
            # Must be from LinkedIn CDN and match the person
            if "media.licdn.com/dms/image" in original and "profile" in original:
                # Verify the title contains the person's name (first or last)
                name_parts = name.lower().split()
                if any(part in title for part in name_parts if len(part) > 2):
                    log.info(f"  Found photo for {name} via SerpAPI")
                    return original
        return ""
    except Exception as e:
        log.debug(f"Photo search failed for {name}: {e}")
        return ""


def _validate_linkedin_urls(jobs: list) -> list:
    """Validate stakeholder LinkedIn URLs by checking for 404s.
    Returns the jobs list with broken LinkedIn URLs cleared out."""
    checked = {}  # url → True (valid) / False (broken)
    broken_count = 0
    check_count = 0
    max_checks = 50  # Rate-limit to avoid hammering LinkedIn

    for j in jobs:
        for s in j.get("stakeholders", []):
            url = s.get("linkedin", "")
            if not url:
                continue
            if url in checked:
                if not checked[url]:
                    s["linkedin"] = ""
                continue
            if check_count >= max_checks:
                continue
            check_count += 1
            try:
                resp = requests.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html",
                    },
                    allow_redirects=True,
                    timeout=10,
                )
                # Only flag as broken if LinkedIn explicitly returns 404 or redirects to /404/
                is_broken = resp.status_code == 404 or "/404" in resp.url
                checked[url] = not is_broken
                if is_broken:
                    log.warning(f"  BROKEN LinkedIn: {s.get('name','')} → {url} (HTTP {resp.status_code}, final: {resp.url[:80]})")
                    s["linkedin"] = ""
                    broken_count += 1
            except Exception as e:
                log.debug(f"  LinkedIn check failed for {url}: {e}")
                checked[url] = True  # Assume valid on network error
            time.sleep(random.uniform(0.5, 1.5))

    log.info(f"  LinkedIn validation: checked {check_count} URLs, {broken_count} broken")
    return jobs


def _get_stakeholders(company: str) -> list:
    """Look up stakeholders for a company from the COMPANY_STAKEHOLDERS dict,
    falling back to automatic SerpAPI-based discovery when no manual entry exists."""
    if not company:
        return []
    company_lower = company.lower().strip()
    # Direct match
    if company_lower in COMPANY_STAKEHOLDERS:
        return COMPANY_STAKEHOLDERS[company_lower]
    # Partial match (e.g. "Check Point Software" matches "check point")
    for key, contacts in COMPANY_STAKEHOLDERS.items():
        if key in company_lower or company_lower in key:
            return contacts
    # Fuzzy match: remove spaces/hyphens and compare (e.g. "blinkops" matches "Blink Ops")
    company_squished = company_lower.replace(" ", "").replace("-", "")
    for key, contacts in COMPANY_STAKEHOLDERS.items():
        key_squished = key.replace(" ", "").replace("-", "")
        if key_squished in company_squished or company_squished in key_squished:
            return contacts
    # No manual entry — try auto-discovery
    return _auto_discover_stakeholders(company)


# ── Auto-stakeholder discovery cache ──────────────────────────────────────
_stakeholder_cache: dict[str, list] = {}   # company_lower → contacts list
_auto_discover_count = 0                    # Track SerpAPI usage per run
AUTO_DISCOVER_MAX = 5                       # Max auto-lookups per pipeline run (conserve SerpAPI quota)

# Leadership title patterns for auto-discovery
_LEADERSHIP_RE = re.compile(
    r'(?:CTO|Chief Technology Officer|Chief Executive Officer|CEO|'
    r'Co-?Founder|VP\s*(?:of\s+)?(?:R&D|Engineering|Research|Technology|Product)|'
    r'Head of (?:Engineering|R&D|Technology)|'
    r'Director of (?:Engineering|R&D)|'
    r'SVP\s+(?:Engineering|R&D)|'
    r'Sr\.?\s*Director\s+(?:Engineering|R&D)|'
    r'General Manager|Country Manager|Managing Director)',
    re.IGNORECASE
)
_SKIP_TITLE_RE = re.compile(
    r'recruiter|talent\s+acq|intern\b|junior|associate|analyst|student|'
    r'looking\s+for|seeking|open\s+to',
    re.IGNORECASE
)


def _parse_linkedin_search_result(result: dict, company_lower: str,
                                   seen_urls: set) -> dict | None:
    """Parse a single SerpAPI organic result into a stakeholder contact.
    Returns a contact dict or None if the result doesn't qualify."""
    link = result.get("link", "")
    title_text = result.get("title", "")
    snippet = result.get("snippet", "")

    # Must be a LinkedIn profile URL
    if "/in/" not in link or link in seen_urls:
        return None

    # Extract name and title from title line
    # Typical formats:
    #   "Name - Title - Company | LinkedIn"
    #   "Name - Company | LinkedIn"
    #   "Name | LinkedIn"
    name = ""
    person_title = ""
    clean_title = title_text.replace(" | LinkedIn", "").replace("| LinkedIn", "").strip()

    if " - " in clean_title:
        parts = [p.strip() for p in clean_title.split(" - ")]
        name = parts[0]
        if len(parts) >= 3:
            person_title = parts[1]
        elif len(parts) == 2:
            # Could be "Name - Title" or "Name - Company"
            if _LEADERSHIP_RE.search(parts[1]):
                person_title = parts[1]
    elif clean_title:
        name = clean_title.split("|")[0].strip()

    if not name or name.lower() == "linkedin" or len(name) < 3:
        return None

    # Verify the result is actually about this company
    combined = (title_text + " " + snippet).lower()
    company_words = [w for w in company_lower.split() if len(w) > 2]
    if company_words and not any(w in combined for w in company_words):
        # Also try squished match (e.g. "intel" in "intelcorporation")
        squished = company_lower.replace(" ", "")
        if squished not in combined.replace(" ", ""):
            return None

    # Extract/refine title from snippet if we don't have one yet
    if not person_title or not _LEADERSHIP_RE.search(person_title):
        title_match = _LEADERSHIP_RE.search(snippet)
        if title_match:
            # Grab the match and a bit of context
            start = title_match.start()
            end = min(start + 60, len(snippet))
            candidate = snippet[start:end].split("·")[0].split("|")[0].split("…")[0].strip().rstrip(",. ")
            if candidate:
                person_title = candidate

    # Must have a leadership title
    if not person_title or not _LEADERSHIP_RE.search(person_title):
        return None

    # Skip non-leadership profiles
    if _SKIP_TITLE_RE.search(person_title):
        return None

    seen_urls.add(link)
    return {
        "name": name,
        "title": person_title,
        "linkedin": link,
        "source": "Auto-discovered",
        "email": "",
    }


def _auto_discover_stakeholders(company: str) -> list:
    """Use SerpAPI to find CTO / VP R&D / VP Engineering for a company.
    Tries multiple search strategies and parses LinkedIn profiles from results."""
    global _auto_discover_count

    if not SERPAPI_KEY or not company:
        return []

    company_lower = company.lower().strip()

    # Skip companies that are actually job board names, not real employers
    skip_companies = {
        "unknown", "remoterockethub", "efinancialcareers", "jobgether",
        "techaviv", "play", "automatit", "efinancialcareers norway",
        "factored", "attil", "mksinst", "adaptive6",
    }
    if company_lower in skip_companies:
        _stakeholder_cache[company_lower] = []
        return []

    if company_lower in _stakeholder_cache:
        return _stakeholder_cache[company_lower]

    if _auto_discover_count >= AUTO_DISCOVER_MAX:
        _stakeholder_cache[company_lower] = []
        return []

    _auto_discover_count += 1
    contacts = []

    # Try multiple search queries — broader first, then specific
    queries = [
        f'{company} CTO OR CEO site:linkedin.com/in',
        f'{company} "VP Engineering" OR "VP R&D" OR "Head of R&D" site:linkedin.com/in',
    ]

    try:
        seen_urls = set()
        for query in queries:
            if len(contacts) >= 2:
                break
            resp = requests.get("https://serpapi.com/search", params={
                "q": query,
                "api_key": SERPAPI_KEY,
                "gl": "il",
                "hl": "en",
                "num": 5,
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            for r in data.get("organic_results", []):
                if len(contacts) >= 2:
                    break
                parsed = _parse_linkedin_search_result(r, company_lower, seen_urls)
                if parsed:
                    contacts.append(parsed)

            time.sleep(random.uniform(0.3, 0.8))

        if contacts:
            log.info(f"  Auto-discovered {len(contacts)} stakeholder(s) for {company}: "
                     f"{', '.join(c['name'] + ' (' + c['title'] + ')' for c in contacts)}")
        else:
            log.debug(f"  No stakeholders auto-discovered for {company}")

        time.sleep(random.uniform(0.5, 1.5))

    except Exception as e:
        log.debug(f"  Auto-discover failed for {company}: {e}")

    _stakeholder_cache[company_lower] = contacts
    return contacts


def _generate_outreach_messages(job: dict) -> None:
    """Generate personalized LinkedIn outreach messages for each stakeholder.
    Adds 'connectMsg' and 'followUpMsg' fields to each stakeholder dict."""
    company = job.get("company", "Unknown")
    job_title = job.get("title", "").split(" at ")[0].split(" - ")[0].strip()
    category = job.get("category", "devops")
    is_customer = job.get("isDeveleapCustomer", False)

    # Map categories to Develeap service descriptions
    service_map = {
        "devops": "DevOps & cloud-native transformation",
        "finops": "FinOps and cloud cost optimization",
        "ai": "AI/ML infrastructure and MLOps",
        "agentic": "Agentic AI and automation",
    }
    service = service_map.get(category, "DevOps & cloud engineering")

    for s in job.get("stakeholders", []):
        first_name = s.get("name", "").split()[0] if s.get("name") else "there"
        title = s.get("title", "")

        if is_customer:
            # Warm intro — they already know Develeap
            connect_msg = (
                f"Hi {first_name}, I'm Dori from Develeap. "
                f"I noticed {company} is growing the team with a {job_title} role — "
                f"great to see! As a current partner, I'd love to discuss how we can "
                f"support your scaling efforts. Would love to connect."
            )
            followup_msg = (
                f"Thanks for connecting, {first_name}! "
                f"Since Develeap already works with {company}, I wanted to reach out "
                f"about the {job_title} hiring. We often help teams ramp up faster "
                f"with interim {service} expertise while permanent hires onboard. "
                f"Would a quick chat be useful?"
            )
        else:
            # Cold outreach
            connect_msg = (
                f"Hi {first_name}, I noticed {company} is hiring a {job_title} — "
                f"sounds like exciting growth! I lead Develeap, an Israeli {service} "
                f"consultancy. Would love to connect and share how we help teams like "
                f"yours move faster."
            )
            followup_msg = (
                f"Thanks for connecting, {first_name}! "
                f"I wanted to share how Develeap helps companies like {company} "
                f"accelerate their {service} initiatives. We've worked with 50+ "
                f"Israeli tech companies on similar challenges. "
                f"Would you be open to a 15-min intro call this week?"
            )

        # LinkedIn connection notes have a 300-char limit
        if len(connect_msg) > 295:
            connect_msg = connect_msg[:292] + "..."

        s["connectMsg"] = connect_msg
        s["followUpMsg"] = followup_msg


def _company_matches(company: str, customer_list: list) -> bool:
    """Check if company name matches any entry in customer list (word-boundary aware)."""
    company_lower = company.lower().strip()
    for c in customer_list:
        c_lower = c.lower()
        if c_lower == company_lower:
            return True
        # Word-boundary match: "Aqua" matches "Aqua Security" but not "AquaFence"
        pattern = r'(?:^|[\s\-_])' + re.escape(c_lower) + r'(?:$|[\s\-_,.])'
        if re.search(pattern, company_lower):
            return True
    return False


def is_develeap_customer(company: str) -> bool:
    """Check if company is a current Develeap customer."""
    return _company_matches(company, DEVELEAP_CUSTOMERS)


def is_develeap_past_customer(company: str) -> bool:
    """Check if company is a past Develeap customer."""
    if is_develeap_customer(company):
        return False  # Active takes precedence
    return _company_matches(company, DEVELEAP_PAST_CUSTOMERS)


def _is_job_title(text: str) -> bool:
    """Return True if text looks like a job title rather than a company name."""
    t = text.lower().strip().rstrip(".")
    # Common job-title words / prefixes
    title_words = {
        "sr", "jr", "senior", "junior", "lead", "staff", "principal", "head",
        "chief", "director", "manager", "vp", "engineer", "developer", "architect",
        "analyst", "consultant", "specialist", "coordinator", "administrator",
        "technician", "intern", "trainee", "associate", "devops", "sre", "mlops",
        "cloud", "platform", "infrastructure", "data", "ai", "ml", "software",
        "backend", "frontend", "fullstack", "full-stack", "full stack",
        "technical", "tech", "site reliability", "security", "devsecops",
        "solution", "solutions", "product", "project", "program", "qa", "test",
        "automation", "release", "build", "deployment", "network",
        "database", "dba", "linux", "windows", "python", "java", "golang",
        "kubernetes", "terraform", "aws", "azure", "gcp", "remote", "hybrid",
        "israel", "tel aviv", "tel-aviv", "ramat gan", "herzliya", "haifa",
        "jerusalem", "beer sheva", "netanya", "petah tikva", "ra'anana",
        "hiring", "job", "jobs", "opening", "position",
        "vacancy", "career", "careers", "apply", "wanted", "looking for",
    }
    # Check if the entire text matches a known non-company phrase
    known_locations = {"tel aviv", "ramat gan", "herzliya", "haifa", "jerusalem",
                       "beer sheva", "netanya", "petah tikva", "ra'anana", "hod hasharon",
                       "israel", "remote", "hybrid", "tel aviv district", "tel aviv yaffo il",
                       "tel aviv yaffo", "il", "new", "2025", "2026", "2027"}
    if t in known_locations:
        return True

    # Contains Hebrew characters → not a valid company name for our purposes
    if re.search(r'[\u0590-\u05FF]', t):
        return True

    # Looks like a parenthetical description, not a company
    if t.startswith("(") or t.startswith("["):
        return True

    # Just a number/year
    if re.match(r'^\d+$', t):
        return True

    words = set(re.split(r"[\s/\-\.]+", t))
    # If most words are title-like, it's a job title
    if len(words) > 0 and len(words & title_words) / len(words) >= 0.5:
        return True
    # Starts with common title prefixes
    if re.match(r"^(sr\.?|jr\.?|senior|junior|lead|staff|principal|head of|chief|director)\b", t):
        return True
    # Ends with a job-role word — strong signal it's a title, not a company
    role_endings = {"engineer", "developer", "architect", "analyst", "consultant",
                    "specialist", "manager", "director", "coordinator", "administrator",
                    "technician", "intern", "designer", "scientist", "researcher",
                    "lead", "officer", "evangelist"}
    last_word = t.split()[-1] if t.split() else ""
    if last_word in role_endings:
        return True
    return False


def extract_company(title: str, snippet: str, url: str = "") -> str:
    """Try to extract company name from search result."""

    # Helper: clean up company name casing
    def _fix_casing(name: str) -> str:
        """Fix common casing issues in extracted company names."""
        # Known abbreviations that should stay uppercase
        abbrev = {"ai", "it", "bmc", "ibm", "sap", "hp", "aws", "gcp", "nso"}
        words = name.split()
        fixed = []
        for w in words:
            if w.lower() in abbrev:
                fixed.append(w.upper())
            else:
                fixed.append(w)
        return " ".join(fixed)

    # Known ATS slug → clean company name mapping
    ATS_SLUG_MAP = {
        "unity3d": "Unity",
        "pagayais": "Pagaya",
        "nextinsurance66": "Next Insurance",
        "catonetworks": "Cato Networks",
        "jobgether": "Jobgether",
        "castailabs": "Castai",
        "castaigroupinc": "CAST AI",
        "oligosecurity": "Oligo Security",
        "chaoslabs": "Chaos Labs",
        "blinkops": "Blink Ops",
        "quantummachines": "Quantum Machines",
        "majesticlabs": "Majestic Labs",
        "joinattil": "Attil",
        "phasev": "PhaseV",
        "quanthealth": "Quant Health",
        "aquasec": "Aqua Security",
        # Workday slugs
        "leidos": "Leidos",
        "mastercard": "Mastercard",
        "amat": "Applied Materials",
        "salesforce": "Salesforce",
    }

    # 0. ATS URL patterns — HIGHEST PRIORITY (most reliable source of company name)
    # Greenhouse / Lever / Ashby / Comeet / Workday URLs embed the company slug
    for ats_pat in [
        r"greenhouse\.io/([a-z0-9\-]+)/jobs",
        r"boards\.greenhouse\.io/([a-z0-9\-]+)",
        r"job-boards\.greenhouse\.io/([a-z0-9\-]+)",
        r"job-boards\.eu\.greenhouse\.io/([a-z0-9\-]+)",
        r"lever\.co/([a-z0-9\-]+)",
        r"jobs\.ashbyhq\.com/([a-z0-9\-]+)",
        r"jobs\.lever\.co/([a-z0-9\-]+)",
        r"comeet\.com/jobs/([a-z0-9\-]+)",
        r"([a-z0-9\-]+)\.wd\d+\.myworkdayjobs\.com",
    ]:
        m = re.search(ats_pat, url, re.IGNORECASE)
        if m:
            slug = m.group(1).lower()
            if slug in ATS_SLUG_MAP:
                return ATS_SLUG_MAP[slug]
            clean = slug.replace("-", " ").title()
            if len(clean) > 1:
                return _fix_casing(clean)

    # 0b. Hebrew LinkedIn title pattern: "COMPANY גיוס עובדים ROLE"
    heb_match = re.match(r'^([A-Za-z0-9\.\-\s&]+?)\s+גיוס\s+עובדים', title)
    if heb_match:
        company = heb_match.group(1).strip()
        if company and not _is_job_title(company):
            return _fix_casing(company)

    # 1. LinkedIn URL pattern: .../TITLE-at-COMPANY-1234567
    if "linkedin.com" in url:
        m = re.search(r"/jobs/view/.*?-at-(.+?)-\d{5,}", url)
        if m:
            company = _fix_casing(m.group(1).replace("-", " ").title())
            if not _is_job_title(company):
                return company

    # 1c. Known career site URL patterns: careers.COMPANY.com, jobs.COMPANY.com
    m = re.search(r"https?://(?:careers|jobs)\.([a-z0-9\-]+)\.", url)
    if m:
        domain_company = _fix_casing(m.group(1).replace("-", " ").title())
        if len(domain_company) > 2 and domain_company.lower() not in {
            "secret", "lhh", "secrettelaviv", "efinancial",
        }:
            return _fix_casing(domain_company)

    # 1c. COMPANY.com/careers or similar career page patterns
    m = re.search(r"https?://(?:www\.)?([a-z0-9\-]+)\.(?:com|io|co\.il|ai)/.+", url)
    if m:
        domain_company = m.group(1).replace("-", " ").title()
        # Only use domain as company for known career-hosting patterns
        job_boards = {
            "builtin", "startup", "glassdoor", "indeed", "alljobs", "drushim",
            "facebook", "google", "jobify360", "machinelearning", "aidevtlv",
            "linkedin", "secrettelaviv", "aijobs", "efinancialcareers",
            "monster", "ziprecruiter", "dice", "stackoverflow", "hired",
            "angel", "wellfound", "lever", "greenhouse", "workday",
            "jobify360", "goozali", "lhh",
        }
        if len(domain_company) > 2 and domain_company.lower() not in job_boards:
            # Verify the URL looks like a career/job page, not a random page
            if re.search(r"/(careers|jobs|position|openings|join|hiring|vacancy)", url, re.IGNORECASE):
                return _fix_casing(domain_company)

    # 2. "Role at Company" or "Role @ Company" pattern — use the LAST match
    at_pattern = r"(?:\bat|@)\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,]|\s+in\s+|\s+is\s+|\s*$)"
    all_at_matches = list(re.finditer(at_pattern, title))
    m = all_at_matches[-1] if all_at_matches else None
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    # 3. "Company - Role" or "Company | Role" (only if left side is NOT a job title)
    m = re.match(r"^([^-–|]{2,35}?)\s*[-–|]\s*(.+)", title)
    if m:
        left = m.group(1).strip()
        right = m.group(2).strip()
        # If left looks like a company (not a job title) → use it
        if not _is_job_title(left):
            return left
        # Otherwise try right side for "Role - Company" pattern
        # Take the last segment after the last dash/pipe
        parts = re.split(r"\s*[-–|]\s*", title)
        if len(parts) >= 2 and not _is_job_title(parts[-1].strip()):
            return parts[-1].strip()

    # 4. "Company is hiring" pattern
    m = re.search(r"([A-Z][A-Za-z0-9\.\-&]{1,25})\s+(?:is hiring|careers|jobs)", title + " " + snippet)
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    # 5. Try snippet with "at/@ Company" pattern
    m = re.search(r"(?:\bat|@)\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,\.]|\s+in\s+|\s+is\s+|\s*$)", snippet)
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    return "Unknown"


def extract_location(title: str, snippet: str) -> str:
    """Extract location from text."""
    text = f"{title} {snippet}"
    locations = {
        "tel aviv": "Tel Aviv", "ramat gan": "Ramat Gan", "herzliya": "Herzliya",
        "haifa": "Haifa", "jerusalem": "Jerusalem", "beer sheva": "Beer Sheva",
        "be'er sheva": "Beer Sheva", "netanya": "Netanya", "petah tikva": "Petah Tikva",
        "ra'anana": "Ra'anana", "raanana": "Ra'anana", "hod hasharon": "Hod HaSharon",
        "remote": "Remote", "hybrid": "Hybrid",
    }
    text_lower = text.lower()
    for key, val in locations.items():
        if key in text_lower:
            return val
    return "Israel"


def parse_search_results(raw_results: list[dict]) -> list[dict]:
    """Parse raw search results into structured job listings."""
    jobs = []
    seen_urls = set()

    for r in raw_results:
        url = r.get("url", "")
        title = r.get("title", "")
        snippet = r.get("snippet", "")

        # Skip duplicates and non-job URLs
        if url in seen_urls or not url:
            continue
        seen_urls.add(url)

        # Skip results that are clearly not job listings
        title_lower = title.lower()
        skip_keywords = ["how to", "salary", "resume", "interview tips", "career advice",
                         "blog", "article", "guide", "tutorial", "top 10", "best companies",
                         "average salary", "job description template", "what is a",
                         "conference", "meetup", "event", "webinar", "course",
                         "jobs in israel", "apply now", "remote jobs in",
                         "archives", "משרות דרושים", "as a service for startups"]
        if any(kw in title_lower for kw in skip_keywords):
            continue

        # Skip Hebrew aggregator pages ("we found N job offers", "jobs wanted")
        hebrew_skip = ["מצאנו", "הצעות עבודה", "משרות אחרונות", "חיפוש משרות"]
        if any(kw in title for kw in hebrew_skip):
            continue

        # Skip aggregator titles like "DevOps Engineer Jobs..." or "5 AI Engineer jobs..."
        if re.search(r'(?:^\d+\s+)?(?:.*?\bjobs?\b.*?\bin\b|.*?\bjobs?\b\s*\(\d+\))', title_lower):
            continue

        # Skip search/aggregator pages — only allow individual job listing URLs
        url_lower = url.lower()
        skip_url_patterns = [
            # Search result pages
            "google.com/search", "indeed.com/q-", "indeed.com/jobs?",
            "linkedin.com/jobs/search",
            # LinkedIn job search pages (e.g. /jobs/devops-engineer-jobs)
            # Only /jobs/view/ are individual listings
            "glassdoor.com/Job/",
            # Generic job listing indexes
            "/jobs?q=", "/search?",
        ]
        if any(p in url for p in skip_url_patterns):
            continue

        # LinkedIn: only accept /jobs/view/ (individual listings) or /posts/ (FTS)
        if "linkedin.com/jobs" in url_lower and "/jobs/view/" not in url_lower:
            continue
        # LinkedIn posts: only accept if they came from FTS (have _source_override)
        if "linkedin.com/posts/" in url_lower and not r.get("_source_override"):
            continue

        # Skip generic job board index/search pages
        if re.search(r"(alljobs\.co\.il/SearchResults|drushim\.co\.il/.*\?)", url):
            continue

        # Skip SPA career sites where location can't be verified server-side
        spa_domains = ["jobs.apple.com", "careers.google.com", "careers.microsoft.com"]
        if any(d in url_lower for d in spa_domains):
            continue

        # Skip pages that are clearly job indexes, not individual listings
        index_url_patterns = [
            r"/jobs/?$", r"/careers/?$", r"/openings/?$",
            r"/jobs/?\?", r"/location/", r"/locations/", r"/category/",
            r"/job-location-category/", r"/jobs/mena/",
            r"/list/", r"startup\.jobs/",
            r"secrettelaviv\.com", r"efinancialcareers\.com",
            r"aidevtlv\.com", r"machinelearning\.co\.il",
            r"remoterocketship\.com", r"devjobs\.co\.il",
            r"simplyhired\.com", r"jooble\.", r"talent\.com",
            r"jobrapido\.", r"careerjet\.",
            r"gotfriends\.co\.il", r"whist\.co\.il", r"medulla\.co\.il",
            r"jobify360\.co\.il",
        ]
        if any(re.search(p, url_lower) for p in index_url_patterns):
            continue

        # Use _source_override from LinkedIn FTS results, otherwise detect from URL
        source = r.get("_source_override") or detect_source(url)
        category = detect_category(title, snippet)
        # For LinkedIn FTS results, prefer the pre-extracted company name
        if r.get("_source_override") == "linkedin_fts" and r.get("company"):
            company = r["company"]
        else:
            company = extract_company(title, snippet, url)
        location = extract_location(title, snippet)

        # Generate stable ID from URL
        job_id = hashlib.md5(url.encode()).hexdigest()[:8]

        jobs.append({
            "id": job_id,
            "title": title[:80],
            "subtitle": snippet[:60] if snippet else "",
            "company": company,
            "companyIndustry": "",
            "location": location,
            "locationSlug": location.lower().replace(" ", "-"),
            "source": source,
            "sourceUrl": url,
            "category": category,
            "posted": "",  # Will be filled by date extraction
            "isNew": True,
            "isDeveleapCustomer": is_develeap_customer(company),
            "isPastCustomer": is_develeap_past_customer(company),
            "_snippet": snippet,  # Keep full snippet for closed/date detection
            "description": snippet[:120] if snippet else title,
            "skills": [],
            "stakeholders": _get_stakeholders(company),
            "logo": _get_company_logo(company, url),
        })

    # Fetch real posting dates, company names, and closed status
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    active_jobs = []
    for j in jobs:
        url = j.get("sourceUrl", "")
        snippet_full = j.get("_snippet", "")  # Google search snippet
        snippet_lower = snippet_full.lower()

        # ── 1. Check Google snippet for closed signals (most reliable!) ──
        snippet_closed_phrases = [
            "no longer accepting applications",
            "this job is no longer available",
            "position has been filled",
            "job has expired",
        ]
        if any(p in snippet_lower for p in snippet_closed_phrases):
            log.info(f"  Skipping closed (snippet): {j['title'][:50]}")
            continue

        # ── 2. Extract date from Google snippet (relative dates) ──
        snippet_date = ""
        from datetime import timedelta
        # Patterns like "3 days ago", "1 year ago", "2 weeks ago" in snippet
        rel_match = re.search(r'(\d+)\s+(hour|day|week|month|year)s?\s+ago', snippet_lower)
        if rel_match:
            n = int(rel_match.group(1))
            unit = rel_match.group(2)
            now = datetime.now(timezone.utc)
            if unit == "hour":
                dt = now - timedelta(hours=n)
            elif unit == "day":
                dt = now - timedelta(days=n)
            elif unit == "week":
                dt = now - timedelta(weeks=n)
            elif unit == "month":
                dt = now - timedelta(days=n * 30)
            elif unit == "year":
                dt = now - timedelta(days=n * 365)
            snippet_date = dt.strftime("%Y-%m-%d")
            log.info(f"  Date from snippet: {snippet_date} ({rel_match.group()}) for {j['title'][:40]}")
        # Hebrew relative dates in snippet: "לפני X ימים"
        if not snippet_date:
            heb_match = re.search(r'לפני\s+(?:‏)?(\d+)\s*(?:‏)?\s*(ימים|שבועות|חודשים|שנים|שעות)', snippet_full)
            if heb_match:
                n = int(heb_match.group(1))
                unit_heb = heb_match.group(2)
                now = datetime.now(timezone.utc)
                unit_map = {"שעות": "hours", "ימים": "days", "שבועות": "weeks", "חודשים": "months", "שנים": "years"}
                unit = unit_map.get(unit_heb, "days")
                if unit == "hours":
                    dt = now - timedelta(hours=n)
                elif unit == "days":
                    dt = now - timedelta(days=n)
                elif unit == "weeks":
                    dt = now - timedelta(weeks=n)
                elif unit == "months":
                    dt = now - timedelta(days=n * 30)
                elif unit == "years":
                    dt = now - timedelta(days=n * 365)
                snippet_date = dt.strftime("%Y-%m-%d")
                log.info(f"  Date from Hebrew snippet: {snippet_date} for {j['title'][:40]}")

        # ── 3. Skip listings older than 14 days ──
        if snippet_date:
            from datetime import datetime as dt_cls
            try:
                post_dt = dt_cls.strptime(snippet_date, "%Y-%m-%d")
                age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt).days
                if age_days > 14:
                    log.info(f"  Skipping old listing ({age_days} days): {j['title'][:50]}")
                    continue
            except ValueError:
                pass

        # ── 4. Scrape page for additional data ──
        # LinkedIn FTS posts: skip page scraping (they're social posts, not job pages)
        is_fts = j.get("source") == "linkedin_fts"
        if is_fts:
            # FTS listings: use today's date, skip all page-level checks
            j["posted"] = snippet_date if snippet_date else today
            j.pop("_snippet", None)
            # Skip Develeap's own listings
            if j["company"].lower() in ("develeap", "develeap ltd", "develeap ltd."):
                log.info(f"  Skipping Develeap's own listing: {j['title'][:50]}")
                continue
            active_jobs.append(j)
            continue

        if url:
            page_data = scrape_job_page(url)

            # Skip career/multi-listing pages (e.g. expired Greenhouse job IDs)
            if page_data.get("is_career_page"):
                log.info(f"  Skipping career page (not a specific job): {j['title'][:50]}")
                continue

            # Skip closed listings detected from page HTML
            if page_data.get("closed"):
                log.info(f"  Skipping closed (page): {j['title'][:50]}")
                continue

            # Use page date if we don't have snippet date
            if page_data.get("date") and not snippet_date:
                snippet_date = page_data["date"]
                log.info(f"  Date from page: {snippet_date} for {j['title'][:40]}")
                # Check if page date is older than 14 days
                try:
                    from datetime import datetime as dt_cls_pg
                    post_dt_pg = dt_cls_pg.strptime(snippet_date, "%Y-%m-%d")
                    age_days_pg = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt_pg).days
                    if age_days_pg > 14:
                        log.info(f"  Skipping old listing from page date ({age_days_pg} days): {j['title'][:50]}")
                        continue
                except ValueError:
                    pass

            # Fix company if still Unknown
            if j["company"] == "Unknown" and page_data.get("company"):
                j["company"] = page_data["company"]
                j["isDeveleapCustomer"] = is_develeap_customer(page_data["company"])
                log.info(f"  Company from page: {page_data['company']}")

            # ── 5. Skip listings that are NOT in Israel ──
            loc_country = page_data.get("location_country", "").lower()
            if loc_country:
                # List of Israel indicators
                israel_indicators = ["israel", "il", "tel aviv", "herzliya", "haifa",
                                     "jerusalem", "ramat gan", "ra'anana", "raanana",
                                     "petah tikva", "netanya", "beer sheva", "hod hasharon",
                                     "rehovot", "rishon lezion", "kfar saba", "bnei brak",
                                     "modi'in", "yokneam", "caesarea"]
                is_israel = any(ind in loc_country for ind in israel_indicators)
                # Also check if it's a known non-Israel country
                non_israel_countries = ["india", "united states", "usa", "uk", "united kingdom",
                                        "germany", "france", "china", "japan", "canada",
                                        "australia", "brazil", "singapore", "ireland",
                                        "netherlands", "spain", "italy", "sweden", "poland",
                                        "romania", "czech", "hungary", "ukraine", "turkey",
                                        "south korea", "mexico", "argentina", "chile",
                                        "bangalore", "hyderabad", "mumbai", "delhi", "pune",
                                        "chennai", "kolkata", "noida", "gurgaon", "gurugram",
                                        "san francisco", "new york", "london", "berlin",
                                        "paris", "amsterdam", "toronto", "sydney", "tokyo",
                                        "shanghai", "dublin", "austin", "seattle", "boston",
                                        "cupertino", "mountain view", "palo alto"]
                is_non_israel = any(ind in loc_country for ind in non_israel_countries)
                if is_non_israel and not is_israel:
                    log.info(f"  Skipping non-Israel listing ({loc_country}): {j['title'][:50]}")
                    continue

            # ── 6. Skip very old listings from page date (>180 days) ──
            page_date_for_age = page_data.get("date", "")
            if page_date_for_age and not snippet_date:
                try:
                    from datetime import datetime as dt_cls2
                    post_dt2 = dt_cls2.strptime(page_date_for_age, "%Y-%m-%d")
                    age_days2 = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt2).days
                    if age_days2 > 14:
                        log.info(f"  Skipping old listing from page date ({age_days2} days, {page_date_for_age}): {j['title'][:50]}")
                        continue
                except ValueError:
                    pass

            # ── 7. LinkedIn with no date: keep if page is reachable ──
            # LinkedIn aggressively blocks page content (listedAt, JSON-LD, companyName)
            # from data center IPs. Since the listing was found via search engine results
            # (DuckDuckGo/SerpAPI), the URL is valid and the job likely exists.
            # Genuinely closed/expired listings are already caught by:
            #   - Step 1: Google snippet closed signals
            #   - Step 4: Page-level "job closed" / "no longer accepting" phrases
            # Only skip if the page returned a non-200 HTTP status (truly gone).
            if "linkedin.com" in url and not snippet_date and not page_data.get("date"):
                http_status = page_data.get("_http_status", 200)
                # 429 = rate limited (not gone), 200 = reachable but data blocked
                # Only skip if page returned 404/410 (listing truly removed)
                if http_status in (404, 410):
                    log.info(f"  Skipping LinkedIn listing (HTTP {http_status}, listing removed): {j['title'][:50]}")
                    continue
                log.info(f"  Keeping LinkedIn listing without date (HTTP {http_status}): {j['title'][:50]}")

            time.sleep(random.uniform(0.5, 1.5))  # Rate limit

        j["posted"] = snippet_date if snippet_date else today
        j.pop("_snippet", None)  # Remove internal field before dashboard

        # Skip Develeap's own listings
        if j["company"].lower() in ("develeap", "develeap ltd", "develeap ltd."):
            log.info(f"  Skipping Develeap's own listing: {j['title'][:50]}")
            continue

        active_jobs.append(j)

    fts_count = sum(1 for j in active_jobs if j.get("source") == "linkedin_fts")
    log.info(f"  Filtered: {len(jobs)} → {len(active_jobs)} (removed {len(jobs) - len(active_jobs)} closed/Develeap)")
    if fts_count:
        log.info(f"  Includes {fts_count} linkedin_fts listings")
    return active_jobs


# ── Dashboard Update ───────────────────────────────────────────────────────

def load_existing_jobs(html: str) -> list[dict]:
    """Extract existing ALL_JOBS from dashboard HTML."""
    match = re.search(r'let ALL_JOBS\s*=\s*(\[.*?\]);\s*$', html, re.DOTALL | re.MULTILINE)
    if match:
        raw = match.group(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            try:
                # Fix invalid backslash escapes (e.g. "DataOps \ MLOps")
                fixed = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', raw)
                # Fix unquoted keys
                fixed = re.sub(r'(?<=[{,])\s*(\w+)\s*:', r' "\1":', fixed)
                # Remove trailing commas before } or ]
                fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
                return json.loads(fixed)
            except json.JSONDecodeError:
                log.warning("Failed to parse existing ALL_JOBS, keeping as-is")
    return []


# ── Company name normalization ──────────────────────────────────────────────
# Maps variant/alternate company names to a single canonical form.
# Keys are lowercase; values are the canonical display name.
COMPANY_ALIASES = {
    "checkpoint":           "Check Point Software",
    "check point software": "Check Point Software",
    "check point":          "Check Point Software",
    "vastdata":             "VAST Data",
    "vast data":            "VAST Data",
    "wizinc":               "Wiz",
    "wiz":                  "Wiz",
    "doitintl":             "DoiT International",
    "doit international":   "DoiT International",
    "tikalk":               "Tikal",
    "tikal":                "Tikal",
    "somekhchaikin":        "KPMG Israel",
    "kpmg":                 "KPMG Israel",
    "kpmg israel":          "KPMG Israel",
}


def _normalize_company(name: str) -> str:
    """Return the canonical company name, or the original if no alias."""
    return COMPANY_ALIASES.get(name.lower().strip(), name)


def _normalize_title(title: str) -> str:
    """Normalize job title for dedup matching.

    Strips source-name suffixes like '- Comeet', '- CAREERS AT NVIDIA',
    '- Myworkdayjobs.com', '- Lever', etc.  Also removes parenthetical
    job IDs like '(25020)' and 'at Company - Comeet' patterns.
    """
    t = title.lower().strip()
    # Order matters: check compound patterns BEFORE simple suffix stripping
    # 1. Remove 'at Company - Source' suffix (e.g. "Cloud Security Engineer at Port - Comeet")
    t = re.sub(r'\s+at\s+[\w\s]+-\s*(?:comeet|lever|greenhouse|careers)\s*$', '', t)
    # 2. Remove trailing source names: "- Comeet", "- Lever", etc.
    t = re.sub(r'\s*-\s*(?:comeet|lever|greenhouse|jobgether|myworkdayjobs\.com)\s*$', '', t)
    # 2b. Remove "| Source" suffix (e.g. "FinOps Engineer @ Ness | LHH Job Board")
    t = re.sub(r'\s*\|\s*(?:lhh job board|glassdoor|indeed|linkedin|drushim|alljobs)\s*$', '', t, flags=re.IGNORECASE)
    # 2c. Remove "@ Company Name" suffix when it's a company name at end of title
    #     e.g. "FinOps Engineer @ Ness Technologies Israel" → "FinOps Engineer"
    #     Only strip if what follows @ looks like a company (2+ words or known pattern)
    t = re.sub(r'\s*@\s+(?:[A-Z][\w]*[\s]){1,5}[\w]*\s*$', '', t)
    # Also handle lowercase variant
    t = re.sub(r'\s*@\s+\S+(?:\s+\S+){1,4}\s*$', '', t)
    # 3. Remove "- CAREERS AT <company>" suffix
    t = re.sub(r'\s*-\s*careers\s+at\s+\S+\s*$', '', t)
    # 4. Remove LinkedIn-style "Company Name גיוס עובדים" prefix (Hebrew for "hiring")
    #    e.g. "Check Point Software גיוס עובדים Senior FinOps Engineer" → "Senior FinOps Engineer"
    t = re.sub(r'^.*?גיוס\s*עובדים\s*', '', t)
    # 5. Remove "דרושים" (wanted) and "דרוש/ה" prefix patterns
    t = re.sub(r'^דרושים\s*', '', t)
    t = re.sub(r'^דרוש/?ה?\s*', '', t)
    # 5b. Remove Hebrew suffix "לנס (NESS)" or "לחברת X" (to company X)
    t = re.sub(r'\s*ל[\u0590-\u05FF]+\s*(?:\([^)]+\))?\s*התפקיד.*$', '', t)
    # 5c. Remove location suffixes: "| Tel Aviv District", "- Tel Aviv-Yafo, Israel", etc.
    #     These vary by source and cause the same job to appear "new" when re-scraped
    #     from a different source with a different location format.
    t = re.sub(
        r'\s*[\|–—-]\s*(?:tel\s*aviv|jerusalem|haifa|beer\s*sheva|ramat\s*gan|herzliya|'
        r'petah\s*tikva|netanya|rishon|rehovot|modiin|bnei\s*brak|kfar\s*saba|'
        r'raanana|ashdod|ashkelon|eilat|nazareth|acre|tiberias|'
        r'israel|remote|hybrid|on-?site|worldwide|global)[\w\s,.\-–—]*$',
        '', t, flags=re.IGNORECASE
    )
    # Also strip generic "| City District/Region/Area" patterns at end
    t = re.sub(r'\s*\|\s*[A-Za-z\s]+(?:district|region|area|county|province|state)\s*$', '', t, flags=re.IGNORECASE)
    # Strip "- City, Country" or "- City, State" trailing location patterns
    t = re.sub(r'\s*-\s*[A-Za-z\s-]+,\s*(?:israel|il|us|usa|uk|remote)\s*$', '', t, flags=re.IGNORECASE)
    # 6. Remove parenthetical job IDs like (25020)
    t = re.sub(r'\s*\(\d+\)\s*', ' ', t)
    # 7. Clean up trailing punctuation and whitespace
    t = re.sub(r'[\s.,;:]+$', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _is_company_page(j: dict) -> bool:
    """Return True if this listing is a company career-page link, not a specific job.

    Detects patterns like 'Jobs at Vonage', 'Jobs at Deloitte - Comeet',
    or titles that are just the company name (e.g. 'Mobileye', 'Mobileye - Lever').
    """
    title = j.get("title", "").strip()
    company = j.get("company", "").strip()
    t_lower = title.lower()
    c_lower = company.lower()

    # "Jobs at X" or "Jobs at X - Comeet" or "Careers at X"
    if re.match(r'^(?:jobs|careers)\s+at\s+', t_lower):
        return True
    # Title == company name (with optional source suffix like "- Lever", "- Careers")
    cleaned = re.sub(r'\s*-\s*(comeet|lever|greenhouse|jobgether|careers)\s*$', '', t_lower).strip()
    if cleaned == c_lower and cleaned:
        return True
    # Also check against normalized company name
    norm_lower = _normalize_company(company).lower()
    if cleaned == norm_lower and cleaned:
        return True
    return False


def _consolidate_duplicates(jobs: list[dict]) -> list[dict]:
    """Consolidate duplicate listings in the job list.

    Finds jobs that match on company + normalized title but come from
    different sources (or are exact dupes).  Keeps the best entry and
    records the others as altSources.
    """
    from collections import defaultdict

    groups = defaultdict(list)
    for j in jobs:
        comp = _normalize_company(j.get("company", "")).lower().strip()
        norm = _normalize_title(j.get("title", ""))
        groups[(comp, norm)].append(j)

    consolidated = []
    merge_count = 0
    for (comp, norm), entries in groups.items():
        if len(entries) == 1:
            consolidated.append(entries[0])
            continue

        # Multiple entries for same company+role — pick the best primary
        # Prefer: most recent posted date, then entry with most stakeholders
        entries.sort(key=lambda x: (x.get("posted", ""), len(x.get("stakeholders", []))), reverse=True)
        primary = entries[0]

        # Merge altSources from all duplicates
        alt_sources = list(primary.get("altSources", []))
        seen_urls = {primary.get("sourceUrl", "")}
        seen_urls.update(a.get("sourceUrl", "") for a in alt_sources)

        for dup in entries[1:]:
            dup_url = dup.get("sourceUrl", "")
            if dup_url and dup_url not in seen_urls:
                alt_sources.append({
                    "source": detect_source(dup_url),
                    "sourceUrl": dup_url,
                    "title": dup.get("title", "")[:80]
                })
                seen_urls.add(dup_url)
            # Also pull in any altSources the duplicate had
            for a in dup.get("altSources", []):
                a_url = a.get("sourceUrl", "")
                if a_url and a_url not in seen_urls:
                    alt_sources.append(a)
                    seen_urls.add(a_url)

        if alt_sources:
            primary["altSources"] = alt_sources
        merge_count += len(entries) - 1
        consolidated.append(primary)

    if merge_count:
        log.info(f"  Consolidation: merged {merge_count} duplicate listings into existing entries")
    return consolidated


def _load_hidden_companies() -> set:
    """Load hidden companies from outreach_status.json (synced from dashboard)."""
    try:
        with open("outreach_status.json", "r") as f:
            data = json.load(f)
        return {c.lower() for c in data.get("hiddenCompanies", [])}
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def merge_jobs(existing: list[dict], new_jobs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Merge new jobs with existing, return (merged, only_new)."""
    # Filter out Develeap's own listings (but keep Unknown company jobs to prevent
    # them from being re-discovered as "new" on every run — which caused duplicate
    # Slack notifications for listings scraped with company="Unknown")
    develeap_names = {"develeap", "develeap ltd", "develeap ltd."}
    existing = [j for j in existing if j.get("company", "").lower() not in develeap_names
                or j.get("_isMock")]

    # Remove company-page listings (not specific job postings)
    before_cp = len(existing)
    existing = [j for j in existing if not _is_company_page(j)]
    if before_cp != len(existing):
        log.info(f"  Removed {before_cp - len(existing)} company-page listings (not specific jobs)")

    # Remove aggregator/index pages from existing jobs
    def _is_aggregator(j):
        t = j.get("title", "").lower()
        sub = j.get("subtitle", "").lower()
        u = j.get("sourceUrl", "").lower()
        combined = t + " " + sub
        # Title/subtitle patterns: "X jobs in Israel", "jobs (N)", "Archives", "jobs wanted"
        if re.search(r'(?:^\d+\s+)?(?:.*?\bjobs?\b.*?\bin\b|.*?\bjobs?\b\s*\(\d+\))', t):
            return True
        if any(kw in combined for kw in ["jobs in israel", "apply now", "remote jobs in",
                                   "archives", "משרות דרושים", "jobs wanted",
                                   "as a service for startups", "open positions",
                                   "see our list", "career opportunities",
                                   "we're hiring", "join our team", "jobs at "]):
            return True
        # URL patterns for known aggregators
        agg_domains = ["remoterocketship.com", "devjobs.co.il", "simplyhired.com",
                       "jooble.", "talent.com", "jobrapido.", "careerjet.",
                       "secrettelaviv.com", "efinancialcareers.com",
                       "aidevtlv.com", "machinelearning.co.il", "gotfriends.co.il",
                       "whist.ai", "startup.jobs"]
        # NOTE: comeet.com is an ATS (individual job pages), NOT an aggregator.
        # Do NOT add it here — it caused a severe bug where Comeet listings were
        # removed from existing every run, then re-added as "new", triggering
        # duplicate Slack notifications indefinitely.
        if any(d in u for d in agg_domains):
            return True
        return False

    before_agg = len(existing)
    existing = [j for j in existing if not _is_aggregator(j)]
    if before_agg != len(existing):
        log.info(f"  Removed {before_agg - len(existing)} aggregator pages from existing jobs")

    # Remove jobs with empty or broken URLs
    before_url = len(existing)
    existing = [j for j in existing if j.get("sourceUrl", "").startswith("http")]
    if before_url != len(existing):
        log.info(f"  Removed {before_url - len(existing)} jobs with empty/broken URLs")

    # Validate existing ATS listings by checking for career-page redirects
    # (e.g. expired Greenhouse job IDs redirect to company careers page)
    def _is_ats_career_page(j):
        u = j.get("sourceUrl", "").lower()
        if not u:
            return False
        # Only check ATS URLs where expired IDs can redirect to career pages
        ats_patterns = [
            (r'greenhouse\.io/.+/jobs/\d+', 'greenhouse.io'),
            (r'lever\.co/.+/[a-f0-9-]{20,}', 'lever.co'),
        ]
        is_ats = False
        for pat, domain in ats_patterns:
            if domain in u and re.search(pat, u):
                is_ats = True
                break
        if not is_ats:
            return False
        # Spot-check: HEAD request to detect redirect to career page
        try:
            resp = requests.head(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }, timeout=8, allow_redirects=True)
            final = resp.url.lower()
            # Greenhouse: lost the /jobs/\d+ in final URL
            if 'greenhouse.io' in u and re.search(r'/jobs/\d+', u):
                if not re.search(r'/jobs/\d+', final):
                    log.info(f"  Existing job redirects to career page: {u[:60]} → {final[:60]}")
                    return True
        except Exception:
            pass
        return False

    before_ats_cp = len(existing)
    existing = [j for j in existing if not _is_ats_career_page(j)]
    if before_ats_cp != len(existing):
        log.info(f"  Removed {before_ats_cp - len(existing)} ATS career-page redirects from existing jobs")

    # Remove SPA career sites where location can't be verified server-side
    # (e.g. jobs.apple.com /en-il/ shows jobs from all countries, not just Israel)
    spa_unverifiable = ["jobs.apple.com", "careers.google.com", "careers.microsoft.com"]
    before_spa = len(existing)
    existing = [j for j in existing if not any(d in j.get("sourceUrl", "") for d in spa_unverifiable)]
    if before_spa != len(existing):
        log.info(f"  Removed {before_spa - len(existing)} unverifiable SPA career pages from existing jobs")

    # Re-check existing listings — remove closed, stale (>14d), and non-Israel
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cleaned = []
    for j in existing:
        # Skip all cleanup checks for mock/test listings
        if j.get("_isMock"):
            cleaned.append(j)
            continue
        url = j.get("sourceUrl", "")

        # ── Age-check existing jobs by their stored date ──
        posted = j.get("posted", "")
        if posted:
            try:
                from datetime import datetime as dt_cls3
                post_dt3 = dt_cls3.strptime(posted, "%Y-%m-%d")
                age_days3 = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt3).days
                if age_days3 > 14:
                    log.info(f"  Removing stale existing listing ({age_days3} days): {j.get('title', '')[:50]}")
                    continue
            except ValueError:
                pass

        if "linkedin.com" in url:
            page_data = scrape_job_page(url)
            if page_data.get("closed"):
                log.info(f"  Removing closed listing: {j.get('title', '')[:50]}")
                continue
            # If we now got a real date, update it
            if page_data.get("date"):
                if j.get("posted") != page_data["date"]:
                    log.info(f"  Updated date: {j.get('title', '')[:40]} → {page_data['date']}")
                    j["posted"] = page_data["date"]
                # Re-check age with the updated date
                try:
                    from datetime import datetime as dt_cls4
                    post_dt4 = dt_cls4.strptime(page_data["date"], "%Y-%m-%d")
                    age_days4 = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt4).days
                    if age_days4 > 14:
                        log.info(f"  Removing stale listing after date update ({age_days4} days): {j.get('title', '')[:50]}")
                        continue
                except ValueError:
                    pass
            # If page has no date, the listing is likely stale — LinkedIn strips
            # metadata from old/closed listings. Remove regardless of stored date.
            if not page_data.get("date"):
                log.info(f"  Removing existing LinkedIn listing with no verifiable date: {j.get('title', '')[:50]}")
                continue

            # Check location country
            loc_country = page_data.get("location_country", "").lower()
            if loc_country:
                israel_indicators = ["israel", "il", "tel aviv", "herzliya", "haifa",
                                     "jerusalem", "ramat gan", "ra'anana", "raanana",
                                     "petah tikva", "netanya", "beer sheva"]
                non_israel_countries = ["india", "united states", "usa", "uk", "united kingdom",
                                        "germany", "france", "china", "japan", "canada",
                                        "australia", "brazil", "singapore", "ireland",
                                        "bangalore", "hyderabad", "mumbai", "delhi", "pune",
                                        "cupertino", "mountain view", "palo alto",
                                        "san francisco", "new york", "london", "berlin"]
                is_israel = any(ind in loc_country for ind in israel_indicators)
                is_non_israel = any(ind in loc_country for ind in non_israel_countries)
                if is_non_israel and not is_israel:
                    log.info(f"  Removing non-Israel existing listing ({loc_country}): {j.get('title', '')[:50]}")
                    continue
            time.sleep(random.uniform(0.3, 0.8))
        cleaned.append(j)

    log.info(f"  Existing cleanup: {len(existing)} → {len(cleaned)} (removed {len(existing) - len(cleaned)} closed)")
    existing = cleaned

    # Normalize company names (e.g. "Checkpoint" → "Check Point Software")
    for j in existing:
        j["company"] = _normalize_company(j.get("company", ""))

    # Consolidate duplicates within existing listings before processing new ones
    existing = _consolidate_duplicates(existing)

    # Index existing by URL, by exact company+title, AND by normalized_company+normalized_title
    existing_urls = {j.get("sourceUrl", ""): j for j in existing if j.get("sourceUrl")}
    existing_keys = {f'{j.get("company","").lower()}|{j.get("title","").lower()}': j for j in existing}
    existing_norm = {f'{_normalize_company(j.get("company","")).lower()}|{_normalize_title(j.get("title",""))}': j for j in existing}

    # Mark existing jobs as not new; update stakeholders (preserve enrichment)
    for j in existing:
        j["isNew"] = False
        old_stakeholders = j.get("stakeholders", [])
        new_stakeholders = _get_stakeholders(j.get("company", ""))
        # Preserve ALL enrichment data from previously enriched stakeholders
        # Index old stakeholders by linkedin URL and by name (fallback)
        old_by_li = {s.get("linkedin", ""): s for s in old_stakeholders if s.get("linkedin")}
        old_by_name = {s.get("name", "").lower(): s for s in old_stakeholders if s.get("name")}
        # Fields that the base _get_stakeholders provides (safe to overwrite)
        base_fields = {"name", "title", "linkedin", "source"}
        for s in new_stakeholders:
            li = s.get("linkedin", "")
            name_lower = s.get("name", "").lower()
            old = old_by_li.get(li) or old_by_name.get(name_lower)
            if old:
                # Copy over all enrichment fields (photo, phone, email,
                # _apolloData, connectMsg, followUpMsg, etc.)
                for k, v in old.items():
                    if k not in base_fields and k not in s:
                        s[k] = v
        j["stakeholders"] = new_stakeholders
        # Update logo
        j["logo"] = _get_company_logo(j.get("company", ""), j.get("sourceUrl", ""))
        # Re-classify source from URL (picks up newly added SOURCE_MAP entries)
        # Preserve linkedin_fts source (don't overwrite with generic "linkedin")
        if j.get("source") != "linkedin_fts":
            j["source"] = detect_source(j.get("sourceUrl", ""))
        # Re-classify category (picks up newly added categories like security, sre, etc.)
        j["category"] = detect_category(j.get("title", ""), j.get("description", "") or j.get("subtitle", ""))
        # Re-classify customer status
        company = j.get("company", "")
        j["isDeveleapCustomer"] = is_develeap_customer(company)
        j["isPastCustomer"] = is_develeap_past_customer(company)

    truly_new = []
    _hidden = _load_hidden_companies()
    for j in new_jobs:
        # Normalize company name on incoming jobs
        j["company"] = _normalize_company(j.get("company", ""))

        # Skip Unknown/empty company jobs from new — they stay in existing for dedup
        # but we don't want to re-add them as new listings
        # Exception: linkedin_fts results are allowed with Unknown company (they're social
        # posts where company extraction is harder; the hiring signal is still valuable)
        if j.get("company", "").strip() in ("Unknown", "") and j.get("source") != "linkedin_fts":
            continue

        # Skip company-page listings from new jobs too
        if _is_company_page(j):
            log.info(f"  Skipping company-page listing: \"{j.get('title', '')}\" ({j.get('company', '')})")
            continue

        # Skip new listings from hidden companies (user marked as not relevant)
        if j.get("company", "").lower() in _hidden:
            log.info(f"  Skipping hidden company listing: \"{j.get('title', '')}\" ({j.get('company', '')})")
            continue

        url = j.get("sourceUrl", "")
        comp_lower = j.get("company", "").lower()
        key = f'{comp_lower}|{j.get("title","").lower()}'
        norm_key = f'{comp_lower}|{_normalize_title(j.get("title",""))}'

        # Check all three indexes: URL, exact key, and normalized key
        if url not in existing_urls and key not in existing_keys and norm_key not in existing_norm:
            truly_new.append(j)
        else:
            # Duplicate listing found on a different source — record the alternate source
            match = existing_urls.get(url) or existing_keys.get(key) or existing_norm.get(norm_key)
            if match and url and url != match.get("sourceUrl", ""):
                alt_source = detect_source(url)
                alt_sources = match.get("altSources", [])
                # Avoid adding the same source URL twice
                if not any(a.get("sourceUrl") == url for a in alt_sources):
                    alt_sources.append({
                        "source": alt_source,
                        "sourceUrl": url,
                        "title": j.get("title", "")[:80]
                    })
                    match["altSources"] = alt_sources
                    log.info(f"  Alt source added: {match.get('company','')} — {alt_source} ({url[:60]})")

    merged = existing + truly_new

    # Final consolidation pass — catches any duplicates between existing and truly_new
    merged = _consolidate_duplicates(merged)

    # ── Freshness cutoff: remove anything older than 14 days ──
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%d")
    before_count = len(merged)
    merged = [j for j in merged if (j.get("posted") or "9999") >= cutoff]
    dropped = before_count - len(merged)
    if dropped:
        log.info(f"  Freshness filter: dropped {dropped} listings older than {cutoff}")

    # Sort by date descending
    merged.sort(key=lambda x: x.get("posted", ""), reverse=True)
    # Keep max 200 listings
    merged = merged[:200]

    # Also filter truly_new to only include fresh listings
    truly_new = [j for j in truly_new if (j.get("posted") or "9999") >= cutoff]

    # ── Fix isNew based on actual posted date ──
    # isNew should only be True if the job was posted within the last 36 hours,
    # NOT just because the scraper discovered it for the first time.
    # Jobs posted days/weeks ago that we're seeing for the first time are NOT "new".
    new_cutoff = (datetime.now(timezone.utc) - timedelta(hours=36)).strftime("%Y-%m-%d")
    for j in merged:
        posted = j.get("posted", "")
        if j.get("isNew") and posted and posted < new_cutoff:
            j["isNew"] = False

    return merged, truly_new


def update_dashboard_html(html: str, jobs: list[dict]) -> str:
    """Replace ALL_JOBS array and timestamp in dashboard HTML."""
    # Format jobs as JS array
    jobs_json = json.dumps(jobs, ensure_ascii=False, indent=2)
    # Replace ALL_JOBS — use lambda to avoid re.sub interpreting backslashes in replacement
    replacement = f'let ALL_JOBS = {jobs_json};'
    html = re.sub(
        r'let ALL_JOBS\s*=\s*\[.*?\];\s*$',
        lambda _: replacement,
        html,
        flags=re.DOTALL | re.MULTILINE
    )
    # Update LAST_UPDATED constant
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    html = re.sub(
        r'(?:const|let)\s+LAST_UPDATED\s*=\s*"[^"]*"',
        lambda _: f'let LAST_UPDATED = "{now_iso}"',
        html
    )
    return html


# ── Netlify Deploy ─────────────────────────────────────────────────────────

def deploy_to_netlify(html: str) -> bool:
    """Deploy dashboard HTML to Netlify."""
    if not NETLIFY_TOKEN:
        log.error("NETLIFY_TOKEN not set, skipping deploy")
        return False

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.html", html)
        # Netlify _headers file to ensure correct Content-Type
        zf.writestr("_headers", "/\n  Content-Type: text/html; charset=UTF-8\n/index.html\n  Content-Type: text/html; charset=UTF-8\n")
    buf.seek(0)

    try:
        resp = requests.post(
            f"https://api.netlify.com/api/v1/sites/{NETLIFY_SITE_ID}/deploys",
            headers={
                "Authorization": f"Bearer {NETLIFY_TOKEN}",
                "Content-Type": "application/zip",
            },
            data=buf.read(),
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        log.info(f"Deployed to {result.get('ssl_url', 'unknown')}")
        return True
    except Exception as e:
        log.error(f"Netlify deploy failed: {e}")
        return False


# ── Slack Dedup Tracking ───────────────────────────────────────────────────

def _slack_listing_key(job: dict) -> str:
    """Build a unique identifier for a listing: company|||normalized_title.

    Uses _normalize_company and _normalize_title so that different scrape variants
    of the same job (e.g. Hebrew vs English title, different source suffixes)
    resolve to the same key and avoid duplicate Slack posts.

    NOTE: Category is intentionally excluded from the key. Including category caused
    duplicate Slack notifications when the same job was re-scraped with a different
    category (e.g., "finops" vs "devops") due to different description/subtitle text.

    NOTE: Date is intentionally excluded from the key. Including the date caused
    the same job to be re-posted whenever it was re-scraped with a different date.
    A 30-day staleness window is used instead to allow genuinely re-opened roles
    to be re-posted.

    Uses '|||' as separator instead of '|' because job titles can contain pipes
    (e.g. "Cloud FinOps Engineer | Tel Aviv District") which broke legacy key
    parsing and migration.
    """
    company = _normalize_company(job.get("company") or "").lower().strip()
    title = _normalize_title(job.get("title") or "")
    return f"{company}|||{title}"


def _slack_listing_key_legacy(job: dict) -> str:
    """Legacy key format with date (for backward-compatible dedup)."""
    company = _normalize_company(job.get("company") or "").lower().strip()
    category = (job.get("category") or "").lower().strip()
    title = _normalize_title(job.get("title") or "")
    posted = (job.get("posted") or "")[:10]
    return f"{company}|{category}|{title}|{posted}"


# Staleness window: don't re-post a job if seen within this many days
SLACK_DEDUP_STALENESS_DAYS = 30


def _load_slack_posted() -> dict:
    """Load the posted tracking data.

    Returns dict with:
      - posted_keys: set of keys in ALL formats (legacy company|category|title,
        new company|||title, and date-suffixed variants) for maximum dedup coverage
      - posted_keys_with_dates: set of legacy keys (for backward compat)
      - first_seen: dict mapping key → ISO timestamp of first posting
    """
    try:
        with open(SLACK_POSTED_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            result = {
                "posted_keys": set(data.get("posted_keys", [])),
                "posted_keys_with_dates": set(data.get("posted_keys_with_dates", [])),
                "first_seen": data.get("first_seen", {}),
            }
            # Migrate: extract dateless keys AND new-format keys from legacy entries.
            # Legacy entries can have embedded pipes in titles (e.g. "Cloud FinOps Engineer | Tel Aviv")
            # so we use regex to find the trailing date instead of splitting by |.
            for legacy_key in list(result["posted_keys"]):
                # Check if key ends with |YYYY-MM-DD (date suffix)
                m = re.match(r'^(.+)\|(\d{4}-\d{2}-\d{2})$', legacy_key)
                if m:
                    dateless = m.group(1)  # e.g. "torq|finops|cloud finops engineer | tel aviv district"
                    date_str = m.group(2)
                    # Add the dateless version (old format: company|category|title)
                    result["posted_keys"].add(dateless)
                    result["posted_keys_with_dates"].add(legacy_key)
                    if dateless not in result["first_seen"]:
                        result["first_seen"][dateless] = date_str + "T00:00:00+00:00"
                    # Also build new-format key (company|||title) by extracting company
                    # and title from the old format: company|category|rest_is_title
                    parts = dateless.split("|", 2)  # Split into at most 3 parts
                    if len(parts) >= 3:
                        company_part = parts[0]
                        title_part = parts[2]  # Everything after company|category|
                        # Add both raw and re-normalized versions of the new key
                        # (re-normalize catches location suffixes that old code didn't strip)
                        raw_new_key = f"{company_part}|||{title_part}"
                        norm_new_key = f"{company_part}|||{_normalize_title(title_part)}"
                        for nk in (raw_new_key, norm_new_key):
                            result["posted_keys"].add(nk)
                            if nk not in result["first_seen"]:
                                result["first_seen"][nk] = date_str + "T00:00:00+00:00"

            # Also migrate any dateless legacy keys (company|category|title) to new format
            for key in list(result["posted_keys"]):
                if "|||" not in key:  # Not already new format
                    parts = key.split("|", 2)
                    if len(parts) >= 3 and not re.match(r'^\d{4}-\d{2}-\d{2}$', parts[-1]):
                        # Looks like company|category|title (not a date-keyed entry)
                        company_part = parts[0]
                        title_part = parts[2]
                        raw_new_key = f"{company_part}|||{title_part}"
                        norm_new_key = f"{company_part}|||{_normalize_title(title_part)}"
                        for nk in (raw_new_key, norm_new_key):
                            result["posted_keys"].add(nk)
                            if nk not in result["first_seen"] and key in result["first_seen"]:
                                result["first_seen"][nk] = result["first_seen"][key]

            return result
    except (FileNotFoundError, json.JSONDecodeError):
        return {"posted_keys": set(), "posted_keys_with_dates": set(), "first_seen": {}}


def _save_slack_posted(tracking: dict) -> None:
    """Persist the posted tracking data. Keep last 2000 dateless keys."""
    dateless_keys = sorted(tracking["posted_keys"], reverse=True)[:2000]
    first_seen = {k: v for k, v in tracking.get("first_seen", {}).items() if k in set(dateless_keys)}
    with open(SLACK_POSTED_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "posted_keys": dateless_keys,
            "first_seen": first_seen,
            "updated": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)


def _filter_unposted_jobs(jobs: list[dict]) -> list[dict]:
    """Filter out jobs that have already been posted to Slack.

    Uses multiple key formats (new company|||title and legacy company|category|title)
    to prevent re-posting the same job. A staleness window allows genuinely
    re-opened roles to be re-posted after SLACK_DEDUP_STALENESS_DAYS.
    """
    tracking = _load_slack_posted()
    posted_keys = tracking["posted_keys"]
    first_seen = tracking.get("first_seen", {})
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SLACK_DEDUP_STALENESS_DAYS)).isoformat()

    unposted = []
    skipped_dedup = 0
    skipped_stale_repost = 0

    for j in jobs:
        key = _slack_listing_key(j)  # New format: company|||title

        # Build all possible key variants to check (handles format transitions)
        company = _normalize_company(j.get("company") or "").lower().strip()
        category = (j.get("category") or "").lower().strip()
        title = _normalize_title(j.get("title") or "")
        keys_to_check = [
            key,                                    # New format: company|||title
            f"{company}|{category}|{title}",        # Legacy format with category
        ]

        matched_key = None
        for k in keys_to_check:
            if k in posted_keys:
                matched_key = k
                break

        if matched_key:
            # Check staleness: if first_seen is within the window, skip
            seen_at = first_seen.get(matched_key, "")
            if seen_at and seen_at >= cutoff:
                skipped_dedup += 1
                continue
            elif seen_at:
                # Seen more than STALENESS_DAYS ago — allow re-post (genuinely re-opened role)
                log.info(f"  Re-posting stale job (first seen {seen_at[:10]}): {j.get('title','')[:50]}")
                unposted.append(j)
                continue
            else:
                skipped_dedup += 1
                continue

        # Also check legacy keys with dates for backward compat
        legacy_key = _slack_listing_key_legacy(j)
        if legacy_key in tracking.get("posted_keys_with_dates", set()):
            skipped_dedup += 1
            continue
        unposted.append(j)

    total_filtered = skipped_dedup + skipped_stale_repost
    if total_filtered > 0:
        log.info(f"  Slack dedup: {len(jobs)} candidates → {len(unposted)} new (filtered {skipped_dedup} duplicates)")
    return unposted


# ── Slack Notification ─────────────────────────────────────────────────────

def notify_slack(new_jobs: list[dict]) -> bool:
    """Post new listings to Slack #bdr-updates via incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False
    if not new_jobs:
        log.info("No new jobs to notify about")
        return True

    cat_emoji = {"devops": ":gear:", "ai": ":robot_face:", "agentic": ":zap:", "finops": ":moneybag:"}
    cat_labels = {"devops": "DevOps", "ai": "AI/ML", "agentic": "Agentic", "finops": "FinOps"}

    # Separate Develeap customer listings
    customer_jobs = [j for j in new_jobs if j.get("isDeveleapCustomer")]
    other_jobs = [j for j in new_jobs if not j.get("isDeveleapCustomer")]

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f":newspaper:  {len(new_jobs)} New Job Listings Found", "emoji": True}
    })

    # Develeap customer alerts first (individual cards)
    if customer_jobs:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":rotating_light: *Develeap Customer Listings*"}
        })
        for j in customer_jobs:
            cat = cat_labels.get(j.get("category", ""), "DevOps")
            emoji = cat_emoji.get(j.get("category", ""), ":briefcase:")
            url = j.get("sourceUrl", "")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":star: *<{url}|{j['title'][:60]}>*\n"
                        f"Company: *{j['company']}*  |  {emoji} {cat}  |  :round_pushpin: {j.get('location', 'Israel')}"
                    )
                }
            })
        blocks.append({"type": "divider"})

    # All other listings as a compact table
    if other_jobs:
        if customer_jobs:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": ":briefcase: *Other New Listings*"}
            })

        # Group into chunks to stay within Slack's text limit
        chunk_size = 10
        for i in range(0, len(other_jobs), chunk_size):
            chunk = other_jobs[i:i + chunk_size]
            lines = []
            for j in chunk:
                cat = cat_labels.get(j.get("category", ""), "DevOps")
                emoji = cat_emoji.get(j.get("category", ""), ":briefcase:")
                url = j.get("sourceUrl", "")
                company = j["company"] if j["company"] != "Unknown" else "_Unknown_"
                lines.append(
                    f"{emoji}  <{url}|*{j['title'][:55]}*>\n"
                    f"      {company}  ·  {j.get('location', 'Israel')}"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n\n".join(lines[:chunk_size])}
            })

    # Footer with dashboard link
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": ":bar_chart: <https://dorikafri.github.io/develeap-bdr-job-monitor/|Open Full Dashboard>  |  Powered by Develeap BDR Monitor"
        }]
    })

    payload = {"blocks": blocks}

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        log.info(f"Slack notification sent for {len(new_jobs)} new listings")
        # Record posted keys so we never re-post these listings
        tracking = _load_slack_posted()
        now_iso = datetime.now(timezone.utc).isoformat()
        for j in new_jobs:
            key = _slack_listing_key(j)
            tracking["posted_keys"].add(key)
            if key not in tracking.get("first_seen", {}):
                tracking.setdefault("first_seen", {})[key] = now_iso
        _save_slack_posted(tracking)
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False


# ── Main───────────────────────────────────────────────────────────────────

def main():
    global _auto_discover_count
    _auto_discover_count = 0  # Reset per run
    _stakeholder_cache.clear()

    log.info("=== Develeap BDR Job Monitor Update ===")

    # Load workflow config to check which nodes are enabled
    wf_config = _load_workflow_config()
    if wf_config:
        log.info("Loaded workflow config (version %s)", wf_config.get("version", "?"))
        if not _is_node_enabled(wf_config, "discovery"):
            log.info("Job Discovery node is DISABLED in workflow config — skipping run")
            return

    # 1. Search for jobs
    log.info(f"Searching with {len(SEARCH_QUERIES)} queries...")
    all_raw = []
    for query in SEARCH_QUERIES:
        results = search_jobs(query)
        all_raw.extend(results)
        log.info(f"  '{query}' → {len(results)} results")
        time.sleep(random.uniform(1.0, 2.5))

    # 1b. Also search Google Jobs engine (structured job listings)
    log.info("Searching Google Jobs engine...")
    gj_results = search_google_jobs()
    all_raw.extend(gj_results)
    log.info(f"Google Jobs engine: {len(gj_results)} results")

    # 1c. Add seed jobs (manually curated listings for categories search engines miss)
    all_raw.extend(SEED_JOBS)
    log.info(f"Added {len(SEED_JOBS)} seed jobs")

    # 1d. LinkedIn FTS: search LinkedIn posts for hiring announcements
    log.info("Searching LinkedIn posts (FTS)...")
    fts_results = search_linkedin_fts()
    all_raw.extend(fts_results)
    log.info(f"LinkedIn FTS: {len(fts_results)} hiring posts found")

    log.info(f"Total raw results: {len(all_raw)}")

    # 2. Parse results into structured jobs
    new_jobs = parse_search_results(all_raw)
    log.info(f"Parsed {len(new_jobs)} unique job listings")

    # 3. Load existing dashboard
    if os.path.exists(DASHBOARD_PATH):
        with open(DASHBOARD_PATH, "r", encoding="utf-8") as f:
            html = f.read()
        existing = load_existing_jobs(html)
        log.info(f"Existing dashboard has {len(existing)} listings")
    else:
        log.error(f"Dashboard not found at {DASHBOARD_PATH}")
        return

    # 3b. Clean existing jobs: re-extract company from ATS URLs (most reliable)
    #     and fix entries where company looks like a job title
    ats_url_patterns = [
        r"greenhouse\.io/", r"lever\.co/", r"ashbyhq\.com/", r"comeet\.com/jobs/",
        r"\.myworkdayjobs\.com",
    ]
    for j in existing:
        url = j.get("sourceUrl", "")
        old_company = j.get("company", "")
        needs_fix = False

        # Always re-extract from ATS URLs (they embed the real company slug)
        if any(re.search(p, url) for p in ats_url_patterns):
            fixed = extract_company("", "", url)  # URL-only extraction
            if fixed != "Unknown" and fixed.lower() != old_company.lower():
                needs_fix = True
        # Also fix entries where company looks like a job title
        elif _is_job_title(old_company) or old_company in ("Unknown", ""):
            fixed = extract_company(j.get("title", ""), j.get("description", ""), url)
            if fixed != old_company:
                needs_fix = True

        if needs_fix:
            log.info(f"  Fixed company: '{old_company}' → '{fixed}'")
            j["company"] = fixed
            j["isDeveleapCustomer"] = is_develeap_customer(fixed)
            j["isPastCustomer"] = is_develeap_past_customer(fixed)
            j["stakeholders"] = _get_stakeholders(fixed)
            j["logo"] = _get_company_logo(fixed, url)

    # 4. Merge and identify new listings
    merged, truly_new = merge_jobs(existing, new_jobs)
    log.info(f"After merge: {len(merged)} total, {len(truly_new)} new")
    customer_new = [j for j in truly_new if j.get("isDeveleapCustomer")]
    if customer_new:
        log.info(f"  🌟 {len(customer_new)} new listings from Develeap customers!")

    # 4a-2. Log auto-discovery stats
    auto_found = sum(1 for j in merged if any(s.get("source") == "Auto-discovered" for s in j.get("stakeholders", [])))
    if _auto_discover_count > 0:
        log.info(f"  Auto-discovered stakeholders for {auto_found} listings ({_auto_discover_count} SerpAPI lookups)")

    # 4b. Enrich stakeholders with LinkedIn profile photos
    log.info("Enriching stakeholder photos from LinkedIn...")
    photo_cache = {}  # linkedin_url → base64 data URI (or "" if failed)
    # First pass: collect all already-known photos
    for j in merged:
        for s in j.get("stakeholders", []):
            li = s.get("linkedin", "")
            if li and s.get("photo"):
                photo_cache[li] = s["photo"]
    # Second pass: fetch missing photos (deduplicated by LinkedIn URL)
    photo_count = 0
    fetch_count = 0
    max_fetches = 10  # Rate limit: max SerpAPI image searches per run (conserve quota)
    for j in merged:
        company = j.get("company", "")
        for s in j.get("stakeholders", []):
            name = s.get("name", "")
            li = s.get("linkedin", "")
            cache_key = li or name  # Use LinkedIn URL as key, or name if no URL
            if not cache_key:
                continue
            if cache_key in photo_cache:
                if photo_cache[cache_key]:
                    s["photo"] = photo_cache[cache_key]
                continue
            if fetch_count >= max_fetches:
                photo_cache[cache_key] = ""
                continue
            photo = _fetch_linkedin_photo(name, company, li)
            photo_cache[cache_key] = photo
            fetch_count += 1
            if photo:
                s["photo"] = photo
                photo_count += 1
            time.sleep(random.uniform(0.3, 0.8))  # Brief pause between SerpAPI calls
    # Apply cached photos to any remaining duplicates
    for j in merged:
        for s in j.get("stakeholders", []):
            li = s.get("linkedin", "")
            name = s.get("name", "")
            cache_key = li or name
            if cache_key and not s.get("photo") and photo_cache.get(cache_key):
                s["photo"] = photo_cache[cache_key]
    log.info(f"  Fetched {photo_count} new photos ({fetch_count} SerpAPI requests)")

    # 4c. Validate stakeholder LinkedIn URLs (catch broken/404 profiles)
    log.info("Validating stakeholder LinkedIn URLs...")
    merged = _validate_linkedin_urls(merged)

    # 4d. Generate personalized outreach messages for each stakeholder
    log.info("Generating personalized outreach messages...")
    msg_count = 0
    for j in merged:
        if j.get("stakeholders"):
            _generate_outreach_messages(j)
            msg_count += len(j["stakeholders"])
    log.info(f"  Generated messages for {msg_count} stakeholder contacts")

    # 5. Update dashboard HTML
    updated_html = update_dashboard_html(html, merged)
    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(updated_html)
    # Also write to docs/ for GitHub Pages
    docs_path = os.path.join(os.path.dirname(DASHBOARD_PATH), "..", "docs", "index.html")
    os.makedirs(os.path.dirname(docs_path), exist_ok=True)
    with open(docs_path, "w", encoding="utf-8") as f:
        f.write(updated_html)
    log.info("Dashboard HTML updated (dashboard/ + docs/)")

    # 6. Deploy to Netlify
    if deploy_to_netlify(updated_html):
        log.info("✅ Netlify deploy successful")
    else:
        log.warning("⚠️  Netlify deploy failed")

    # 7. Notify Slack (with dedup to prevent re-posting)
    if truly_new:
        unposted = _filter_unposted_jobs(truly_new)
        if unposted:
            notify_slack(unposted)
        else:
            log.info("All new listings already posted to Slack — skipping")
    else:
        log.info("No new listings — skipping Slack notification")

    log.info("=== Update complete ===")


if __name__ == "__main__":
    main()
