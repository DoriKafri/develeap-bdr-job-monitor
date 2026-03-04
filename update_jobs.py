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
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────
NETLIFY_SITE_ID = os.environ.get("NETLIFY_SITE_ID", "9533027e-5008-40ca-924c-dede933f0473")
NETLIFY_TOKEN = os.environ.get("NETLIFY_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")  # Optional: for better search results
DASHBOARD_PATH = os.environ.get("DASHBOARD_PATH", "dashboard/index.html")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

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
        {"name": "Nataly Kremer", "title": "CPO & Head of R&D", "linkedin": "https://www.linkedin.com/in/natalyk/", "source": "Company Website", "email": ""},
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
        {"name": "Shmulik Fronman", "title": "VP R&D", "linkedin": "https://www.linkedin.com/in/shmulik-fronman-69267767/", "source": "LinkedIn", "email": "shmulik.fronman@pagaya.com"},
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
]

CATEGORY_KEYWORDS = {
    "agentic": ["agentic", "agent", "llm agent", "autonomous agent", "ai agent", "sales agent"],
    "ai": ["ai engineer", "machine learning", "ml engineer", "mlops", "data scientist",
            "deep learning", "nlp", "llm", "generative ai", "genai", "artificial intelligence"],
    "finops": ["finops", "fin ops", "cloud cost", "cloud financial", "cost optimization",
               "cloud economics", "cloud spend", "cost management", "cloud billing",
               "cost engineer", "cloud finance", "cost analyst"],
    "devops": ["devops", "sre", "site reliability", "platform engineer", "cloud engineer",
               "infrastructure", "ci/cd", "kubernetes", "terraform", "devsecops"],
}

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
    """Search with SerpAPI first, fall back to DuckDuckGo."""
    results = search_serpapi(query)
    if not results:
        time.sleep(random.uniform(1.5, 3.0))  # Rate limiting
        results = search_duckduckgo(query)
    return results


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
    Returns {"date": "YYYY-MM-DD" or "", "company": "name" or "", "closed": bool, "location_country": ""}."""
    result = {"date": "", "company": "", "closed": False, "location_country": ""}
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
            return result
        text = resp.text[:100000]  # Limit to first 100KB
        log.info(f"  Scrape {url[:60]}: status={resp.status_code}, size={len(resp.text)}, truncated={len(text)}")

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
    """Detect job category from title and snippet."""
    text = f"{title} {snippet}".lower()
    # Check most specific categories first
    for kw in CATEGORY_KEYWORDS["agentic"]:
        if kw in text:
            return "agentic"
    for kw in CATEGORY_KEYWORDS["finops"]:
        if kw in text:
            return "finops"
    for kw in CATEGORY_KEYWORDS["ai"]:
        if kw in text:
            return "ai"
    for kw in CATEGORY_KEYWORDS["devops"]:
        if kw in text:
            return "devops"
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


def _get_stakeholders(company: str) -> list:
    """Look up stakeholders for a company from the COMPANY_STAKEHOLDERS dict."""
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
    return []


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

        # LinkedIn: only accept /jobs/view/ (individual listings)
        if "linkedin.com/jobs" in url_lower and "/jobs/view/" not in url_lower:
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

        source = detect_source(url)
        category = detect_category(title, snippet)
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

        # ── 3. Skip very old listings (>6 months) ──
        if snippet_date:
            from datetime import datetime as dt_cls
            try:
                post_dt = dt_cls.strptime(snippet_date, "%Y-%m-%d")
                age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt).days
                if age_days > 180:
                    log.info(f"  Skipping old listing ({age_days} days): {j['title'][:50]}")
                    continue
            except ValueError:
                pass

        # ── 4. Scrape page for additional data ──
        if url:
            page_data = scrape_job_page(url)

            # Skip closed listings detected from page HTML
            if page_data.get("closed"):
                log.info(f"  Skipping closed (page): {j['title'][:50]}")
                continue

            # Use page date if we don't have snippet date
            if page_data.get("date") and not snippet_date:
                snippet_date = page_data["date"]
                log.info(f"  Date from page: {snippet_date} for {j['title'][:40]}")

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
                    if age_days2 > 180:
                        log.info(f"  Skipping old listing from page date ({age_days2} days, {page_date_for_age}): {j['title'][:50]}")
                        continue
                except ValueError:
                    pass

            # ── 7. LinkedIn with no date = likely stale, skip ──
            # If neither snippet nor page scrape found a date for a LinkedIn listing,
            # it's very likely old/closed (LinkedIn strips metadata from old listings)
            if "linkedin.com" in url and not snippet_date and not page_data.get("date"):
                log.info(f"  Skipping LinkedIn listing with no date (likely stale): {j['title'][:50]}")
                continue

            time.sleep(random.uniform(0.5, 1.5))  # Rate limit

        j["posted"] = snippet_date if snippet_date else today
        j.pop("_snippet", None)  # Remove internal field before dashboard

        # Skip Develeap's own listings
        if j["company"].lower() in ("develeap", "develeap ltd", "develeap ltd."):
            log.info(f"  Skipping Develeap's own listing: {j['title'][:50]}")
            continue

        active_jobs.append(j)

    log.info(f"  Filtered: {len(jobs)} → {len(active_jobs)} (removed {len(jobs) - len(active_jobs)} closed/Develeap)")
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


def merge_jobs(existing: list[dict], new_jobs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Merge new jobs with existing, return (merged, only_new)."""
    # Filter out Develeap's own listings from existing jobs too
    develeap_names = {"develeap", "develeap ltd", "develeap ltd."}
    existing = [j for j in existing if j.get("company", "").lower() not in develeap_names]

    # Remove aggregator/index pages from existing jobs
    def _is_aggregator(j):
        t = j.get("title", "").lower()
        u = j.get("sourceUrl", "").lower()
        # Title patterns: "X jobs in Israel", "jobs (N)", "Archives", "jobs wanted"
        if re.search(r'(?:^\d+\s+)?(?:.*?\bjobs?\b.*?\bin\b|.*?\bjobs?\b\s*\(\d+\))', t):
            return True
        if any(kw in t for kw in ["jobs in israel", "apply now", "remote jobs in",
                                   "archives", "משרות דרושים", "jobs wanted",
                                   "as a service for startups"]):
            return True
        # URL patterns for known aggregators
        agg_domains = ["remoterocketship.com", "devjobs.co.il", "simplyhired.com",
                       "jooble.", "talent.com", "jobrapido.", "careerjet.",
                       "secrettelaviv.com", "efinancialcareers.com",
                       "aidevtlv.com", "machinelearning.co.il", "gotfriends.co.il",
                       "whist.ai", "startup.jobs"]
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

    # Remove SPA career sites where location can't be verified server-side
    # (e.g. jobs.apple.com /en-il/ shows jobs from all countries, not just Israel)
    spa_unverifiable = ["jobs.apple.com", "careers.google.com", "careers.microsoft.com"]
    before_spa = len(existing)
    existing = [j for j in existing if not any(d in j.get("sourceUrl", "") for d in spa_unverifiable)]
    if before_spa != len(existing):
        log.info(f"  Removed {before_spa - len(existing)} unverifiable SPA career pages from existing jobs")

    # Re-check existing listings — remove closed, stale (>180d), and non-Israel
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cleaned = []
    for j in existing:
        url = j.get("sourceUrl", "")

        # ── Age-check existing jobs by their stored date ──
        posted = j.get("posted", "")
        if posted:
            try:
                from datetime import datetime as dt_cls3
                post_dt3 = dt_cls3.strptime(posted, "%Y-%m-%d")
                age_days3 = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt3).days
                if age_days3 > 180:
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
                    if age_days4 > 180:
                        log.info(f"  Removing stale listing after date update ({age_days4} days): {j.get('title', '')[:50]}")
                        continue
                except ValueError:
                    pass
            # If page has no date AND stored date looks like it was auto-assigned today, remove
            if not page_data.get("date") and j.get("posted") == today:
                log.info(f"  Removing existing LinkedIn listing with no real date: {j.get('title', '')[:50]}")
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

    # Index existing by URL and company+title
    existing_urls = {j.get("sourceUrl", ""): j for j in existing if j.get("sourceUrl")}
    existing_keys = {f'{j.get("company","").lower()}|{j.get("title","").lower()}': j for j in existing}

    # Mark existing jobs as not new; update stakeholders (preserve photos)
    for j in existing:
        j["isNew"] = False
        old_stakeholders = j.get("stakeholders", [])
        new_stakeholders = _get_stakeholders(j.get("company", ""))
        # Preserve photos from previously enriched stakeholders
        old_photos = {s.get("linkedin", ""): s.get("photo", "") for s in old_stakeholders if s.get("photo")}
        for s in new_stakeholders:
            li = s.get("linkedin", "")
            if li and li in old_photos:
                s["photo"] = old_photos[li]
        j["stakeholders"] = new_stakeholders
        # Update logo
        j["logo"] = _get_company_logo(j.get("company", ""), j.get("sourceUrl", ""))
        # Re-classify source from URL (picks up newly added SOURCE_MAP entries)
        j["source"] = detect_source(j.get("sourceUrl", ""))
        # Re-classify customer status
        company = j.get("company", "")
        j["isDeveleapCustomer"] = is_develeap_customer(company)
        j["isPastCustomer"] = is_develeap_past_customer(company)

    truly_new = []
    for j in new_jobs:
        url = j.get("sourceUrl", "")
        key = f'{j.get("company","").lower()}|{j.get("title","").lower()}'
        if url not in existing_urls and key not in existing_keys:
            truly_new.append(j)

    merged = existing + truly_new
    # Sort by date descending
    merged.sort(key=lambda x: x.get("posted", ""), reverse=True)
    # Keep max 200 listings
    merged = merged[:200]

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
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    log.info("=== Develeap BDR Job Monitor Update ===")

    # 1. Search for jobs
    log.info(f"Searching with {len(SEARCH_QUERIES)} queries...")
    all_raw = []
    for query in SEARCH_QUERIES:
        results = search_jobs(query)
        all_raw.extend(results)
        log.info(f"  '{query}' → {len(results)} results")
        time.sleep(random.uniform(1.0, 2.5))

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
    max_fetches = 80  # Rate limit: max SerpAPI image searches per run
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

    # 7. Notify Slack
    if truly_new:
        notify_slack(truly_new)
    else:
        log.info("No new listings — skipping Slack notification")

    log.info("=== Update complete ===")


if __name__ == "__main__":
    main()
