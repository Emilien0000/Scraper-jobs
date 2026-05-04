# scraper-service/main.py  v2.0
# Stratégie anti-détection : RSS (priorité) → fetch direct avec headers réalistes
# Identique à la logique du repo Liberaa (RSS + user-agent rotatif + fallback fetch)
# Pas de jobspy/Playwright → pas de timeout 504
#
# Setup :
#   pip install fastapi uvicorn httpx feedparser python-dateutil
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
# Variable d'environnement :
#   SCRAPER_SECRET=ton_secret_partage

import os
import re
import json
import random
import asyncio
import feedparser
import httpx
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, urlencode, urljoin, parse_qs
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dateutil import parser as dateparser

app = FastAPI(title="JobScraper Anti-Detection v2", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

SCRAPER_SECRET = os.environ.get("SCRAPER_SECRET", "")

# ── User-Agents rotatifs (stratégie Liberaa) ──────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
]

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def browser_headers(referer: str = "https://www.google.fr/") -> dict:
    """Headers imitant un vrai navigateur — clé du bypass anti-bot"""
    return {
        "User-Agent": random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Cache-Control": "max-age=0",
    }

# ── Helpers ────────────────────────────────────────────────────────────────────

def detect_platform(url: str) -> str:
    if "indeed.com"          in url: return "indeed"
    if "linkedin.com"        in url: return "linkedin"
    if "hellowork.com"       in url: return "hellowork"
    if "stage.fr"            in url: return "stagefr"
    if "adzuna.fr"           in url: return "adzuna"
    if "francetravail"       in url: return "francetravail"
    if "pole-emploi"         in url: return "francetravail"
    if "labonnealternance"   in url: return "lba"
    if "welcometothejungle"  in url: return "wtj"
    return "generic"

def guess_type(title: str, description: str = "") -> str:
    text = (title + " " + description).lower()
    if re.search(r"alternance|apprentissage|contrat pro", text): return "alternance"
    if re.search(r"stage|intern|internship",               text): return "stage"
    return "emploi"

def safe_date(val) -> str:
    if not val:
        return datetime.now(timezone.utc).isoformat()
    try:
        return dateparser.parse(str(val)).replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

def make_id(platform: str, url: str) -> str:
    return f"{platform}-{abs(hash(url)) % (10**9):09d}"

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()

# ── Stratégie 1 : RSS Indeed ──────────────────────────────────────────────────
# Indeed expose un flux RSS stable (non-documenté mais fiable).
# C'est la même approche que Liberaa — pas de JS, pas de bot-detection.

def build_indeed_rss(search_url: str) -> str:
    parsed = urlparse(search_url)
    params = parse_qs(parsed.query)
    q  = params.get("q",  ["alternance"])[0]
    l  = params.get("l",  ["France"])[0]
    jt = params.get("jt", [None])[0]
    rss_params = {"q": q, "l": l, "format": "rss", "fromage": "14", "sort": "date"}
    if jt:
        rss_params["jt"] = jt
    base = f"{parsed.scheme}://{parsed.netloc}/rss"
    return base + "?" + urlencode(rss_params)

async def scrape_indeed_rss(search_url: str, limit: int = 20) -> list[dict]:
    rss_url = build_indeed_rss(search_url)
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        r = await client.get(rss_url, headers={
            "User-Agent": random_ua(),
            "Accept": "application/rss+xml,application/xml,text/xml,*/*",
        })
    r.raise_for_status()
    feed = feedparser.parse(r.text)
    jobs = []
    for entry in feed.entries[:limit]:
        title   = entry.get("title", "")
        url_job = entry.get("link",  search_url)
        desc    = strip_html(entry.get("summary", ""))[:400]
        date    = safe_date(entry.get("published") or entry.get("updated"))
        company = ""
        if hasattr(entry, "source") and isinstance(entry.source, dict):
            company = entry.source.get("title", "")
        # Indeed RSS inclut parfois la ville dans indeed_city ou tags
        location = getattr(entry, "indeed_city", "") or ""
        if not location and entry.get("tags"):
            location = entry.tags[0].get("term", "")
        jobs.append({
            "id":          make_id("indeed", url_job),
            "source_url":  search_url,
            "title":       title,
            "company":     company,
            "location":    location,
            "url":         url_job,
            "description": desc,
            "date":        date,
            "type":        guess_type(title, desc),
        })
    return jobs

# ── Stratégie 2 : France Travail API publique ─────────────────────────────────

async def scrape_francetravail(search_url: str, limit: int = 20) -> list[dict]:
    parsed = urlparse(search_url)
    params = parse_qs(parsed.query)
    mots  = params.get("motsCles", params.get("q", ["alternance"]))[0]
    
    api_url = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
    api_params = {"motsCles": mots, "range": f"0-{min(limit-1, 149)}", "sort": "1"}
    
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(api_url, params=api_params, headers={
            "Accept": "application/json",
            "User-Agent": random_ua(),
        })
    data = r.json()
    offres = data.get("resultats", [])
    
    jobs = []
    for o in offres[:limit]:
        url_job  = o.get("origineOffre", {}).get("urlOrigine", search_url)
        title    = o.get("intitule", "")
        company  = o.get("entreprise", {}).get("nom", "")
        location = o.get("lieuTravail", {}).get("libelle", "")
        desc     = (o.get("description", "") or "")[:400]
        jobs.append({
            "id":          make_id("francetravail", url_job or o.get("id", "")),
            "source_url":  search_url,
            "title":       title,
            "company":     company,
            "location":    location,
            "url":         url_job,
            "description": desc,
            "date":        safe_date(o.get("dateCreation")),
            "type":        guess_type(title, desc),
        })
    return jobs

# ── Stratégie 3 : Fetch HTML + JSON-LD (generic) ─────────────────────────────
# Fonctionne pour HelloWork, Adzuna, Stage.fr, LBA, etc.
# Extrait les blocs JSON-LD (schéma JobPosting standard) puis fallback regex.

async def scrape_generic_fetch(search_url: str, platform: str, limit: int = 20) -> list[dict]:
    async with httpx.AsyncClient(
        timeout=25,
        follow_redirects=True,
        headers=browser_headers(),
    ) as client:
        r = await client.get(search_url)

    html = r.text
    jobs = []

    # Extraction JSON-LD
    blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    for block in blocks:
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                t = item.get("@type", "")
                if not isinstance(t, str) or "jobposting" not in t.lower():
                    continue
                title    = item.get("title", "")
                url_job  = item.get("url", search_url)
                desc     = strip_html(item.get("description", ""))[:400]
                company  = ""
                if isinstance(item.get("hiringOrganization"), dict):
                    company = item["hiringOrganization"].get("name", "")
                location = ""
                if isinstance(item.get("jobLocation"), dict):
                    addr = item["jobLocation"].get("address", {})
                    location = addr.get("addressLocality", "") if isinstance(addr, dict) else str(addr)
                jobs.append({
                    "id":          make_id(platform, url_job),
                    "source_url":  search_url,
                    "title":       title,
                    "company":     company,
                    "location":    location,
                    "url":         url_job,
                    "description": desc,
                    "date":        safe_date(item.get("datePosted")),
                    "type":        guess_type(title, desc),
                })
                if len(jobs) >= limit:
                    break
        except Exception:
            continue
        if len(jobs) >= limit:
            break

    # Fallback heuristique si pas de JSON-LD
    if not jobs:
        links = re.findall(r'href=["\'](/(?:emploi|job|offre|annonce|recherche|poste)[^"\']{5,})["\']', html)
        seen = set()
        for path in links:
            if path in seen or len(jobs) >= limit:
                break
            seen.add(path)
            full_url = urljoin(search_url, path)
            label    = path.split("/")[-1].replace("-", " ").replace("_", " ").strip()[:80].title()
            jobs.append({
                "id":          make_id(platform, full_url),
                "source_url":  search_url,
                "title":       label or "Offre",
                "company":     "",
                "location":    "",
                "url":         full_url,
                "description": "",
                "date":        datetime.now(timezone.utc).isoformat(),
                "type":        guess_type(label),
            })

    return jobs[:limit]

# ── Dispatcher ────────────────────────────────────────────────────────────────

async def scrape_url(url: str, limit: int = 20) -> dict:
    platform = detect_platform(url)
    jobs: list[dict] = []
    error: str | None = None

    try:
        if platform == "indeed":
            try:
                jobs = await scrape_indeed_rss(url, limit)
            except Exception:
                # RSS a échoué (rare) → fallback fetch
                jobs = await scrape_generic_fetch(url, platform, limit)

        elif platform == "francetravail":
            try:
                jobs = await scrape_francetravail(url, limit)
            except Exception:
                jobs = await scrape_generic_fetch(url, platform, limit)

        else:
            jobs = await scrape_generic_fetch(url, platform, limit)

    except Exception as e:
        error = str(e)

    return {
        "url":       url,
        "platform":  platform,
        "jobs":      jobs,
        "count":     len(jobs),
        "error":     error,
        "scrapedAt": datetime.now(timezone.utc).isoformat(),
    }

# ── Modèles ────────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    urls: list[str]
    results_wanted: int = 20

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "job-scraper", "version": "2.0.0", "strategy": "rss+fetch+jsonld"}

@app.post("/scrape")
async def scrape_post(
    body: ScrapeRequest,
    x_scraper_secret: Optional[str] = Header(default=None),
):
    if SCRAPER_SECRET and x_scraper_secret != SCRAPER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not body.urls:
        raise HTTPException(status_code=400, detail="urls requis")

    # Toutes les URLs en parallèle
    results = list(await asyncio.gather(*[scrape_url(u, body.results_wanted) for u in body.urls]))
    errors  = [{"url": r["url"], "error": r["error"]} for r in results if r["error"]]

    return {
        "ok":      True,
        "results": results,
        "errors":  errors,
        "total":   sum(r["count"] for r in results),
    }

@app.get("/scrape")
async def scrape_get(
    url: str = Query(...),
    x_scraper_secret: Optional[str] = Header(default=None),
):
    """Debug : GET /scrape?url=https://fr.indeed.com/jobs?q=alternance"""
    if SCRAPER_SECRET and x_scraper_secret != SCRAPER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    result = await scrape_url(url, 10)
    return {"ok": True, "results": [result], "errors": [], "total": result["count"]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)