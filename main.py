# scraper-service/main.py  v3.0
# ─────────────────────────────────────────────────────────────────────────────
# Stratégie principale : JobSpy (python-jobspy) → Indeed France
#   - tls-client en interne → bypass bot-detection sans proxy
#   - Fallback : scrape_generic_fetch (JSON-LD) pour les autres plateformes
#
# Install :
#   pip install fastapi uvicorn httpx feedparser python-dateutil python-jobspy
#
# Variable d'environnement :
#   SCRAPER_SECRET=ton_secret_partage
# ─────────────────────────────────────────────────────────────────────────────

import os, re, json, random, asyncio
import httpx
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, urlencode, urljoin, parse_qs
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dateutil import parser as dateparser

app = FastAPI(title="JobScraper JobSpy v3", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

SCRAPER_SECRET = os.environ.get("SCRAPER_SECRET", "")

# Thread pool pour exécuter JobSpy (synchrone) sans bloquer la boucle asyncio
_executor = ThreadPoolExecutor(max_workers=4)

# ── Helpers ──────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
]

def random_ua(): return random.choice(USER_AGENTS)

def browser_headers(referer="https://www.google.fr/"):
    return {
        "User-Agent": random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "DNT": "1",
        "Connection": "keep-alive",
    }

def detect_platform(url: str) -> str:
    if "indeed.com"         in url: return "indeed"
    if "linkedin.com"       in url: return "linkedin"
    if "hellowork.com"      in url: return "hellowork"
    if "welcometothejungle" in url: return "wtj"
    if "adzuna"             in url: return "adzuna"
    if "francetravail"      in url: return "francetravail"
    if "pole-emploi"        in url: return "francetravail"
    if "labonnealternance"  in url: return "lba"
    if "stage.fr"           in url: return "stagefr"
    return "generic"

def extract_keywords(url: str) -> str:
    params = parse_qs(urlparse(url).query)
    for key in ("q", "motsCles", "keywords", "query", "k", "what"):
        if params.get(key): return params[key][0]
    return "développeur"

def extract_location(url: str) -> str:
    params = parse_qs(urlparse(url).query)
    for key in ("l", "lieuTravail", "location", "where", "loc"):
        if params.get(key) and params[key][0]: return params[key][0]
    return "France"

def extract_indeed_jobtype(url: str) -> str | None:
    """
    Lit le parametre sc= d'une URL Indeed pour detecter le filtre contrat.
    CPAHG = Alternance, QADT5 = Apprentissage -> "internship" cote JobSpy.
    """
    params = parse_qs(urlparse(url).query)
    sc = params.get("sc", [""])[0]
    jt = params.get("jt", [""])[0]
    if any(code in sc for code in ("CPAHG", "QADT5")):
        return "internship"
    if jt in ("internship", "contract", "fulltime", "parttime"):
        return jt
    return None

def guess_type(title: str, description: str = "") -> str:
    text = (title + " " + description).lower()
    if re.search(r"alternance|apprentissage|contrat pro", text): return "alternance"
    if re.search(r"stage|intern|internship",               text): return "stage"
    return "emploi"

def safe_date(val) -> str:
    if not val: return datetime.now(timezone.utc).isoformat()
    try:    return dateparser.parse(str(val)).replace(tzinfo=timezone.utc).isoformat()
    except: return datetime.now(timezone.utc).isoformat()

def make_id(platform: str, url: str) -> str:
    return f"{platform}-{abs(hash(url)) % (10**9):09d}"

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


# ── Stratégie 1 : JobSpy → Indeed France ─────────────────────────────────────
# JobSpy utilise tls-client (fingerprint TLS de vrai navigateur) → bypass
# la détection bot d'Indeed même depuis une IP datacenter (Render, Railway…).
# Il tourne en synchrone → on l'exécute dans un ThreadPoolExecutor.

def _jobspy_scrape_sync(keywords: str, location: str, results_wanted: int, job_type: str | None) -> list[dict]:
    """Exécution synchrone de JobSpy — à appeler via run_in_executor."""
    from jobspy import scrape_jobs  # import ici pour ne pas planter si absent

    kwargs = dict(
        site_name       = ["indeed"],
        search_term     = keywords,
        location        = location or "France",
        results_wanted  = results_wanted,
        country_indeed  = "france",      # indeed.fr
        hours_old       = 72,            # offres des 3 derniers jours
        description_format = "markdown",
        verbose         = 0,
    )
    if job_type:
        kwargs["job_type"] = job_type   # "internship", "fulltime", "contract"

    df = scrape_jobs(**kwargs)
    if df is None or df.empty:
        return []

    jobs = []
    for _, row in df.iterrows():
        title   = str(row.get("title",   "") or "")
        company = str(row.get("company", "") or "")
        city    = str(row.get("city",    "") or "")
        state   = str(row.get("state",   "") or "")
        loc     = ", ".join(filter(None, [city, state])) or str(row.get("location", "") or "")
        url_job = str(row.get("job_url", "") or "")
        desc    = str(row.get("description", "") or "")[:400]
        date    = safe_date(row.get("date_posted"))
        jtype   = str(row.get("job_type", "") or "")

        # Normalise le type JobSpy → nos catégories
        if "intern" in jtype.lower() or guess_type(title, desc) == "stage":
            norm_type = "stage"
        elif guess_type(title, desc) == "alternance":
            norm_type = "alternance"
        else:
            norm_type = "emploi"

        jobs.append({
            "id":          make_id("indeed", url_job),
            "source_url":  "indeed",
            "title":       title,
            "company":     company,
            "location":    loc,
            "url":         url_job,
            "description": desc,
            "date":        date,
            "type":        norm_type,
        })
    return jobs


async def scrape_indeed_jobspy(keywords: str, location: str, limit: int, job_type: str | None = None) -> list[dict]:
    """Wrapper async autour de JobSpy."""
    loop = asyncio.get_event_loop()

    jobs = await loop.run_in_executor(
        _executor,
        _jobspy_scrape_sync,
        keywords, location, limit, job_type,
    )
    return jobs[:limit]


# ── Stratégie 2 : fetch HTML + JSON-LD (fallback générique) ──────────────────
# Fonctionne pour HelloWork, Adzuna, WTJ, stage.fr, etc.

async def scrape_generic_fetch(search_url: str, platform: str, limit: int = 20) -> list[dict]:
    async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers=browser_headers()) as client:
        r = await client.get(search_url)

    html  = r.text
    jobs  = []

    # Extraction JSON-LD (schéma JobPosting)
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
                title   = item.get("title", "")
                url_job = item.get("url", search_url)
                desc    = strip_html(item.get("description", ""))[:400]
                company = ""
                if isinstance(item.get("hiringOrganization"), dict):
                    company = item["hiringOrganization"].get("name", "")
                location = ""
                if isinstance(item.get("jobLocation"), dict):
                    addr = item["jobLocation"].get("address", {})
                    if isinstance(addr, dict):
                        location = addr.get("addressLocality", "")
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
                if len(jobs) >= limit: break
        except Exception:
            continue
        if len(jobs) >= limit: break

    # Fallback heuristique si pas de JSON-LD
    if not jobs:
        links = re.findall(r'href=["\'](/(?:emploi|job|offre|annonce|recherche|poste)[^"\']{5,})["\']', html)
        seen  = set()
        for path in links:
            if path in seen or len(jobs) >= limit: break
            seen.add(path)
            full_url = urljoin(search_url, path)
            label    = path.split("/")[-1].replace("-", " ").replace("_", " ").strip()[:80].title()
            jobs.append({
                "id":          make_id(platform, full_url),
                "source_url":  search_url,
                "title":       label or "Offre",
                "company":     "", "location":    "",
                "url":         full_url, "description": "",
                "date":        datetime.now(timezone.utc).isoformat(),
                "type":        guess_type(label),
            })

    return jobs[:limit]


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def scrape_url(url: str, limit: int = 20) -> dict:
    platform      = detect_platform(url)
    keywords      = extract_keywords(url)
    location      = extract_location(url)
    jobs: list    = []
    error         = None
    strategy_used = "unknown"

    try:
        if platform == "indeed":
            # ── Priorité : JobSpy (tls-client, bypass Indeed) ──
            indeed_jt = extract_indeed_jobtype(url)
            try:
                jobs = await scrape_indeed_jobspy(keywords, location, limit, indeed_jt)
                strategy_used = "jobspy_indeed"
                if not jobs:
                    raise ValueError("JobSpy a retourné 0 résultat")
            except Exception as e1:
                # Fallback générique (peu de chances de marcher sur Indeed
                # mais on essaie quand même)
                try:
                    jobs = await scrape_generic_fetch(url, platform, limit)
                    strategy_used = f"generic_fallback (jobspy failed: {e1})"
                except Exception as e2:
                    raise RuntimeError(f"jobspy: {e1} | generic: {e2}")

        else:
            # Toutes les autres plateformes : fetch JSON-LD
            jobs = await scrape_generic_fetch(url, platform, limit)
            strategy_used = "generic_fetch"

    except Exception as e:
        error = str(e)

    return {
        "url":       url,
        "platform":  platform,
        "jobs":      jobs,
        "count":     len(jobs),
        "error":     error,
        "strategy":  strategy_used,
        "scrapedAt": datetime.now(timezone.utc).isoformat(),
    }


# ── Modèles ───────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    urls:           list[str]
    results_wanted: int = 20


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "job-scraper", "version": "3.0.0", "strategy": "jobspy+jsonld"}


@app.post("/scrape")
async def scrape_post(
    body: ScrapeRequest,
    x_scraper_secret: Optional[str] = Header(default=None),
):
    if SCRAPER_SECRET and x_scraper_secret != SCRAPER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not body.urls:
        raise HTTPException(status_code=400, detail="urls requis")

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
    """Debug : GET /scrape?url=https://fr.indeed.com/jobs?q=développeur"""
    if SCRAPER_SECRET and x_scraper_secret != SCRAPER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    result = await scrape_url(url, 10)
    return {"ok": True, "results": [result], "errors": [], "total": result["count"]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)