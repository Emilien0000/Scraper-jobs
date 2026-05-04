# scraper-service/main.py
# Service FastAPI utilisant JobSpy pour le scraping multi-source
# Deploy sur Railway, Render, ou Fly.io
#
# Setup :
#   pip install fastapi uvicorn python-jobspy
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
# Variable d'environnement :
#   SCRAPER_SECRET=ton_secret_partage  (même valeur dans scrape.js)

import os
import asyncio
import re
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import jobspy

app = FastAPI(title="JobSpy Scraper Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

SCRAPER_SECRET = os.environ.get("SCRAPER_SECRET", "")

# ── Détection de plateforme depuis l'URL ──────────────────────────────────────

PLATFORM_MAP = {
    "indeed.com":                   "indeed",
    "linkedin.com":                 "linkedin",
    "glassdoor.com":                "glassdoor",
    "zip_recruiter":                "zip_recruiter",
    "google.com/search":            "google",
    "bayt.com":                     "bayt",
}

def detect_platform(url: str) -> str:
    for pattern, platform in PLATFORM_MAP.items():
        if pattern in url:
            return platform
    return "indeed"  # Fallback

def extract_search_params(url: str) -> dict:
    """
    Extrait search_term, location et les éventuels paramètres
    depuis une URL de recherche (Indeed, LinkedIn, etc.)
    """
    params = {
        "search_term": "alternance",
        "location": "France",
        "job_type": None,
        "country_indeed": "France",
    }

    # Indeed FR : q= et l=
    if "indeed.com" in url:
        q = re.search(r"[?&]q=([^&]+)", url)
        l = re.search(r"[?&]l=([^&]+)", url)
        jt = re.search(r"[?&]jt=([^&]+)", url)
        if q: params["search_term"] = q.group(1).replace("+", " ")
        if l: params["location"] = l.group(1).replace("+", " ")
        if jt:
            jt_val = jt.group(1).lower()
            if "fulltime" in jt_val:    params["job_type"] = "fulltime"
            elif "parttime" in jt_val:  params["job_type"] = "parttime"
            elif "internship" in jt_val: params["job_type"] = "internship"
            elif "contract" in jt_val:  params["job_type"] = "contract"

    # LinkedIn
    elif "linkedin.com" in url:
        kw = re.search(r"keywords=([^&]+)", url)
        loc = re.search(r"location=([^&]+)", url)
        if kw: params["search_term"] = kw.group(1).replace("%20", " ").replace("+", " ")
        if loc: params["location"] = loc.group(1).replace("%20", " ").replace("+", " ")

    # Glassdoor
    elif "glassdoor" in url:
        kw = re.search(r"keyword=([^&]+)", url)
        loc = re.search(r"locT=[^&]+&locId=[^&]+", url)
        if kw: params["search_term"] = kw.group(1).replace("+", " ")

    return params

def guess_type(title: str, description: str = "") -> str:
    text = (title + " " + description).lower()
    if re.search(r"alternance|apprentissage|contrat pro", text): return "alternance"
    if re.search(r"stage|intern|internship", text):              return "stage"
    return "emploi"

# ── Modèles ────────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    urls: list[str]
    results_wanted: int = 20

class JobResult(BaseModel):
    id: str
    source_url: str
    title: str
    company: str
    location: str
    url: str
    description: str
    date: str
    type: str

# ── Endpoint de santé ─────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "jobspy-scraper", "version": "1.0.0"}

# ── Endpoint de scraping ──────────────────────────────────────────────────────

@app.post("/scrape")
async def scrape(
    body: ScrapeRequest,
    x_scraper_secret: Optional[str] = Header(default=None)
):
    # Vérification du secret si défini
    if SCRAPER_SECRET and x_scraper_secret != SCRAPER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not body.urls:
        raise HTTPException(status_code=400, detail="urls requis")

    results = []
    errors  = []

    for url in body.urls:
        try:
            platform = detect_platform(url)
            params   = extract_search_params(url)

            # Appel JobSpy
            loop = asyncio.get_event_loop()
            jobs_df = await loop.run_in_executor(
                None,
                lambda: jobspy.scrape_jobs(
                    site_name=[platform],
                    search_term=params["search_term"],
                    location=params["location"],
                    results_wanted=body.results_wanted,
                    country_indeed=params.get("country_indeed", "France"),
                    job_type=params.get("job_type"),
                    # Proxies optionnel : proxies=["http://user:pass@host:port"]
                )
            )

            jobs_out = []
            for _, row in jobs_df.iterrows():
                title = str(row.get("title", "") or "")
                desc  = str(row.get("description", "") or "")[:400]
                jurl  = str(row.get("job_url", url) or url)
                jid   = f"{platform}-{abs(hash(jurl)) % (10**9):09d}"
                date_val = row.get("date_posted")
                if date_val is not None and str(date_val) != "NaT" and str(date_val) != "nan":
                    try:
                        date_str = str(date_val)
                        # Essaye de parser la date
                        dt = datetime.fromisoformat(date_str.split("T")[0])
                        date_iso = dt.replace(tzinfo=timezone.utc).isoformat()
                    except Exception:
                        date_iso = datetime.now(timezone.utc).isoformat()
                else:
                    date_iso = datetime.now(timezone.utc).isoformat()

                jobs_out.append({
                    "id":          jid,
                    "source_url":  url,
                    "title":       title,
                    "company":     str(row.get("company", "") or ""),
                    "location":    str(row.get("location", "") or ""),
                    "url":         jurl,
                    "description": desc,
                    "date":        date_iso,
                    "type":        guess_type(title, desc),
                })

            results.append({
                "url":       url,
                "platform":  platform,
                "jobs":      jobs_out,
                "count":     len(jobs_out),
                "scrapedAt": datetime.now(timezone.utc).isoformat(),
            })

        except Exception as e:
            errors.append({"url": url, "error": str(e)})

    return {
        "ok":      True,
        "results": results,
        "errors":  errors,
        "total":   sum(r["count"] for r in results),
    }

# ── Endpoint single URL (debug) ────────────────────────────────────────────────

@app.get("/scrape")
async def scrape_get(
    url: str = Query(...),
    x_scraper_secret: Optional[str] = Header(default=None)
):
    """Debug : GET /scrape?url=https://fr.indeed.com/..."""
    return await scrape(
        ScrapeRequest(urls=[url], results_wanted=10),
        x_scraper_secret=x_scraper_secret
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)