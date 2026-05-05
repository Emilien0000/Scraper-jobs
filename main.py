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
#   SCRAPER_SECRET=ton_secret_partagefsss
# ─────────────────────────────────────────────────────────────────────────────
import tls_client
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

# Supabase REST client (léger, sans supabase-py)
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_SERVICE_KEY", "")  # service_role key (bypass RLS)
AUTO_SCRAPE_INTERVAL = int(os.environ.get("AUTO_SCRAPE_INTERVAL", "300"))  # 5 min par défaut
ADZUNA_APP_ID    = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY   = os.environ.get("ADZUNA_APP_KEY", "")
FT_CLIENT_ID     = os.environ.get("FT_CLIENT_ID", "")      # France Travail API OAuth2
FT_CLIENT_SECRET = os.environ.get("FT_CLIENT_SECRET", "")  # https://francetravail.io

app = FastAPI(title="JobScraper JobSpy v3.1", version="3.1.0")

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
    if "adzuna.fr"          in url: return "adzuna"
    if "adzuna.com"         in url: return "adzuna"
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

def extract_indeed_jobtype(url: str) -> tuple[str | None, str | None]:
    """
    Lit les paramètres sc= et jt= d'une URL Indeed.
    Retourne (jobspy_type, filter_category) :
      - jobspy_type   : valeur passée à JobSpy (hint de recherche, peu fiable)
      - filter_category : catégorie à enforcer en post-filtrage
          "alternance" → on ne garde que les offres alternance/apprentissage
          "stage"      → on ne garde que les stages
          None         → pas de post-filtrage
    """
    params = parse_qs(urlparse(url).query)
    sc = params.get("sc", [""])[0]
    jt = params.get("jt", [""])[0]

    # Codes Indeed identifiés : CPAHG=Alternance, QADT5=Apprentissage
    if any(code in sc for code in ("CPAHG", "QADT5")):
        return ("internship", "alternance")
    if "internship" in sc.lower() or jt == "internship":
        return ("internship", "stage")
    if jt in ("contract", "fulltime", "parttime"):
        return (jt, None)
    return (None, None)


def filter_by_category(jobs: list[dict], category: str | None) -> list[dict]:
    """
    Post-filtre les offres selon la catégorie voulue.
    Stratégie permissive : signal direct OU indirect car Indeed tronque
    les descriptions à ~400 chars et "alternance" peut ne pas y apparaître.
    """
    if not category:
        return jobs

    ALTERNANCE_DIRECT = re.compile(
        r"alternance|alternant|apprentissage|contrat d.apprentissage|"
        r"contrat pro|contrat d.alternance|en alternance|par alternance",
        re.IGNORECASE
    )
    ALTERNANCE_INDIRECT = re.compile(
        r"\bcfa\b|\bopco\b|rythme.*entreprise|formation.*entreprise|"
        r"en\s+alternance|par\s+alternance|contrat\s+pro\b|"
        r"école.*entreprise|entreprise.*école|école.*alternance|"
        r"bac\s*[+]\s*[1-5]\s+(?:en\s+)?alternance",
        re.IGNORECASE
    )
    STAGE_RE = re.compile(
        r"\bstage\b|stagiaire|internship|\bintern\b",
        re.IGNORECASE
    )

    filtered = []
    for job in jobs:
        title = job.get("title", "")
        desc  = job.get("description", "")
        jtype = str(job.get("type", "")).lower()
        text  = f"{title} {desc}"

        if category == "alternance":
            if (
                ALTERNANCE_DIRECT.search(text)
                or jtype == "alternance"
                or ALTERNANCE_INDIRECT.search(text)
            ):
                job["type"] = "alternance"
                filtered.append(job)

        elif category == "stage":
            if STAGE_RE.search(text) or jtype == "stage":
                job["type"] = "stage"
                filtered.append(job)

    return filtered

def guess_type(title: str, description: str = "") -> str:
    text = (title + " " + description).lower()
    # Alternance EN PREMIER — sinon "stage en alternance" serait classé stage
    if re.search(r"alternance|alternant|apprentissage|contrat pro|contrat d.alternance", text): return "alternance"
    if re.search(r"\bstage\b|stagiaire|intern\b|internship", text): return "stage"
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
        hours_old       = 240,           # offres des 10 derniers jours (72h trop restrictif)
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
        # IMPORTANT : on check alternance EN PREMIER car JobSpy retourne "internship"
        # pour TOUT (stages ET alternances) — le texte est le seul signal fiable.
        guessed = guess_type(title, desc)
        if guessed == "alternance":
            norm_type = "alternance"
        elif guessed == "stage":
            norm_type = "stage"
        elif "intern" in jtype.lower():
            # JobSpy dit internship mais le texte ne tranché pas → stage par défaut
            norm_type = "stage"
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


# ── Stratégie LinkedIn : JobSpy → LinkedIn ───────────────────────────────────

def _jobspy_linkedin_sync(keywords: str, location: str, results_wanted: int, job_type: str | None) -> list[dict]:
    """Exécution synchrone de JobSpy pour LinkedIn — à appeler via run_in_executor."""
    from jobspy import scrape_jobs

    kwargs = dict(
        site_name          = ["linkedin"],
        search_term        = keywords,
        location           = location or "France",
        results_wanted     = results_wanted,
        hours_old          = 240,
        description_format = "markdown",
        verbose            = 0,
    )
    if job_type:
        kwargs["job_type"] = job_type

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

        guessed = guess_type(title, desc)
        if guessed == "alternance":
            norm_type = "alternance"
        elif guessed == "stage":
            norm_type = "stage"
        elif "intern" in jtype.lower():
            norm_type = "stage"
        else:
            norm_type = "emploi"

        jobs.append({
            "id":          make_id("linkedin", url_job),
            "source_url":  "linkedin",
            "title":       title,
            "company":     company,
            "location":    loc,
            "url":         url_job,
            "description": desc,
            "date":        date,
            "type":        norm_type,
        })
    return jobs


async def scrape_linkedin_jobspy(keywords: str, location: str, limit: int, job_type: str | None = None) -> list[dict]:
    """Wrapper async autour de JobSpy LinkedIn."""
    loop = asyncio.get_event_loop()
    jobs = await loop.run_in_executor(
        _executor,
        _jobspy_linkedin_sync,
        keywords, location, limit, job_type,
    )
    return jobs[:limit]


# ── Stratégie 2 : fetch HTML + JSON-LD (fallback générique) ──────────────────
# Fonctionne pour HelloWork, Adzuna, WTJ, stage.fr, etc.

async def scrape_generic_fetch(search_url: str, platform: str, limit: int = 20) -> list[dict]:
    # Remplacement de httpx par tls_client pour bypasser Cloudflare/Indeed comme le fait JobSpy
    session = tls_client.Session(
        client_identifier="chrome_124",
        random_tls_extension_order=True
    )
    
    # tls_client est synchrone, on l'exécute dans l'executor pour ne pas bloquer FastAPI
    loop = asyncio.get_event_loop()
    r = await loop.run_in_executor(
        None, 
        lambda: session.get(search_url, headers=browser_headers())
    )
    
    html = r.text
    jobs = []

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



# ── Stratégie HelloWork : HTML scraping (BeautifulSoup) ─────────────────────
# HelloWork utilise des composants Lit/Web Components avec des inputs cachés.
# Sélecteur fiable : li[data-id-storage-item-id]
# Technique inspirée de github.com/Zeffut/JobScraper

import re as _re
from datetime import timedelta as _timedelta

def _extract_hellowork_params(url: str) -> dict:
    """Extrait keywords, lieu, contrat depuis une URL HelloWork de recherche."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    path_parts = [p for p in parsed.path.split("/") if p]

    # Paramètre k (HelloWork) ou q (fallback)
    keywords = params.get("k", params.get("q", [""]))[0]
    location = params.get("l", ["France"])[0] or "France"
    contract = params.get("c", [""])[0].lower()

    # Fallback slug
    if not keywords and len(path_parts) >= 2:
        slug = path_parts[-1] if path_parts[-1] not in ("recherche.html", "emploi") else ""
        if slug:
            keywords = slug.replace("-h-f", "").replace("-f-h", "").replace("-", " ").strip()
    if not keywords:
        keywords = "développeur"

    # Détecter contrat dans le path
    path_lower = parsed.path.lower()
    if not contract:
        if "alternance" in path_lower: contract = "alternance"
        elif "stage"    in path_lower: contract = "stage"

    return {"keywords": keywords, "location": location, "contract": contract}


def _parse_hw_relative_date(text: str) -> str:
    """Convertit une date relative FR HelloWork en ISO."""
    now = datetime.now(timezone.utc)
    text = text.lower().strip()
    patterns = [
        (_re.compile(r"il y a (\d+)\s*minute"), lambda m: now - _timedelta(minutes=int(m.group(1)))),
        (_re.compile(r"il y a (\d+)\s*heure"),  lambda m: now - _timedelta(hours=int(m.group(1)))),
        (_re.compile(r"il y a (\d+)\s*jour"),   lambda m: now - _timedelta(days=int(m.group(1)))),
        (_re.compile(r"il y a (\d+)\s*semaine"),lambda m: now - _timedelta(weeks=int(m.group(1)))),
        (_re.compile(r"il y a (\d+)\s*mois"),   lambda m: now - _timedelta(days=int(m.group(1)) * 30)),
        (_re.compile(r"aujourd"),                lambda m: now),
        (_re.compile(r"hier"),                   lambda m: now - _timedelta(days=1)),
    ]
    for pat, handler in patterns:
        m = pat.search(text)
        if m:
            return handler(m).isoformat()
    return now.isoformat()


def _parse_hw_cards_sync(html: str, search_url: str, contract: str, limit: int) -> list[dict]:
    """Parse les cartes HelloWork depuis le HTML brut."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    jobs = []

    # Sélecteur principal (structure 2024-2025 HelloWork)
    cards = soup.select("li[data-id-storage-item-id]")
    if not cards:
        # Fallback : liens d'offres
        seen_hrefs: set = set()
        for link in soup.select("a[href*='/emplois/']"):
            href = link.get("href", "")
            if href in seen_hrefs: continue
            seen_hrefs.add(href)
            parent = link.find_parent("li")
            if parent and parent not in cards:
                cards.append(parent)

    for card in cards:
        if len(jobs) >= limit:
            break
        try:
            job_id = card.get("data-id-storage-item-id", "")

            # URL
            link_el = card.select_one("a[href*='/emplois/']")
            url_job = link_el.get("href", "") if link_el else f"/fr-fr/emplois/{job_id}.html"
            if url_job.startswith("/"):
                url_job = f"https://www.hellowork.com{url_job}"

            # Titre (input caché le plus fiable)
            title_input = card.select_one('input[name="title"]')
            if title_input:
                title = title_input.get("value", "").strip()
            else:
                el = card.select_one("p.tw-typo-l, h3, h2")
                title = el.get_text(strip=True) if el else ""

            # Société
            company_input = card.select_one('input[name="company"]')
            if company_input:
                company = company_input.get("value", "").strip()
            else:
                el = card.select_one("p.tw-typo-s.tw-inline")
                company = el.get_text(strip=True) if el else ""

            # Localisation depuis les tags
            location = "France"
            for tag in card.select("div.tw-tag-secondary-s"):
                text = tag.get_text(strip=True)
                if _re.search(r"\d{2,5}$|^\d{5}", text):
                    location = text
                    break

            # Date relative
            date_el = card.select_one("div.tw-text-grey-500, span.tw-text-grey-500")
            date_text = date_el.get_text(strip=True) if date_el else ""
            date_iso  = _parse_hw_relative_date(date_text) if date_text else datetime.now(timezone.utc).isoformat()

            if not title or not url_job:
                continue

            # Type de contrat
            jtype = guess_type(title, "")
            if contract == "alternance": jtype = "alternance"
            elif contract == "stage":   jtype = "stage"

            jobs.append({
                "id":          make_id("hellowork", url_job),
                "source_url":  search_url,
                "title":       title,
                "company":     company,
                "location":    location,
                "url":         url_job,
                "description": "",
                "date":        date_iso,
                "type":        jtype,
            })
        except Exception as ex:
            print(f"[hellowork] carte skip: {ex}")
            continue

    return jobs


async def scrape_hellowork(search_url: str, limit: int = 20) -> list[dict]:
    """
    Scrape HelloWork par HTML scraping (BeautifulSoup).
    Technique : li[data-id-storage-item-id] + inputs cachés title/company.
    Inspiré de github.com/Zeffut/JobScraper
    """
    hw_params = _extract_hellowork_params(search_url)
    keywords  = hw_params["keywords"]
    location  = hw_params["location"]
    contract  = hw_params["contract"]

    # Construire l'URL de recherche HelloWork
    from urllib.parse import urlencode as _urlencode
    base = "https://www.hellowork.com/fr-fr/emploi/recherche.html"
    qp: dict = {"k": keywords, "l": location}
    if contract:
        qp["c"] = contract
    # Tri par date (d=7 = 7 derniers jours)
    qp["d"] = "7"
    hw_url = f"{base}?{_urlencode(qp)}"
    print(f"[hellowork] URL recherche: {hw_url}")

    headers = {
        "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection":      "keep-alive",
    }

    jobs: list[dict] = []
    try:
        loop = asyncio.get_event_loop()

        def _fetch_and_parse():
            import tls_client as _tls
            sess = _tls.Session(client_identifier="chrome_120", random_tls_extension_order=True)
            r = sess.get(hw_url, headers=headers)
            print(f"[hellowork] status={r.status_code} len={len(r.text)}")
            if r.status_code != 200:
                raise Exception(f"HelloWork HTTP {r.status_code}")
            return _parse_hw_cards_sync(r.text, search_url, contract, limit)

        jobs = await loop.run_in_executor(_executor, _fetch_and_parse)
        print(f"[hellowork] {len(jobs)} offres parsées")

    except Exception as e:
        print(f"[hellowork] scraping error: {e} — fallback JSON-LD")
        jobs = await scrape_generic_fetch(search_url, "hellowork", limit)

    return jobs[:limit]



# ── Stratégie Adzuna : API officielle ────────────────────────────────────────
# Adzuna agrège Indeed, Monster, etc. via une API REST gratuite.
# Clés gratuites sur https://developer.adzuna.com/
# Env vars : ADZUNA_APP_ID, ADZUNA_APP_KEY

def _extract_adzuna_params(url: str) -> dict:
    """Extrait keywords, lieu, contrat depuis une URL Adzuna."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    keywords = params.get("q", params.get("what", ["développeur"]))[0]
    location = params.get("l", params.get("where", [""]))[0]
    contract = params.get("c", [""])[0].lower()
    path_lower = parsed.path.lower()
    if not contract:
        if "alternance" in path_lower or "alternance" in url.lower(): contract = "alternance"
        elif "stage"    in path_lower or "stage"    in url.lower(): contract = "stage"
    return {"keywords": keywords, "location": location, "contract": contract}


async def scrape_adzuna(search_url: str, limit: int = 20) -> list[dict]:
    """
    Scrape Adzuna via leur API REST officielle.
    Nécessite ADZUNA_APP_ID et ADZUNA_APP_KEY dans les variables d'environnement.
    """
    # Relire depuis l'env à chaque appel (comme FT) pour éviter les globals vides
    adzuna_id  = os.environ.get("ADZUNA_APP_ID", "") or ADZUNA_APP_ID
    adzuna_key = os.environ.get("ADZUNA_APP_KEY", "") or ADZUNA_APP_KEY
    if not adzuna_id or not adzuna_key:
        print("[adzuna] ⚠️  ADZUNA_APP_ID / ADZUNA_APP_KEY non définis — skip")
        return []

    az_params = _extract_adzuna_params(search_url)
    keywords  = az_params["keywords"]
    location  = az_params["location"]
    contract  = az_params["contract"]

    # Enrichir les keywords selon le contrat
    if contract == "alternance" and "alternance" not in keywords.lower():
        keywords += " alternance"
    elif contract == "stage" and "stage" not in keywords.lower():
        keywords += " stage"

    # Pas de filtre Python côté Adzuna — on fait confiance au moteur de recherche.
    # L'ancienne logique stem/7chars filtrait trop agressivement (accents composés,
    # variantes françaises...) et rejetait des offres légitimes.
    # Adzuna est une vraie API de recherche : si les résultats semblent hors-sujet,
    # c'est le mot-clé "what" qui est trop générique — pas besoin de sur-filtrer.

    api_params: dict = {
        "app_id":           adzuna_id,
        "app_key":          adzuna_key,
        "results_per_page": min(limit * 2, 50),
        "what":             keywords,
        "sort_by":          "date",
        "max_days_old":     10,
    }
    if location:
        api_params["where"] = location

    jobs: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                "https://api.adzuna.com/v1/api/jobs/fr/search/1",
                params=api_params,
            )
            if r.status_code != 200:
                raise Exception(f"Adzuna API status {r.status_code}: {r.text[:200]}")

            data    = r.json()
            results = data.get("results", [])
            print(f"[adzuna] {len(results)} résultats bruts pour keywords='{keywords}'")

            for item in results:
                title   = item.get("title", "").strip()
                company = item.get("company", {}).get("display_name", "").strip()
                loc     = item.get("location", {}).get("display_name", "").strip() or "France"
                url_job = item.get("redirect_url", "")
                desc    = strip_html(item.get("description", ""))[:400]
                created = item.get("created", "")
                date    = safe_date(created)
                jtype   = guess_type(title, desc)
                if contract == "alternance": jtype = "alternance"
                elif contract == "stage":   jtype = "stage"

                if not title or not url_job:
                    continue

                jobs.append({
                    "id":          make_id("adzuna", url_job),
                    "source_url":  search_url,
                    "title":       title,
                    "company":     company,
                    "location":    loc,
                    "url":         url_job,
                    "description": desc,
                    "date":        date,
                    "type":        jtype,
                })
                if len(jobs) >= limit:
                    break

    except Exception as e:
        print(f"[adzuna] error: {e}")

    print(f"[adzuna] ✅ {len(jobs)} offres après filtre pertinence")
    return jobs[:limit]



# ── Stratégie France Travail : API officielle OAuth2 ─────────────────────────
# Clés gratuites sur https://francetravail.io → "Créer une application"
# Scope requis : "o2dsoffre" (offres d'emploi)
# Env vars : FT_CLIENT_ID, FT_CLIENT_SECRET

_ft_token_cache: dict = {"token": None, "expires_at": 0}

async def _ft_get_token() -> str | None:
    """Récupère un token OAuth2 France Travail (mis en cache)."""
    import time
    # Relire depuis l'env à chaque appel pour éviter les valeurs vides au démarrage
    ft_id     = os.environ.get("FT_CLIENT_ID", "")
    ft_secret = os.environ.get("FT_CLIENT_SECRET", "")
    if not ft_id or not ft_secret:
        print("[francetravail] ⚠️  FT_CLIENT_ID ou FT_CLIENT_SECRET non définis dans l'environnement")
        return None
    now = time.time()
    if _ft_token_cache["token"] and now < _ft_token_cache["expires_at"] - 30:
        return _ft_token_cache["token"]
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://entreprise.francetravail.fr/connexion/oauth2/access_token",
            params={"realm": "/partenaire"},
            data={
                "grant_type": "client_credentials",
                "scope":      "api_offresdemploiv2 o2dsoffre",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=(ft_id, ft_secret),
        )
        if r.status_code != 200:
            print(f"[francetravail] ❌ Token error {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        _ft_token_cache["token"]      = data["access_token"]
        _ft_token_cache["expires_at"] = now + data.get("expires_in", 1490)
        print("[francetravail] ✅ Token OK")
        return _ft_token_cache["token"]


def _extract_ft_params(url: str) -> dict:
    """Extrait keywords, lieu, contrat depuis une URL France Travail."""
    parsed   = urlparse(url)
    params   = parse_qs(parsed.query)
    keywords = params.get("motsCles", params.get("motsCle", params.get("q", ["développeur"])))[0]
    dept     = params.get("departement", params.get("lieux", [""]))[0]
    contract = params.get("typeContrat", [""])[0].upper()
    path_low = parsed.path.lower()
    url_low  = url.lower()
    # Détecter alternance/stage dans l'URL
    if not contract:
        if "alternance" in url_low or "apprentissage" in url_low:
            contract = "ALT"   # code France Travail pour apprentissage/alternance
        elif "stage" in url_low:
            contract = "STG"
    return {"keywords": keywords, "dept": dept, "contract": contract}


async def scrape_francetravail(search_url: str, limit: int = 20) -> list[dict]:
    """
    Scrape France Travail via leur API REST officielle v2.
    Endpoint : GET /offres/search
    """
    token = await _ft_get_token()
    if not token:
        print("[francetravail] ⚠️  Pas de token — FT_CLIENT_ID/FT_CLIENT_SECRET manquants ?")
        return []

    ft_p     = _extract_ft_params(search_url)
    keywords = ft_p["keywords"]
    dept     = ft_p["dept"]
    contract = ft_p["contract"]

    # Enrichir les keywords selon contrat
    if contract == "ALT" and "alternance" not in keywords.lower():
        keywords += " alternance"
    elif contract == "STG" and "stage" not in keywords.lower():
        keywords += " stage"

    api_params: dict = {
        "motsCles":    keywords,
        "sort":        1,        # tri par date
        "range":       f"0-{min(limit * 2, 149)}",
    }
    if dept:
        api_params["departement"] = dept
    if contract:
        api_params["typeContrat"] = contract

    # Filtre de pertinence (même logique qu'Adzuna)
    kw_tokens = [w.strip().lower() for w in re.split(r"[\s,;/+]+", keywords) if len(w.strip()) >= 3]
    STOP_WORDS = {"les", "des", "pour", "avec", "dans", "sur", "par", "une", "and", "the", "stage", "alternance", "apprentissage"}
    kw_tokens  = [t for t in kw_tokens if t not in STOP_WORDS]
    kw_pattern = re.compile("|".join(re.escape(t) for t in kw_tokens), re.IGNORECASE) if kw_tokens else None

    # Normaliser les keywords (accents NFC, strip espaces)
    import unicodedata
    keywords = unicodedata.normalize("NFC", keywords).strip()
    api_params["motsCles"] = keywords

    print(f"[francetravail] 🔍 Appel API — keywords='{keywords}' dept='{dept}' contract='{contract}' range='{api_params['range']}'")

    jobs: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search",
                params=api_params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept":        "application/json",
                },
            )
            print(f"[francetravail] 📡 HTTP {r.status_code} — {r.text[:300]}")
            if r.status_code == 206 or r.status_code == 200:
                data    = r.json()
                results = data.get("resultats", [])
            elif r.status_code == 204:
                print("[francetravail] ⚠️  HTTP 204 — aucun résultat (critères trop restrictifs ?)")
                results = []
            else:
                raise Exception(f"FT API status {r.status_code}: {r.text[:200]}")

            print(f"[francetravail] {len(results)} résultats bruts pour '{keywords}'")

            for item in results:
                title    = item.get("intitule", "").strip()
                url_job  = item.get("origineOffre", {}).get("urlOrigine", "") or                            f"https://candidat.francetravail.fr/offres/recherche/detail/{item.get('id','')}"
                company  = item.get("entreprise", {}).get("nom", "").strip()
                loc_obj  = item.get("lieuTravail", {})
                loc      = loc_obj.get("libelle", "") or loc_obj.get("codePostal", "") or "France"
                desc_raw = item.get("description", "")
                desc     = strip_html(desc_raw)[:400]
                date     = safe_date(item.get("dateCreation") or item.get("dateActualisation"))

                # Type de contrat France Travail → nos catégories
                ft_type   = item.get("typeContrat", "").upper()
                ft_nature = item.get("natureContrat", "").upper()
                ft_libelle = item.get("typeContratLibelle", "").upper()

                is_alt = (
                    ft_type in ("ALT", "APP")
                    or "ALTERNANCE" in ft_nature or "ALTERNANCE" in ft_libelle
                    or "APPRENTISSAGE" in ft_nature or "APPRENTISSAGE" in ft_libelle
                )
                is_stg = ft_type == "STG" or "STAGE" in ft_nature or "STAGE" in ft_libelle

                if is_alt:
                    jtype = "alternance"
                elif is_stg:
                    jtype = "stage"
                else:
                    jtype = guess_type(title, desc)

                if not title or not url_job:
                    continue

                # ── Filtre strict selon le contrat demandé ──────────────────────
                # Si l'utilisateur filtre sur "alternance", on rejette toute offre
                # que FT ne classe pas explicitement comme ALT/APP — même si le
                # moteur FT l'a renvoyée. Idem pour "stage".
                if contract == "ALT" and jtype != "alternance":
                    # Dernière chance : le texte confirme-t-il l'alternance ?
                    text_check = f"{title} {desc}".lower()
                    if not re.search(r"alternance|alternant|apprentissage|contrat pro|contrat d.alternance", text_check):
                        print(f"[francetravail] ❌ rejeté (non-alternance): '{title}' [{ft_type}/{ft_nature}]")
                        continue
                    jtype = "alternance"  # confirmé par le texte

                elif contract == "STG" and jtype != "stage":
                    text_check = f"{title} {desc}".lower()
                    if not re.search(r"stage|stagiaire|internship", text_check):
                        print(f"[francetravail] ❌ rejeté (non-stage): '{title}' [{ft_type}/{ft_nature}]")
                        continue
                    jtype = "stage"

                # Filtre de pertinence mots-clés (uniquement si pas de contrat spécifique)
                if kw_pattern and not contract:
                    if not kw_pattern.search(title) and not kw_pattern.search(desc):
                        print(f"[francetravail] ❌ hors-sujet ignoré: '{title}'")
                        continue

                jobs.append({
                    "id":          make_id("francetravail", url_job),
                    "source_url":  search_url,
                    "title":       title,
                    "company":     company,
                    "location":    loc,
                    "url":         url_job,
                    "description": desc,
                    "date":        date,
                    "type":        jtype,
                })
                if len(jobs) >= limit:
                    break

    except Exception as e:
        print(f"[francetravail] error: {e}")

    print(f"[francetravail] ✅ {len(jobs)} offres après filtre")
    return jobs[:limit]



# ── Stratégie Welcome to the Jungle : DÉSACTIVÉE (IP datacenter bloquée) ────
# WTTJ bloque toutes les IPs datacenter (Render, Railway, Fly.io, etc.)
# via une allowlist réseau stricte — "Host not in allowlist".
# Le scraping Algolia n'est possible que depuis une IP résidentielle ou un proxy.
# Options : ScraperAPI, BrightData, Oxylabs, ou proxy résidentiel custom.
# Pour l'instant : retourne [] sans crasher.

def _extract_wttj_params(url: str) -> dict:
    """Extrait keywords, lieu et type de contrat depuis une URL WTTJ."""
    import unicodedata
    parsed   = urlparse(url)
    params   = parse_qs(parsed.query)
    url_low  = url.lower()

    keywords = params.get("query", params.get("q", [""]))[0]
    if not keywords:
        parts = [p for p in parsed.path.split("/") if p and p not in ("fr", "en", "jobs", "search")]
        if parts:
            keywords = parts[-1].replace("-", " ").strip()
    if not keywords:
        keywords = "développeur"

    location = params.get("aroundLatLngViaIP", [""])[0] or params.get("location", ["France"])[0]

    contracts = params.get("refinementList[contract_type][]", [])
    contract = contracts[0] if contracts else ""
    if not contract:
        if "alternance" in url_low or "apprenticeship" in url_low:
            contract = "APPRENTICESHIP"
        elif "stage" in url_low or "internship" in url_low:
            contract = "INTERNSHIP"

    return {"keywords": keywords, "location": location, "contract": contract}


async def scrape_wttj(search_url: str, limit: int = 20) -> list[dict]:
    """
    WTTJ bloque les IPs datacenter — retourne [] sans crasher.
    Le log avertit clairement de la cause sans polluer les autres utilisateurs.
    """
    print("[wttj] ⚠️  WTTJ bloque les IPs datacenter (Render/Railway). "
          "Pour activer WTTJ, un proxy résidentiel est nécessaire (ScraperAPI, BrightData…).")
    return []




# ── Dispatcher ────────────────────────────────────────────────────────────────

async def scrape_url(url: str, limit: int = 20) -> dict:
    platform      = detect_platform(url)
    keywords      = extract_keywords(url)
    location      = extract_location(url)
    jobs: list    = []
    error         = None
    strategy_used = "unknown"

    # 1. On extrait la catégorie d'entrée de jeu, pour TOUTES les stratégies
    # Extraire la catégorie selon la plateforme
    if platform == "indeed":
        _, filter_cat = extract_indeed_jobtype(url)
    elif platform == "linkedin":
        if "alternance" in url.lower():
            filter_cat = "alternance"
        elif "stage" in url.lower() or "internship" in url.lower():
            filter_cat = "stage"
        else:
            filter_cat = None
    elif platform == "hellowork":
        hw_p = _extract_hellowork_params(url)
        c = hw_p.get("contract", "")
        if c in ("alternance", "apprentissage"): filter_cat = "alternance"
        elif c == "stage":                        filter_cat = "stage"
        else:                                     filter_cat = None
    elif platform == "adzuna":
        az_p = _extract_adzuna_params(url)
        c = az_p.get("contract", "")
        if c == "alternance": filter_cat = "alternance"
        elif c == "stage":    filter_cat = "stage"
        else:                 filter_cat = None
    elif platform == "wtj":
        wttj_p = _extract_wttj_params(url)
        c = wttj_p.get("contract", "")
        if c == "APPRENTICESHIP":  filter_cat = "alternance"
        elif c == "INTERNSHIP":    filter_cat = "stage"
        else:                      filter_cat = None
    elif platform == "francetravail":
        ft_p = _extract_ft_params(url)
        c = ft_p.get("contract", "")
        if c == "ALT":  filter_cat = "alternance"
        elif c == "STG": filter_cat = "stage"
        else:            filter_cat = None
    else:
        filter_cat = None

    try:
        if platform == "indeed":
            search_keywords = keywords
            # Forcer le mot pour aider JobSpy
            if filter_cat == "alternance" and "alternance" not in search_keywords.lower():
                search_keywords += " alternance"
            
            try:
                # 2. On demande à JobSpy de ratisser TRES large (ex: 20 * 8 = 160 offres) 
                # pour compenser le fait qu'il n'utilise pas le paramètre d'URL exact.
                fetch_limit = limit * 8 if filter_cat else limit
                # FIX : passer job_type à JobSpy pour qu'il filtre aussi côté Indeed
                jobspy_type = "internship" if filter_cat in ("alternance", "stage") else None
                jobs = await scrape_indeed_jobspy(search_keywords, location, fetch_limit, jobspy_type)
                strategy_used = "jobspy_indeed"
                
                # On ne lève plus d'erreur ici si c'est vide, on laisse couler vers le filtre final
            except Exception as e1:
                # Si JobSpy plante sec, le fallback va utiliser la VRAIE URL avec le paramètre sc=... 
                # et le nouveau tls_client passera les sécurités !
                jobs = await scrape_generic_fetch(url, platform, limit * 4)
                strategy_used = f"generic_fallback (jobspy failed: {e1})"
        elif platform == "linkedin":
            # Détection du job type depuis les paramètres de l'URL LinkedIn
            # ex: ?f_JT=I (internship), ?f_JT=F (fulltime), ?keywords=...
            params = parse_qs(urlparse(url).query)
            jt_param = params.get("f_JT", [""])[0].upper()
            linkedin_type_map = {"I": "internship", "F": "fulltime", "P": "parttime", "C": "contract", "T": "contract"}
            jobspy_type = linkedin_type_map.get(jt_param, None)

            # Enrichir les keywords si alternance dans l'URL
            search_keywords = keywords
            if "alternance" in url.lower() and "alternance" not in search_keywords.lower():
                search_keywords += " alternance"

            try:
                jobs = await scrape_linkedin_jobspy(search_keywords, location, limit * 3, jobspy_type)
                strategy_used = "jobspy_linkedin"
            except Exception as e1:
                jobs = await scrape_generic_fetch(url, platform, limit * 2)
                strategy_used = f"generic_fallback (linkedin jobspy failed: {e1})"

        elif platform == "hellowork":
            try:
                jobs = await scrape_hellowork(url, limit)
                strategy_used = "hellowork_api"
            except Exception as e_hw:
                jobs = await scrape_generic_fetch(url, platform, limit * 2)
                strategy_used = f"generic_fallback (hellowork failed: {e_hw})"

        elif platform == "adzuna":
            try:
                jobs = await scrape_adzuna(url, limit)
                strategy_used = "adzuna_api"
            except Exception as e_az:
                jobs = await scrape_generic_fetch(url, platform, limit * 2)
                strategy_used = f"generic_fallback (adzuna failed: {e_az})"

        elif platform == "wtj":
            try:
                jobs = await scrape_wttj(url, limit)
                strategy_used = "wttj_algolia"
            except Exception as e_wttj:
                jobs = await scrape_generic_fetch(url, platform, limit * 2)
                strategy_used = f"generic_fallback (wttj failed: {e_wttj})"

        elif platform == "francetravail":
            try:
                jobs = await scrape_francetravail(url, limit)
                strategy_used = "francetravail_api"
            except Exception as e_ft:
                jobs = await scrape_generic_fetch(url, platform, limit * 2)
                strategy_used = f"generic_fallback (francetravail failed: {e_ft})"

        else:
            # Toutes les autres plateformes
            jobs = await scrape_generic_fetch(url, platform, limit * 2)
            strategy_used = "generic_fetch"

    except Exception as e:
        error = str(e)

    # 3. LE BLINDAGE FINAL : On post-filtre systématiquement ici, peu importe la stratégie !
    if filter_cat:
        jobs = filter_by_category(jobs, filter_cat)

    # On s'assure de ne renvoyer que la limite demandée (20)
    # IMPORTANT : forcer source_url = URL réelle de recherche (pas la string fixe "indeed"/"linkedin")
    # pour que le frontend puisse matcher j.sourceUrl === filterUrl
    for job in jobs:
        job["source_url"] = url
    final_jobs = jobs[:limit]

    return {
        "url":       url,
        "platform":  platform,
        "jobs":      final_jobs,
        "count":     len(final_jobs),
        "error":     error,
        "strategy":  strategy_used,
        "scrapedAt": datetime.now(timezone.utc).isoformat(),
    }


# ── Modèles ───────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    urls:           list[str]
    results_wanted: int = 20
    user_id:        Optional[str] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "job-scraper", "version": "3.1.0", "strategy": "jobspy_indeed+linkedin+hellowork+adzuna+jsonld"}


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

    # Si un user_id est fourni, on upsert directement dans Supabase avec user_id
    if body.user_id and SUPABASE_URL and SUPABASE_KEY:
        jobs_to_insert = []
        seen = set()
        for result in results:
            for job in (result.get("jobs") or []):
                if job.get("url") and job["url"] not in seen:
                    seen.add(job["url"])
                    jobs_to_insert.append({
                        "user_id":     body.user_id,
                        "source_url":  result.get("url", ""), 
                        "title":       job.get("title", "(sans titre)"),
                        "company":     job.get("company", ""),
                        "location":    job.get("location", ""),
                        "url":         job["url"],
                        "description": job.get("description", ""),
                        "date":        _normalize_date(job.get("date")),
                        "type":        job.get("type", "emploi"),
                    })
        if jobs_to_insert:
            try:
                # Purge les anciens jobs de cet user avant d'insérer les nouveaux
                await _supabase_delete("jb_jobs", body.user_id)
                print(f"[scrape] 🗑️  Anciens jobs purgés pour user {body.user_id[:8]}…")
                for i in range(0, len(jobs_to_insert), 50):
                    await _supabase_upsert("jb_jobs", jobs_to_insert[i:i+50], on_conflict="user_id,url")
                print(f"[scrape] 💾 {len(jobs_to_insert)} offres insérées pour user {body.user_id[:8]}…")
            except Exception as e:
                print(f"[scrape] ❌ Upsert erreur: {e}")

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


# ── Auto-scraper background ───────────────────────────────────────────────────
# Toutes les AUTO_SCRAPE_INTERVAL secondes (défaut 300 = 5 min) :
#   1. Lit la table user_filters dans Supabase pour récupérer tous les liens actifs
#   2. Scrappe chaque URL unique (dédupliquée entre users)
#   3. Upsert les offres dans jb_jobs
#   4. Met à jour lastScraped + jobCount dans user_filters pour chaque user

async def _supabase_get(path: str) -> list[dict]:
    """Lecture REST Supabase (service role)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/{path}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Accept":        "application/json",
            }
        )
        r.raise_for_status()
        return r.json()

async def _supabase_upsert(table: str, rows: list[dict], on_conflict: str = "id") -> None:
    """Upsert REST Supabase (service role)."""
    if not SUPABASE_URL or not SUPABASE_KEY or not rows:
        return
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey":         SUPABASE_KEY,
                "Authorization":  f"Bearer {SUPABASE_KEY}",
                "Content-Type":   "application/json",
                "Prefer":         f"resolution=merge-duplicates,return=minimal",
            },
            params={"on_conflict": on_conflict},
            json=rows,
        )
        r.raise_for_status()

async def _supabase_delete(table: str, user_id: str) -> None:
    """Supprime tous les jobs d un user (DELETE REST Supabase, service role)."""
    if not SUPABASE_URL or not SUPABASE_KEY or not user_id:
        return
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.delete(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer":        "return=minimal",
            },
            params={"user_id": f"eq.{user_id}"},
        )
        r.raise_for_status()

async def _supabase_patch(table: str, row_id: str, patch: dict) -> None:
    """PATCH d'une ligne Supabase (service role)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "return=minimal",
            },
            params={"id": f"eq.{row_id}"},
            json=patch,
        )
        r.raise_for_status()

def _normalize_date(val) -> str:
    if not val:
        return datetime.now(timezone.utc).isoformat()
    try:
        return dateparser.parse(str(val)).replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

async def auto_scrape_cycle() -> None:
    """Un cycle complet d'auto-scrape pour tous les users."""
    now = datetime.now(timezone.utc)
    print(f"[auto-scrape] Début cycle — {now.isoformat()}")

    # Purge des jobs trop anciens (> 7 jours) pour garder la table propre
    if SUPABASE_URL and SUPABASE_KEY:
        cutoff = (now.replace(hour=0, minute=0, second=0) - __import__("datetime").timedelta(days=7)).isoformat()
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.delete(
                    f"{SUPABASE_URL}/rest/v1/jb_jobs",
                    headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "return=minimal"},
                    params={"date": f"lt.{cutoff}"},
                )
                print(f"[auto-scrape] 🧹 Purge jobs > 7j : status {r.status_code}")
        except Exception as e:
            print(f"[auto-scrape] ⚠️  Purge échouée : {e}")
    try:
        # 1. Lire tous les user_filters
        rows = await _supabase_get("user_filters?select=id,filters")
    except Exception as e:
        print(f"[auto-scrape] ❌ Lecture user_filters: {e}")
        return

    # 2. Construire la map url → liste de (userId, filterIndex)
    url_to_users: dict[str, list[tuple[str, int]]] = {}
    for row in rows:
        filters = row.get("filters") or []
        if not isinstance(filters, list):
            continue
        for idx, f in enumerate(filters):
            if isinstance(f, dict) and f.get("enabled") and f.get("url"):
                url = f["url"]
                url_to_users.setdefault(url, []).append((row["id"], idx))

    if not url_to_users:
        print("[auto-scrape] Aucun lien actif trouvé.")
        return

    print(f"[auto-scrape] {len(url_to_users)} URL(s) unique(s) à scraper pour {len(rows)} user(s)")

    # 3. Scraper toutes les URLs uniques (en parallèle, max 5 à la fois)
    sem = asyncio.Semaphore(5)
    scrape_results: dict[str, dict] = {}

    async def scrape_one(url: str):
        async with sem:
            try:
                result = await scrape_url(url, limit=20)
                scrape_results[url] = result
                print(f"[auto-scrape] ✅ {url[:60]}… → {result['count']} offres")
            except Exception as e:
                print(f"[auto-scrape] ❌ {url[:60]}… : {e}")
                scrape_results[url] = {"url": url, "jobs": [], "count": 0, "error": str(e), "scrapedAt": datetime.now(timezone.utc).isoformat()}

    await asyncio.gather(*[scrape_one(u) for u in url_to_users])

    # 4. Upsert les jobs dans jb_jobs — UN PAR USER pour respecter l isolation
    total_inserted = 0
    for row in rows:
        user_id = row["id"]
        filters = row.get("filters") or []
        if not isinstance(filters, list):
            continue

        user_urls = {f["url"] for f in filters if isinstance(f, dict) and f.get("enabled") and f.get("url")}
        if not user_urls:
            continue

        user_jobs = []
        seen_for_user = set()
        for url in user_urls:
            result = scrape_results.get(url)
            if not result:
                continue
            for job in (result.get("jobs") or []):
                if job.get("url") and job["url"] not in seen_for_user:
                    seen_for_user.add(job["url"])
                    user_jobs.append({
                        "user_id":     user_id,
                        "source_url":  url,
                        "title":       job.get("title", "(sans titre)"),
                        "company":     job.get("company", ""),
                        "location":    job.get("location", ""),
                        "url":         job["url"],
                        "description": job.get("description", ""),
                        "date":        _normalize_date(job.get("date")),
                        "type":        job.get("type", "emploi"),
                        "scraped_at":  datetime.now(timezone.utc).isoformat(),
                    })

        if user_jobs:
            try:
                batch_size = 50
                for i in range(0, len(user_jobs), batch_size):
                    await _supabase_upsert("jb_jobs", user_jobs[i:i+batch_size], on_conflict="user_id,url")
                total_inserted += len(user_jobs)
                print(f"[auto-scrape] 💾 {len(user_jobs)} offres upsertées pour user {user_id[:8]}…")
            except Exception as e:
                print(f"[auto-scrape] ❌ Upsert jb_jobs user {user_id[:8]}: {e}")

    print(f"[auto-scrape] 💾 Total : {total_inserted} offres insérées.")

    # 5. Mettre à jour lastScraped + jobCount dans user_filters pour chaque user
    for row in rows:
        filters = row.get("filters") or []
        if not isinstance(filters, list):
            continue
        updated = False
        for idx, f in enumerate(filters):
            if isinstance(f, dict) and f.get("enabled") and f.get("url"):
                url = f["url"]
                if url in scrape_results:
                    res = scrape_results[url]
                    filters[idx]["lastScraped"] = res.get("scrapedAt", datetime.now(timezone.utc).isoformat())
                    filters[idx]["jobCount"]    = res.get("count", 0)
                    updated = True
        if updated:
            try:
                await _supabase_patch("user_filters", row["id"], {"filters": filters})
            except Exception as e:
                print(f"[auto-scrape] ❌ Patch user_filters {row['id']}: {e}")

    print(f"[auto-scrape] ✅ Cycle terminé — {total_inserted} offres insérées.")


async def auto_scrape_loop() -> None:
    """Boucle infinie qui lance un cycle toutes les AUTO_SCRAPE_INTERVAL secondes."""
    # Attendre 30s au démarrage pour laisser l'appli se stabiliser
    await asyncio.sleep(30)
    while True:
        try:
            await auto_scrape_cycle()
        except Exception as e:
            print(f"[auto-scrape] ❌ Erreur inattendue dans le cycle: {e}")
        await asyncio.sleep(AUTO_SCRAPE_INTERVAL)


@app.on_event("startup")
async def startup_event():
    """Lance le scheduler background au démarrage de FastAPI."""
    if SUPABASE_URL and SUPABASE_KEY:
        asyncio.create_task(auto_scrape_loop())
        print(f"[auto-scrape] 🚀 Scheduler démarré — cycle toutes les {AUTO_SCRAPE_INTERVAL}s")
    else:
        print("[auto-scrape] ⚠️  SUPABASE_URL / SUPABASE_SERVICE_KEY non définis — scheduler désactivé")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)