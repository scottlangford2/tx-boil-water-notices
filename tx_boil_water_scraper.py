#!/usr/bin/env python3
"""
Texas Active Boil Water Notice Scraper
=======================================
Scrapes Texas water utility and municipal websites to find CURRENTLY ACTIVE
boil water notices/advisories.

Sources:
  1. MunicipalOps.com — aggregator for ~100+ Houston-area MUDs/districts
  2. SWWC (Essential Utilities) Texas Neighborhood Dashboard — multi-county
  3. Consolidated WSC — East Texas alerts page
  4. Major city utility pages — Houston, San Antonio, Austin, Dallas, Fort Worth, etc.
  5. Google News — catches recent BWN announcements statewide

Output: CSV and JSON of entities with currently active boil water notices.

Requirements:
    pip install requests beautifulsoup4 lxml
"""

import csv
import json
import logging
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin, quote_plus

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1.0  # politeness delay between requests

# Phrases that signal an ACTIVE boil water notice (case-insensitive).
# These are intentionally specific to avoid false positives from pages
# that merely link to "Boil Water Notice Information" etc.
BWN_ACTIVE_PHRASES = [
    "boil water notice issued",
    "boil water notice is in effect",
    "boil water notice has been issued",
    "boil water advisory issued",
    "boil water advisory is in effect",
    "boil water advisory has been issued",
    "under a boil water",
    "subject to a boil water",
    "customers should boil",
    "advised to boil",
    "must boil water",
    "must boil all water",
    "boil your water",
    "boil all water",
    "do not use water",
    "do not consume water",
    "do not drink the water",
    "unsafe to drink",
    "until further notice",
    "precautionary boil",
    "due to a line break",
    "due to a water line",
    "due to loss of pressure",
    "low pressure",
]

# Keywords that signal the notice has been LIFTED / is no longer active
BWN_LIFTED_KEYWORDS = [
    "rescind",
    "lifted",
    "no longer in effect",
    "has been cancelled",
    "has been canceled",
    "all clear",
    "safe to drink",
    "notice is over",
    "no active",
    "no current",
    "not currently under",
    "no boil water",
    "there are no",
]


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def is_active_bwn_text(text: str) -> bool:
    """Return True if text describes an ACTIVE boil water notice."""
    lower = text.lower()
    has_bwn = any(phrase in lower for phrase in BWN_ACTIVE_PHRASES)
    is_lifted = any(kw in lower for kw in BWN_LIFTED_KEYWORDS)
    return has_bwn and not is_lifted


def extract_date_from_text(text: str) -> str:
    """Try to pull a date from free-form text."""
    patterns = [
        r"(\d{1,2}/\d{1,2}/\d{2,4})",
        r"(\d{1,2}-\d{1,2}-\d{2,4})",
        r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})",
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def classify_entity(name: str) -> str:
    """Classify entity type from its name."""
    upper = name.upper()
    if "MUD" in upper or "MUNICIPAL UTILITY" in upper or "M.U.D." in upper:
        return "Municipal Utility District (MUD)"
    if "WSC" in upper or "WATER SUPPLY CORP" in upper:
        return "Water Supply Corporation (WSC)"
    if "SUD" in upper or "SPECIAL UTILITY" in upper:
        return "Special Utility District (SUD)"
    if "WCID" in upper or "WATER CONTROL" in upper:
        return "Water Control & Improvement District (WCID)"
    if "FWSD" in upper or "FRESH WATER" in upper:
        return "Fresh Water Supply District (FWSD)"
    if "PUD" in upper or "PUBLIC UTILITY" in upper:
        return "Public Utility District (PUD)"
    if any(x in upper for x in ["CITY OF", "TOWN OF", "VILLAGE OF"]):
        return "Municipality"
    if "COUNTY" in upper:
        return "County"
    if "RURAL WATER" in upper:
        return "Rural Water System"
    if "WATER DISTRICT" in upper:
        return "Water District"
    if "WATER AUTH" in upper:
        return "Water Authority"
    if "RIVER AUTH" in upper:
        return "River Authority"
    return "Water System"


# ---------------------------------------------------------------------------
# 1. MunicipalOps.com — Houston-area MUDs/districts aggregator
# ---------------------------------------------------------------------------

def scrape_municipalops(session: requests.Session) -> list[dict]:
    """
    Scrape municipalops.com/status/ for active boil water notices.
    Structure: <h4> section headers, <ul><li> entries with district links
    and status text like "Boil water notice ... as of MM-DD-YY"
    """
    url = "https://municipalops.com/status/"
    log.info(f"Scraping MunicipalOps: {url}")
    notices = []

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Find the BWN section by heading text
        for heading in soup.find_all(["h3", "h4"]):
            heading_text = heading.get_text(strip=True).lower()
            if "boil water" not in heading_text:
                continue

            # Walk siblings until next heading to find the list
            sibling = heading.find_next_sibling()
            while sibling and sibling.name not in ("h3", "h4"):
                if sibling.name == "ul":
                    for li in sibling.find_all("li"):
                        text = li.get_text(strip=True)
                        link_el = li.find("a")
                        entity_name = link_el.get_text(strip=True) if link_el else ""
                        entity_url = link_el["href"] if link_el and link_el.get("href") else ""
                        if entity_url and not entity_url.startswith("http"):
                            entity_url = urljoin(url, entity_url)

                        # Check if this is active (not rescinded)
                        if is_active_bwn_text(text):
                            notices.append({
                                "entity_name": entity_name,
                                "entity_type": classify_entity(entity_name),
                                "status": "Active",
                                "notice_text": text,
                                "date": extract_date_from_text(text),
                                "source": "MunicipalOps",
                                "source_url": url,
                                "entity_url": entity_url,
                            })
                        elif "rescind" in text.lower() or "lifted" in text.lower():
                            log.info(f"  [Rescinded] {entity_name}: {text[:80]}")
                        else:
                            # Possibly active — include if text mentions boil water
                            # in a way that suggests an active notice
                            if "boil water notice" in text.lower() and "as of" in text.lower():
                                notices.append({
                                    "entity_name": entity_name,
                                    "entity_type": classify_entity(entity_name),
                                    "status": "Possibly Active",
                                    "notice_text": text,
                                    "date": extract_date_from_text(text),
                                    "source": "MunicipalOps",
                                    "source_url": url,
                                    "entity_url": entity_url,
                                })
                sibling = sibling.find_next_sibling() if sibling else None

    except requests.exceptions.RequestException as e:
        log.warning(f"Failed to scrape MunicipalOps: {e}")

    log.info(f"  MunicipalOps: found {len(notices)} active notices")
    return notices


# ---------------------------------------------------------------------------
# 2. SWWC (Essential Utilities) Texas Neighborhood Dashboard
# ---------------------------------------------------------------------------

def scrape_swwc_dashboard(session: requests.Session) -> list[dict]:
    """
    Scrape swwc.com/texas/neighborhood-dashboard/ for neighborhoods
    with non-"Good" status (which would include boil water notices).
    Structure: HTML table with columns: County, Water System Name,
    Detailed Neighborhood, State of the Neighborhood.
    """
    url = "https://www.swwc.com/texas/neighborhood-dashboard/"
    log.info(f"Scraping SWWC Dashboard: {url}")
    notices = []

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers:
                continue

            # Find column indices
            status_idx = None
            name_idx = None
            county_idx = None
            neighborhood_idx = None
            for i, h in enumerate(headers):
                if "state" in h or "status" in h:
                    status_idx = i
                if "water system" in h:
                    name_idx = i
                if "county" in h:
                    county_idx = i
                if "neighborhood" in h or "detailed" in h:
                    neighborhood_idx = i

            if status_idx is None:
                continue

            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) <= status_idx:
                    continue

                status_text = cells[status_idx].get_text(strip=True)
                # Only report non-"Good" statuses (includes BWN, outage, etc.)
                if status_text.lower() != "good" and status_text:
                    system_name = cells[name_idx].get_text(strip=True) if name_idx is not None and len(cells) > name_idx else ""
                    county = cells[county_idx].get_text(strip=True) if county_idx is not None and len(cells) > county_idx else ""
                    neighborhood = cells[neighborhood_idx].get_text(strip=True) if neighborhood_idx is not None and len(cells) > neighborhood_idx else ""

                    entity_name = system_name or neighborhood
                    is_bwn = "boil" in status_text.lower()

                    notices.append({
                        "entity_name": entity_name,
                        "entity_type": classify_entity(entity_name),
                        "status": status_text,
                        "notice_text": f"{county} - {neighborhood} - {status_text}",
                        "date": "",
                        "source": "SWWC Dashboard",
                        "source_url": url,
                        "entity_url": "",
                        "county": county,
                        "neighborhood": neighborhood,
                    })

    except requests.exceptions.RequestException as e:
        log.warning(f"Failed to scrape SWWC Dashboard: {e}")

    log.info(f"  SWWC Dashboard: found {len(notices)} non-Good entries")
    return notices


# ---------------------------------------------------------------------------
# 3. Consolidated WSC — East Texas
# ---------------------------------------------------------------------------

def scrape_consolidated_wsc(session: requests.Session) -> list[dict]:
    """
    Scrape consolidatedwsc.com/alerts for active boil water notices.
    Structure: <h2>/<h3> headings with "Boil Water Notice XXXXXXX - Area Name"
    followed by paragraphs with dates and affected locations.
    """
    url = "https://consolidatedwsc.com/alerts"
    log.info(f"Scraping Consolidated WSC: {url}")
    notices = []

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        for heading in soup.find_all(["h2", "h3", "h4"]):
            text = heading.get_text(strip=True)
            if "boil water" not in text.lower():
                continue
            # Skip generic section headers like "Boil Water Advisories"
            if text.lower().strip() in ("boil water advisories", "boil water notices", "boil water"):
                continue
            # Skip rescind headings
            if "rescind" in text.lower():
                continue

            if True:  # keep indentation level
                # Extract area name from heading like "Boil Water Notice 1130033 - Oak Grove Area"
                # Try "Notice XXXX - Area Name" pattern first
                area_match = re.search(r"\d+\s*[-–]\s*(.+?)(?:\s+Area)?$", text)
                if not area_match:
                    area_match = re.search(r"[-–]\s*(.+?)(?:\s+Area)?$", text)
                area_name = area_match.group(1).strip() if area_match else text

                # Gather detail from following paragraphs
                detail_parts = []
                sibling = heading.find_next_sibling()
                while sibling and sibling.name not in ("h2", "h3", "h4"):
                    sib_text = sibling.get_text(strip=True)
                    if sib_text:
                        detail_parts.append(sib_text)
                    sibling = sibling.find_next_sibling() if sibling else None

                detail = " ".join(detail_parts)
                date = extract_date_from_text(detail) or extract_date_from_text(text)

                # Skip if explicitly rescinded
                combined = f"{text} {detail}".lower()
                if any(kw in combined for kw in BWN_LIFTED_KEYWORDS):
                    log.info(f"  [Lifted] Consolidated WSC: {text[:80]}")
                    continue

                notices.append({
                    "entity_name": f"Consolidated WSC - {area_name}",
                    "entity_type": "Water Supply Corporation (WSC)",
                    "status": "Active",
                    "notice_text": f"{text}. {detail[:300]}",
                    "date": date,
                    "source": "Consolidated WSC",
                    "source_url": url,
                    "entity_url": url,
                })

    except requests.exceptions.RequestException as e:
        log.warning(f"Failed to scrape Consolidated WSC: {e}")

    log.info(f"  Consolidated WSC: found {len(notices)} active notices")
    return notices


# ---------------------------------------------------------------------------
# 4. Major City / Utility Website Scrapers
# ---------------------------------------------------------------------------

# Each entry: (entity_name, url, optional specific parsing hints)
CITY_UTILITY_PAGES = [
    # --- Major cities ---
    ("City of Houston", "https://www.publicworks.houstontx.gov/wss-boil-water-notice"),
    ("City of San Antonio (SAWS)", "https://www.saws.org/service-alerts/"),
    ("City of Austin", "https://www.austintexas.gov/page/boil-water-notice"),
    ("City of Dallas", "https://dallascityhall.com/departments/waterutilities/Pages/default.aspx"),
    ("City of Fort Worth", "https://www.fortworthtexas.gov/departments/water/alerts"),
    ("City of El Paso", "https://www.epwater.org/customer-service/service-alerts"),
    ("City of Arlington", "https://www.arlingtontx.gov/city_hall/departments/water_utilities"),
    ("City of Corpus Christi", "https://www.cctexas.com/water"),
    ("City of Plano", "https://www.plano.gov/431/Water-Utilities"),
    ("City of Lubbock", "https://www.mylubbock.us/departmental-websites/departments/water-department"),
    ("City of Laredo", "https://www.ci.laredo.tx.us/utilities/"),
    ("City of Amarillo", "https://www.amarillo.gov/departments/community-services/utilities-department"),
    ("City of Brownsville", "https://www.brownsvillepub.com/"),
    ("City of McAllen", "https://www.mcallen.net/utilities"),
    ("City of Killeen", "https://www.killeentexas.gov/453/Water-Notices"),
    ("City of Midland", "https://www.midlandtexas.gov/167/Reports-and-Notices"),
    ("City of Odessa", "https://odessa-tx.gov/AlertCenter.aspx"),
    ("City of Beaumont", "https://www.beaumonttexas.gov/158/Water-Utilities"),
    ("City of Round Rock", "https://www.roundrocktexas.gov/departments/utilities-and-environmental-services/"),
    ("City of Waco", "https://www.waco-texas.com/water.asp"),
    ("City of Tyler", "https://www.cityoftyler.org/government/departments/utilities"),
    ("City of San Angelo", "https://www.cosatx.us/departments-services/water-utilities"),
    ("City of College Station", "https://www.cstx.gov/departments___city_hall/water"),
    ("City of Abilene", "https://www.abilenetx.gov/water"),
    ("City of Denton", "https://www.cityofdenton.com/en-us/all-departments/administrative-services/water-utilities"),
    # --- Regional utilities ---
    ("TxWaterCo", "https://www.txwaterco.com/service-alerts"),
    ("Brownsville PUB", "https://www.brownsville-pub.com/bpub-outage-center/water-service-issues/"),
    ("ACF Water (Angelina Co FWSD)", "https://www.acfwater.org/public-notices.html"),
    # --- Water Supply Corporations ---
    ("Western Cass WSC", "https://westerncasswsc.com/alerts"),
    ("Bell-Milam-Falls WSC", "https://bellmilamfallswsc.com/alerts"),
    ("Millsap WSC", "https://millsapwatersupplycorp.com/alerts"),
    ("Staff WSC", "https://staffwsc.com/"),
    ("Bold Springs WSC", "https://boldspringswsc.com/"),
    # --- MUDs and districts ---
    ("Fort Bend County MUD 35", "https://www.fbmud35.com/alerts/"),
    ("Brazoria County MUD 22", "https://www.bcmud22.org/contact/district-alerts/"),
    ("Lakeway MUD", "https://lakewaymud.org/about-us/about-your-water/boil-water-notices/"),
    ("Cypress Creek Utility District", "https://www.cycreekud.com/water/"),
    ("West Travis County PUA", "https://www.wtcpua.org/alerts/"),
    ("Crystal Clear SUD", "https://crystalclearsud.org/alerts"),
    ("Tyler County SUD", "https://tylercountywater.com/alerts"),
]


def scrape_city_page(session: requests.Session, entity_name: str, url: str) -> list[dict]:
    """
    Generic scraper for city/utility pages. Fetches the page and searches
    for boil water notice keywords. Returns notices if active BWN found.
    """
    notices = []
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        page_text = soup.get_text(" ", strip=True)

        # Check if page mentions an active boil water notice
        if not is_active_bwn_text(page_text):
            return notices

        # Try to extract the specific notice text from alert banners, divs, etc.
        notice_text = ""

        # Look in common alert/banner patterns
        for selector in [
            {"class_": re.compile(r"alert|notice|warning|banner|emergency", re.I)},
            {"role": "alert"},
            {"id": re.compile(r"alert|notice|warning|banner", re.I)},
        ]:
            for el in soup.find_all(["div", "section", "aside", "p", "span"], **selector):
                el_text = el.get_text(" ", strip=True)
                if is_active_bwn_text(el_text):
                    notice_text = el_text[:500]
                    break
            if notice_text:
                break

        # Fallback: grab paragraphs mentioning boil water
        if not notice_text:
            for p in soup.find_all(["p", "li", "div"]):
                p_text = p.get_text(" ", strip=True)
                if is_active_bwn_text(p_text) and len(p_text) > 20:
                    notice_text = p_text[:500]
                    break

        if not notice_text:
            notice_text = "Active boil water notice detected on page (see source URL)"

        notices.append({
            "entity_name": entity_name,
            "entity_type": classify_entity(entity_name),
            "status": "Active",
            "notice_text": notice_text,
            "date": extract_date_from_text(notice_text),
            "source": "City/Utility Website",
            "source_url": url,
            "entity_url": url,
        })

    except requests.exceptions.RequestException as e:
        log.debug(f"  Failed to fetch {entity_name} ({url}): {e}")

    return notices


def scrape_all_city_pages(session: requests.Session) -> list[dict]:
    """Scrape all city/utility pages for active BWNs."""
    log.info(f"Scraping {len(CITY_UTILITY_PAGES)} city/utility pages...")
    all_notices = []

    for entity_name, url in CITY_UTILITY_PAGES:
        notices = scrape_city_page(session, entity_name, url)
        if notices:
            log.info(f"  ACTIVE BWN: {entity_name}")
        all_notices.extend(notices)
        time.sleep(REQUEST_DELAY)

    log.info(f"  City/utility pages: found {len(all_notices)} active notices")
    return all_notices


# ---------------------------------------------------------------------------
# 5. Bing News Search — catch-all for active BWNs statewide
# ---------------------------------------------------------------------------

def scrape_bing_news(session: requests.Session) -> list[dict]:
    """
    Search Bing News for recent Texas boil water notices.
    Bing is more permissive for programmatic access than Google.
    """
    queries = [
        '"boil water notice" texas',
        '"boil water advisory" texas',
    ]
    log.info("Searching Bing News for active TX boil water notices...")
    notices = []
    seen_titles = set()

    for query in queries:
        url = f"https://www.bing.com/news/search?q={quote_plus(query)}&qft=sortbydate%3d%221%22&form=YFNR"
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Bing News: <div class="news-card"> or <a class="title">
            for card in soup.find_all("div", class_=re.compile(r"news-card|newsitem", re.I)):
                title_el = card.find("a", class_=re.compile(r"title", re.I))
                if not title_el:
                    title_el = card.find("a")
                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)

                lower_title = title.lower()
                if not any(kw in lower_title for kw in ["boil water", "water advisory", "do not use"]):
                    continue
                if any(kw in lower_title for kw in BWN_LIFTED_KEYWORDS):
                    continue

                href = title_el.get("href", "")
                entity_name = _extract_entity_from_headline(title)

                # Date from source attribution
                source_el = card.find("span", class_=re.compile(r"source|time|date", re.I))
                date = source_el.get_text(strip=True) if source_el else ""

                notices.append({
                    "entity_name": entity_name or "Unknown (see headline)",
                    "entity_type": classify_entity(entity_name) if entity_name else "Unknown",
                    "status": "Reported Active (News)",
                    "notice_text": title,
                    "date": date,
                    "source": "Bing News",
                    "source_url": href,
                    "entity_url": "",
                })

            # Also try the simpler list format Bing sometimes uses
            for a_tag in soup.find_all("a", href=True):
                title = a_tag.get_text(strip=True)
                if not title or title in seen_titles or len(title) < 20:
                    continue
                lower_title = title.lower()
                if not any(kw in lower_title for kw in ["boil water notice", "boil water advisory"]):
                    continue
                if any(kw in lower_title for kw in BWN_LIFTED_KEYWORDS):
                    continue
                if "texas" not in lower_title and "tx" not in lower_title:
                    # Check parent context for texas mention
                    parent_text = (a_tag.parent.get_text(" ", strip=True) if a_tag.parent else "").lower()
                    if "texas" not in parent_text and "tx" not in parent_text:
                        continue

                seen_titles.add(title)
                entity_name = _extract_entity_from_headline(title)
                notices.append({
                    "entity_name": entity_name or "Unknown (see headline)",
                    "entity_type": classify_entity(entity_name) if entity_name else "Unknown",
                    "status": "Reported Active (News)",
                    "notice_text": title,
                    "date": "",
                    "source": "Bing News",
                    "source_url": a_tag.get("href", ""),
                    "entity_url": "",
                })

        except requests.exceptions.RequestException as e:
            log.debug(f"  Bing News search failed for '{query}': {e}")

        time.sleep(REQUEST_DELAY)

    # Deduplicate by entity name
    unique = {}
    for n in notices:
        key = n["entity_name"].lower()
        if key not in unique:
            unique[key] = n
    notices = list(unique.values())

    log.info(f"  Bing News: found {len(notices)} recent BWN mentions")
    return notices


def _extract_entity_from_headline(headline: str) -> str:
    """Try to extract an entity name from a news headline."""
    patterns = [
        # "City of X issues/under/lifts..."
        r"(?:City of|Town of|Village of)\s+([A-Z][a-zA-Z\s]+?)(?:\s+(?:issues?|under|lifts?|rescinds?|residents?|customers?|has))",
        # "X MUD 123"
        r"([A-Z][a-zA-Z\s]+?)\s+(?:MUD|M\.U\.D\.)\s*\d*",
        # "X WSC / Water Supply"
        r"([A-Z][a-zA-Z\s]+?)\s+(?:WSC|Water Supply)",
        # "X residents under / advised"
        r"([A-Z][a-zA-Z\s]+?)\s+(?:residents?|customers?|area)\s+(?:under|advised|told|issued)",
        # "boil water notice issued for X" / "boil water notice for X"
        r"(?:boil water (?:notice|advisory)\s+(?:issued\s+)?(?:for|in)\s+)(?:some\s+|parts?\s+of\s+)?([A-Z][a-zA-Z\s]+?)(?:\s*[,;.]|\s+(?:after|due|following|residents?))",
        # "... notice for some X residents"
        r"(?:notice|advisory)\s+(?:issued\s+)?for\s+(?:some\s+)?([A-Z][a-zA-Z\s]+?)\s+residents?",
        # "... notice issued in X"
        r"(?:notice|advisory)\s+issued\s+(?:in|for)\s+(?:parts?\s+of\s+)?([A-Z][a-zA-Z\s]+?)(?:\s+(?:after|due|following|,|$))",
        # "... for customers in X"
        r"for\s+(?:customers|residents|people)\s+in\s+([A-Z][a-zA-Z\s]+?)(?:\s*$|\s*[,;.])",
    ]
    stopwords = {"texas", "the", "all", "some", "many", "parts", "several", "multiple",
                 "in", "for", "after", "due", "following", "certain", "select"}
    for pat in patterns:
        m = re.search(pat, headline, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Remove leading "some" / "parts of"
            name = re.sub(r"^(?:some|parts?\s+of)\s+", "", name, flags=re.IGNORECASE).strip()
            if len(name) > 2 and name.lower() not in stopwords:
                return name
    return ""


# ---------------------------------------------------------------------------
# 6. DuckDuckGo HTML Search — backup web search for active BWN pages
# ---------------------------------------------------------------------------

def scrape_duckduckgo(session: requests.Session) -> list[dict]:
    """
    Search DuckDuckGo HTML version for active Texas boil water notices.
    DDG's HTML-only endpoint is scraper-friendly (no JS required).
    """
    log.info("Searching DuckDuckGo for active TX boil water notices...")
    notices = []
    seen = set()

    queries = [
        '"boil water notice" texas 2026',
        '"boil water advisory" texas 2026',
    ]

    for query in queries:
        url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # DDG HTML results: <div class="result"> with <a class="result__a"> and <a class="result__snippet">
            for result in soup.find_all("div", class_="result"):
                title_el = result.find("a", class_="result__a")
                snippet_el = result.find("a", class_="result__snippet") or result.find("div", class_="result__snippet")

                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                href = title_el.get("href", "")
                snippet = snippet_el.get_text(strip=True) if snippet_el else ""

                combined = f"{title} {snippet}".lower()
                if not any(kw in combined for kw in ["boil water", "water advisory"]):
                    continue
                if any(kw in combined for kw in BWN_LIFTED_KEYWORDS):
                    continue

                if href in seen:
                    continue
                seen.add(href)

                entity_name = _extract_entity_from_headline(title)

                notices.append({
                    "entity_name": entity_name or title[:60],
                    "entity_type": classify_entity(entity_name) if entity_name else "Unknown",
                    "status": "Reported Active (Web)",
                    "notice_text": f"{title}. {snippet[:200]}",
                    "date": extract_date_from_text(snippet),
                    "source": "DuckDuckGo",
                    "source_url": href,
                    "entity_url": href,
                })

        except requests.exceptions.RequestException as e:
            log.debug(f"  DuckDuckGo search failed for '{query}': {e}")

        time.sleep(REQUEST_DELAY)

    log.info(f"  DuckDuckGo: found {len(notices)} results")
    return notices


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

GEOCACHE_PATH = os.path.join(OUTPUT_DIR, "tx_bwn_geocache.json")

# Pre-built lookup of Texas place names -> (lat, lon).
# Covers all major cities, plus common small towns from BWN data.
TX_PLACES: dict[str, tuple[float, float]] = {
    "houston": (29.7604, -95.3698),
    "san antonio": (29.4241, -98.4936),
    "dallas": (32.7767, -96.7970),
    "austin": (30.2672, -97.7431),
    "fort worth": (32.7555, -97.3308),
    "el paso": (31.7619, -106.4850),
    "arlington": (32.7357, -97.1081),
    "corpus christi": (27.8006, -97.3964),
    "plano": (33.0198, -96.6989),
    "lubbock": (33.5779, -101.8552),
    "laredo": (27.5036, -99.5076),
    "amarillo": (35.2220, -101.8313),
    "brownsville": (25.9017, -97.4975),
    "mcallen": (26.2034, -98.2300),
    "killeen": (31.1171, -97.7278),
    "midland": (31.9973, -102.0779),
    "odessa": (31.8457, -102.3676),
    "beaumont": (30.0802, -94.1266),
    "round rock": (30.5083, -97.6789),
    "waco": (31.5493, -97.1467),
    "tyler": (32.3513, -95.3011),
    "san angelo": (31.4638, -100.4370),
    "college station": (30.6280, -96.3344),
    "abilene": (32.4487, -99.7331),
    "denton": (33.2148, -97.1331),
    "marshall": (32.5449, -94.3674),
    "gilmer": (32.7288, -94.9424),
    "sonora": (30.5668, -100.6431),
    "monterey": (32.4487, -101.9),  # Monterey, TX (near Lubbock)
    "deadwood": (33.0784, -94.7366),  # Deadwood, TX (Panola County)
    "palestine": (31.7621, -95.6308),
    "crockett": (31.3182, -95.4566),
    "grapeland": (31.4918, -95.4777),
    "oak grove": (31.4, -95.5),  # Houston County area
    "tadmor": (31.35, -95.25),  # Houston County area
    "pine mountain": (31.45, -95.4),  # Houston County area
    "lakeway": (30.3632, -97.9795),
    "cypress": (29.9691, -95.6972),
    "spring": (30.0799, -95.4172),
    "conroe": (30.3119, -95.4560),
    "the woodlands": (30.1658, -95.4613),
    "sugar land": (29.6197, -95.6349),
    "pearland": (29.5636, -95.2860),
    "league city": (29.5075, -95.0949),
    "pflugerville": (30.4394, -97.6200),
    "temple": (31.0982, -97.3428),
    "bryan": (30.6744, -96.3700),
    "new braunfels": (29.7030, -98.1245),
    "san marcos": (29.8833, -97.9414),
    "georgetown": (30.6333, -97.6778),
    "cedar park": (30.5052, -97.8203),
    "harlingen": (26.1906, -97.6961),
    "mission": (26.2159, -98.3253),
    "edinburg": (26.3017, -98.1633),
    "longview": (32.5007, -94.7405),
    "texarkana": (33.4418, -94.0477),
    "nacogdoches": (31.6035, -94.6555),
    "lufkin": (31.3382, -94.7291),
    "victoria": (28.8053, -96.9850),
    "wichita falls": (33.9137, -98.4934),
    "sherman": (33.6357, -96.6089),
    "del rio": (29.3627, -100.8968),
    "eagle pass": (28.7091, -100.4995),
    "uvalde": (29.2097, -99.7862),
    "fredericksburg": (30.2752, -98.8720),
    "kerrville": (30.0474, -99.1401),
    "boerne": (29.7947, -98.7320),
    "seguin": (29.5688, -97.9647),
    "bastrop": (30.1105, -97.3153),
    "lockhart": (29.8849, -97.6700),
    "gonzales": (29.5017, -97.4525),
    "cuero": (29.0938, -97.2892),
    "port arthur": (29.8850, -93.9400),
    "galveston": (29.3013, -94.7977),
    "bay city": (28.9828, -95.9694),
    "angleton": (29.1694, -95.4316),
    "freeport": (28.9541, -95.3597),
    "rockport": (28.0206, -97.0544),
    "port lavaca": (28.6150, -96.6261),
    "alpine": (30.3585, -103.6610),
    "pecos": (31.4229, -103.4932),
    "monahans": (31.5943, -102.8924),
    "big spring": (32.2504, -101.4785),
    "snyder": (32.7179, -100.9176),
    "sweetwater": (32.4710, -100.4059),
    "breckenridge": (32.7557, -98.9023),
    "mineral wells": (32.8084, -98.1128),
    "stephenville": (32.2207, -98.2023),
    "granbury": (32.4419, -97.7942),
    "cleburne": (32.3476, -97.3867),
    "corsicana": (32.0954, -96.4689),
    "athens": (32.2049, -95.8552),
    "henderson": (32.1532, -94.7994),
    "jacksonville": (31.9638, -95.2705),
    "rusk": (31.7960, -95.1522),
    "carthage": (32.1574, -94.3374),
    "center": (31.7935, -94.1791),
    "jasper": (30.9202, -93.9966),
    "woodville": (30.7752, -94.4155),
    "livingston": (30.7111, -94.9330),
    "huntsville": (30.7235, -95.5508),
    "madisonville": (30.9499, -95.9114),
    "navasota": (30.3880, -96.0878),
    "brenham": (30.1669, -96.3978),
    "la grange": (29.9058, -96.8767),
    "columbus": (29.7063, -96.5397),
    "hallettsville": (29.4441, -96.9411),
    "yoakum": (29.2886, -97.1514),
    "eagle lake": (29.5894, -96.3331),
    "el campo": (29.1966, -96.2697),
    "wharton": (29.3116, -96.1028),
    "rosenberg": (29.5572, -95.8086),
    "richmond": (29.5822, -95.7608),
    "katy": (29.7858, -95.8244),
    "tomball": (30.0972, -95.6161),
    "humble": (29.9988, -95.2622),
    "baytown": (29.7355, -94.9774),
    "pasadena": (29.6911, -95.2091),
    "deer park": (29.7055, -95.1286),
    "la porte": (29.6658, -95.0194),
    "webster": (29.5377, -95.1183),
    "friendswood": (29.5294, -95.2010),
    "alvin": (29.4239, -95.2441),
    "lake jackson": (29.0439, -95.4344),
    "clute": (29.0247, -95.3986),
    "west columbia": (29.1441, -95.6453),
    "bellville": (29.9502, -96.2567),
    "sealy": (29.7811, -96.1572),
    "hempstead": (30.0972, -96.0789),
    "magnolia": (30.2094, -95.7508),
    "willis": (30.4250, -95.4789),
    "huntsville tx": (30.7235, -95.5508),
    "montgomery": (30.3883, -95.6936),
    "anderson": (30.4863, -95.9878),
    "centerville": (31.2585, -95.9786),
    "buffalo": (31.4635, -96.0580),
    "mexia": (31.6821, -96.4822),
    "groesbeck": (31.5243, -96.5339),
    "marlin": (31.3063, -96.8931),
    "cameron": (30.8533, -96.9769),
    "rockdale": (30.6555, -97.0017),
    "taylor": (30.5728, -97.4092),
    "elgin": (30.3497, -97.3706),
    "smithville": (30.0086, -97.1592),
    "giddings": (30.1825, -96.9364),
    "manor": (30.3408, -97.5567),
    "hutto": (30.5427, -97.5467),
    "leander": (30.5788, -97.8531),
    "dripping springs": (30.1902, -98.0867),
    "wimberley": (29.9974, -98.0989),
    "kyle": (29.9889, -97.8772),
    "buda": (30.0852, -97.8403),
    "flying l ranch": (29.8, -98.75),  # Bandera County
    "bandera": (29.7266, -99.0734),
    "kendall": (29.95, -98.7),
    "hays": (30.05, -98.0),
    "sequoia": (29.9, -95.5),
}


def _load_geocache() -> dict[str, list[float]]:
    """Load cached geocoding results."""
    if os.path.exists(GEOCACHE_PATH):
        try:
            with open(GEOCACHE_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_geocache(cache: dict[str, list[float]]):
    """Save geocoding results to cache."""
    with open(GEOCACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def _extract_place_name(entity_name: str) -> str:
    """Extract a likely place name from an entity name for geocoding."""
    name = entity_name
    # Strip common prefixes/suffixes
    for prefix in ["Consolidated WSC - ", "City of ", "Town of ", "Village of "]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    # Strip notice numbers like "Boil Water Notice 1130033 "
    name = re.sub(r"Boil Water (?:Notice|Advisory)\s*\d*\s*[-–]?\s*", "", name, flags=re.IGNORECASE)
    # Strip "Area", "WSC", "MUD", "SUD", district suffixes
    name = re.sub(r"\s+(?:Area|WSC|MUD|SUD|WCID|FWSD|PUD|PUA|Water Supply.*|Municipal Utility.*|Special Utility.*)$",
                  "", name, flags=re.IGNORECASE)
    # Strip trailing numbers like "MUD 35"
    name = re.sub(r"\s+\d+$", "", name)
    return name.strip()


def geocode_nominatim(session: requests.Session, place: str) -> tuple[float, float] | None:
    """Geocode a place name using OpenStreetMap Nominatim (free, 1 req/sec)."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{place}, Texas, USA",
        "format": "json",
        "limit": 1,
        "countrycodes": "us",
    }
    try:
        resp = session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json()
        if results:
            return (float(results[0]["lat"]), float(results[0]["lon"]))
    except (requests.exceptions.RequestException, KeyError, IndexError, ValueError) as e:
        log.debug(f"  Nominatim geocoding failed for '{place}': {e}")
    return None


def geocode_notices(session: requests.Session, notices: list[dict]) -> list[dict]:
    """
    Add lat/lon coordinates to each notice.
    Uses local lookup table first, then Nominatim API as fallback.
    Results are cached in tx_bwn_geocache.json.
    """
    log.info("Geocoding notices...")
    cache = _load_geocache()
    geocoded_count = 0
    failed = []

    for notice in notices:
        entity = notice["entity_name"]
        place = _extract_place_name(entity)
        place_lower = place.lower().strip()

        # 1. Check local lookup
        if place_lower in TX_PLACES:
            lat, lon = TX_PLACES[place_lower]
            notice["lat"] = lat
            notice["lon"] = lon
            geocoded_count += 1
            continue

        # 2. Check cache
        if place_lower in cache:
            notice["lat"] = cache[place_lower][0]
            notice["lon"] = cache[place_lower][1]
            geocoded_count += 1
            continue

        # 3. Try Nominatim
        coords = geocode_nominatim(session, place)
        if coords:
            lat, lon = coords
            # Sanity check: should be roughly within Texas bounds
            if 25.5 < lat < 36.5 and -106.7 < lon < -93.5:
                notice["lat"] = lat
                notice["lon"] = lon
                cache[place_lower] = [lat, lon]
                geocoded_count += 1
                log.info(f"  Geocoded '{place}' -> ({lat:.4f}, {lon:.4f})")
            else:
                log.warning(f"  Geocoded '{place}' outside Texas bounds: ({lat}, {lon})")
                # Default to center of Texas
                notice["lat"] = 31.0
                notice["lon"] = -99.0
                failed.append(place)
        else:
            # Default to center of Texas
            notice["lat"] = 31.0
            notice["lon"] = -99.0
            failed.append(place)

        time.sleep(1.1)  # Nominatim rate limit: 1 req/sec

    _save_geocache(cache)

    if failed:
        log.warning(f"  Could not geocode: {', '.join(failed)}")
    log.info(f"  Geocoded {geocoded_count}/{len(notices)} notices")
    return notices


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(notices: list[dict], filepath: str):
    if not notices:
        log.warning("No notices to write to CSV.")
        return
    fields = [
        "entity_name", "entity_type", "status", "date",
        "notice_text", "source", "source_url", "entity_url",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(notices)
    log.info(f"CSV written: {filepath} ({len(notices)} records)")


def write_json(notices: list[dict], filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(notices, f, indent=2, default=str)
    log.info(f"JSON written: {filepath} ({len(notices)} records)")


def print_summary(notices: list[dict]):
    print("\n" + "=" * 72)
    print("  TEXAS ACTIVE BOIL WATER NOTICES")
    print(f"  Scraped: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    if not notices:
        print("\n  No active boil water notices found.")
        print("=" * 72)
        return

    print(f"\n  Total active notices found: {len(notices)}")

    # Group by source
    by_source: dict[str, list[dict]] = {}
    for n in notices:
        by_source.setdefault(n["source"], []).append(n)

    for source, items in sorted(by_source.items()):
        print(f"\n  --- {source} ({len(items)} notices) ---")
        for n in items:
            status_tag = f"[{n['status']}]"
            date_str = f" ({n['date']})" if n.get("date") else ""
            print(f"    {status_tag:<20} {n['entity_name'][:45]:<45}{date_str}")
            if n.get("notice_text"):
                # Truncate notice text for display
                short = n["notice_text"][:100].replace("\n", " ")
                print(f"    {'':20} {short}...")

    # Summary by entity type
    type_counts: dict[str, int] = {}
    for n in notices:
        t = n.get("entity_type", "Unknown")
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"\n  By entity type:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t:<45} {c:>3}")

    print("\n" + "=" * 72)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("Starting Texas Active Boil Water Notice Scraper...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session = make_session()

    all_notices = []

    # 1. Aggregator: MunicipalOps
    all_notices.extend(scrape_municipalops(session))
    time.sleep(REQUEST_DELAY)

    # 2. Aggregator: SWWC Dashboard
    all_notices.extend(scrape_swwc_dashboard(session))
    time.sleep(REQUEST_DELAY)

    # 3. Consolidated WSC
    all_notices.extend(scrape_consolidated_wsc(session))
    time.sleep(REQUEST_DELAY)

    # 4. Major city/utility pages
    all_notices.extend(scrape_all_city_pages(session))

    # 5. Bing News
    all_notices.extend(scrape_bing_news(session))
    time.sleep(REQUEST_DELAY)

    # 6. DuckDuckGo web search
    all_notices.extend(scrape_duckduckgo(session))

    # Deduplicate by entity_name + source
    seen = set()
    deduped = []
    for n in all_notices:
        key = (n["entity_name"].lower().strip(), n["source"])
        if key not in seen:
            seen.add(key)
            deduped.append(n)
    all_notices = deduped

    # Geocode
    all_notices = geocode_notices(session, all_notices)

    # Add metadata
    run_metadata = {
        "last_updated": datetime.now().isoformat(),
        "total_notices": len(all_notices),
        "notices": all_notices,
    }

    # Output — timestamped
    csv_path = os.path.join(OUTPUT_DIR, f"tx_active_bwn_{timestamp}.csv")
    json_path = os.path.join(OUTPUT_DIR, f"tx_active_bwn_{timestamp}.json")
    write_csv(all_notices, csv_path)
    write_json(run_metadata, json_path)

    # Output — stable latest file for the map page
    latest_path = os.path.join(OUTPUT_DIR, "tx_active_bwn_latest.json")
    write_json(run_metadata, latest_path)

    print_summary(all_notices)

    log.info("Done.")
    return all_notices


if __name__ == "__main__":
    main()
