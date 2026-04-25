from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional dependency fallback
    BeautifulSoup = None

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - optional dependency fallback
    PdfReader = None


RESEARCH_REPORTS_DIR = Path("research_reports")

PUBLIC_RESEARCH_SOURCES = [
    {
        "institution": "Goldman Sachs",
        "url": "https://www.goldmansachs.com/insights/articles/why-ai-companies-may-invest-more-than-500-billion-in-2026",
    },
    {
        "institution": "Goldman Sachs",
        "url": "https://www.goldmansachs.com/insights/articles/is-there-enough-data-center-capacity-for-ai",
    },
    {
        "institution": "Goldman Sachs",
        "url": "https://www.goldmansachs.com/insights/articles/rising-power-density-disrupts-ai-infrastructure",
    },
    {
        "institution": "Morgan Stanley",
        "url": "https://www.morganstanley.com/insights/articles/powering-ai-energy-market-outlook-2026",
    },
    {
        "institution": "Morgan Stanley",
        "url": "https://www.morganstanley.com/insights/articles/ai-market-trends-institute-2026",
    },
    {
        "institution": "J.P. Morgan",
        "url": "https://www.jpmorgan.com/insights/global-research/artificial-intelligence/generative-ai",
    },
    {
        "institution": "J.P. Morgan",
        "url": "https://www.jpmorgan.com/insights/global-research/artificial-intelligence/ai-impact-job-growth",
    },
]


def fetch_public_research(timeout: int = 12, max_chars: int = 30000) -> pd.DataFrame:
    rows = []
    for source in PUBLIC_RESEARCH_SOURCES:
        try:
            response = requests.get(
                source["url"],
                timeout=timeout,
                headers={"User-Agent": "ai-supply-chain-scanner/0.1"},
            )
            response.raise_for_status()
        except Exception:
            continue

        title, text = _extract_html_text(response.text)
        text = _clean_text(text)[:max_chars]
        if not text:
            continue

        rows.append(
            {
                "institution": source["institution"],
                "title": title or source["url"].rstrip("/").split("/")[-1].replace("-", " ").title(),
                "url": source["url"],
                "published": _extract_date(text),
                "text": text,
                "source_type": "public_web",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def load_local_research_reports(folder: Path | str = RESEARCH_REPORTS_DIR, max_chars: int = 60000) -> pd.DataFrame:
    folder = Path(folder)
    folder.mkdir(exist_ok=True)

    rows = []
    for path in sorted(folder.glob("*")):
        if not path.is_file() or path.suffix.lower() not in {".txt", ".md", ".html", ".htm", ".pdf"}:
            continue
        text = _read_report_file(path)
        text = _clean_text(text)[:max_chars]
        if not text:
            continue

        rows.append(
            {
                "institution": _infer_institution(path.name, text),
                "title": path.stem.replace("_", " ").replace("-", " ").title(),
                "url": str(path),
                "published": _extract_date(text),
                "text": text,
                "source_type": "local_file",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def collect_research_inputs(include_public: bool = True, include_local: bool = True) -> pd.DataFrame:
    frames = []
    if include_public:
        frames.append(fetch_public_research())
    if include_local:
        frames.append(load_local_research_reports())
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(
            columns=["institution", "title", "url", "published", "text", "source_type", "fetched_at"]
        )
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["institution", "title", "url"])


def _extract_html_text(html: str) -> tuple[str, str]:
    if BeautifulSoup is None:
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        title = _clean_text(title_match.group(1)) if title_match else ""
        text = re.sub(r"<[^>]+>", " ", html)
        return title, text

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer"]):
        tag.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    main = soup.find("main") or soup.body or soup
    return title, main.get_text(" ", strip=True)


def _read_report_file(path: Path) -> str:
    if path.suffix.lower() == ".pdf":
        if PdfReader is None:
            return ""
        try:
            reader = PdfReader(str(path))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _extract_date(text: str) -> str:
    match = re.search(
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+\d{4}\b",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(0) if match else ""


def _infer_institution(filename: str, text: str) -> str:
    haystack = f"{filename} {text[:2000]}".lower()
    institution_terms = {
        "J.P. Morgan": ["jpmorgan", "j.p. morgan", "jp morgan"],
        "Goldman Sachs": ["goldman", "goldman sachs"],
        "Morgan Stanley": ["morgan stanley"],
        "Bank of America": ["bank of america", "bofa"],
        "Citi": ["citigroup", "citi"],
        "UBS": ["ubs"],
        "Barclays": ["barclays"],
        "Deutsche Bank": ["deutsche bank"],
    }
    for institution, terms in institution_terms.items():
        if any(term in haystack for term in terms):
            return institution
    return "Local Research"
