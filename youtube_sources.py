from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import feedparser
import pandas as pd
import requests

try:
    import yt_dlp
except ImportError:  # pragma: no cover - optional dependency fallback
    yt_dlp = None


YOUTUBE_CHANNELS_PATH = Path("youtube_channels.csv")
YOUTUBE_TRANSCRIPTS_DIR = Path("youtube_transcripts")
DEFAULT_MAX_VIDEOS_PER_CHANNEL = 3


def ensure_youtube_files() -> None:
    YOUTUBE_TRANSCRIPTS_DIR.mkdir(exist_ok=True)
    if not YOUTUBE_CHANNELS_PATH.exists():
        with YOUTUBE_CHANNELS_PATH.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["channel_name", "channel_id"])
            writer.writeheader()
            writer.writerow({"channel_name": "Example Channel", "channel_id": "UCxxxxxxxxxxxxxxxxxxxxxx"})


def load_youtube_channels(path: Path | str = YOUTUBE_CHANNELS_PATH) -> pd.DataFrame:
    ensure_youtube_files()
    channels = pd.read_csv(path).fillna("")
    channels = channels[channels["channel_id"].str.startswith("UC")]
    return channels


def fetch_channel_videos(
    channels: pd.DataFrame | None = None,
    max_videos_per_channel: int = DEFAULT_MAX_VIDEOS_PER_CHANNEL,
) -> pd.DataFrame:
    channels = load_youtube_channels() if channels is None else channels
    rows = []
    for _, channel in channels.iterrows():
        channel_id = channel["channel_id"]
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries[:max_videos_per_channel]:
            video_url = entry.get("link", "")
            video_id = entry.get("yt_videoid", "") or _video_id_from_url(video_url)
            rows.append(
                {
                    "channel_name": channel.get("channel_name", ""),
                    "channel_id": channel_id,
                    "video_id": video_id,
                    "title": entry.get("title", ""),
                    "url": video_url,
                    "published": entry.get("published", ""),
                    "source_type": "youtube",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return pd.DataFrame(rows)


def collect_youtube_transcripts(
    channels: pd.DataFrame | None = None,
    max_videos_per_channel: int = DEFAULT_MAX_VIDEOS_PER_CHANNEL,
    preferred_languages: Iterable[str] = ("en", "zh", "zh-Hans", "zh-Hant"),
) -> pd.DataFrame:
    videos = fetch_channel_videos(channels=channels, max_videos_per_channel=max_videos_per_channel)
    local_transcripts = load_local_youtube_transcripts()
    rows = []

    for _, video in videos.iterrows():
        transcript_text = _find_local_transcript(video, local_transcripts)
        transcript_source = "local_transcript" if transcript_text else ""
        if not transcript_text:
            transcript_text = fetch_video_transcript(video["url"], preferred_languages=preferred_languages)
            transcript_source = "yt_dlp_caption" if transcript_text else "missing_transcript"

        if not transcript_text:
            continue

        rows.append(
            {
                "institution": video.get("channel_name", "YouTube"),
                "title": video.get("title", ""),
                "url": video.get("url", ""),
                "published": video.get("published", ""),
                "text": transcript_text,
                "source_type": transcript_source,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "video_id": video.get("video_id", ""),
                "channel_name": video.get("channel_name", ""),
            }
        )

    return pd.DataFrame(rows)


def fetch_video_transcript(
    video_url: str,
    preferred_languages: Iterable[str] = ("en", "zh", "zh-Hans", "zh-Hant"),
) -> str:
    if yt_dlp is None:
        return ""

    try:
        with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception:
        return ""

    caption_url = _select_caption_url(info.get("subtitles", {}), preferred_languages)
    if not caption_url:
        caption_url = _select_caption_url(info.get("automatic_captions", {}), preferred_languages)
    if not caption_url:
        return ""

    try:
        response = requests.get(caption_url, timeout=20, headers={"User-Agent": "ai-supply-chain-scanner/0.1"})
        response.raise_for_status()
    except Exception:
        return ""
    return _clean_caption_text(response.text)


def load_local_youtube_transcripts(folder: Path | str = YOUTUBE_TRANSCRIPTS_DIR) -> pd.DataFrame:
    ensure_youtube_files()
    folder = Path(folder)
    rows = []
    for path in sorted(folder.glob("*")):
        if not path.is_file() or path.suffix.lower() not in {".txt", ".md", ".vtt", ".srt"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        rows.append(
            {
                "video_id": _video_id_from_text(path.name),
                "title": path.stem.replace("_", " ").replace("-", " ").title(),
                "text": _clean_caption_text(text),
                "path": str(path),
            }
        )
    return pd.DataFrame(rows)


def _select_caption_url(captions: dict, preferred_languages: Iterable[str]) -> str:
    for language in preferred_languages:
        for caption_language, tracks in captions.items():
            if caption_language.lower().startswith(language.lower()):
                for track in tracks:
                    url = track.get("url", "")
                    if url and track.get("ext") in {"vtt", "srv3", "ttml", "json3"}:
                        return url
                if tracks and tracks[0].get("url"):
                    return tracks[0]["url"]
    return ""


def _find_local_transcript(video: pd.Series, local_transcripts: pd.DataFrame) -> str:
    if local_transcripts.empty:
        return ""
    video_id = str(video.get("video_id", ""))
    if video_id:
        matches = local_transcripts[local_transcripts["video_id"] == video_id]
        if not matches.empty:
            return str(matches.iloc[0]["text"])
    return ""


def _clean_caption_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"WEBVTT|Kind:.*|Language:.*", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\d{1,2}:\d{2}:\d{2}[.,]\d{3}\s+-->\s+\d{1,2}:\d{2}:\d{2}[.,]\d{3}.*", " ", text)
    text = re.sub(r"^\d+$", " ", text, flags=re.MULTILINE)
    text = re.sub(r"\{.*?\}", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _video_id_from_url(url: str) -> str:
    match = re.search(r"(?:v=|youtu\.be/|embed/)([A-Za-z0-9_-]{11})", str(url))
    return match.group(1) if match else ""


def _video_id_from_text(text: str) -> str:
    match = re.search(r"([A-Za-z0-9_-]{11})", str(text))
    return match.group(1) if match else ""
