#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Set

import httpx
from dotenv import load_dotenv
from guessit import guessit

# ---------------------------------------------------------------------------
# Config / env
# ---------------------------------------------------------------------------

load_dotenv()

API_KEY: str = os.getenv("DBD_API_KEY") or ""
API_URL: str = os.getenv("DBD_API_URL") or ""
SDBX_ID: str = os.getenv("DBD_SDBX_ID") or ""

# Rescan interval in minutes. 0 or negative → run once and exit.
RESCAN_INTERVAL_MIN: int = int(os.getenv("DBD_SCAN_INTERVAL_MIN", "60"))

DEFAULT_MIN_SIZE = 100 * 1024 * 1024  # 100 MiB

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".wmv", ".flv", ".mpg", ".mpeg", ".ts"}
AUDIO_EXTS = {".mp3", ".flac", ".aac", ".m4a", ".ogg", ".wav"}

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DbdFile:
    """Represents a file on Debrid-Link."""
    id: str
    name: str
    url: str
    media_type: str  # "video" | "audio"
    size: int
    path: str = ""


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DbdAPIError(Exception):
    """Generic Debrid-Link API error."""


class RateLimitError(DbdAPIError):
    """Rate limit (floodDetected)."""

    def __init__(self, reset_after: int) -> None:
        self.reset_after = reset_after
        super().__init__(f"Rate limit reached, retry in ~{reset_after // 60} minutes")


class SkipFile(Exception):
    """Raised when a file should be skipped during mapping."""


# ---------------------------------------------------------------------------
# Debrid-Link API client (async)
# ---------------------------------------------------------------------------

class DbdAPI:
    """Async client for Debrid-Link API."""

    def __init__(self, api_key: str, base_url: str, client: httpx.AsyncClient) -> None:
        if not api_key:
            raise ValueError("Missing API key")
        if not base_url:
            raise ValueError("Missing API base URL")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.client = client

        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    async def get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 10.0,
        retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        Perform a GET request on the API.

        Returns JSON dict or None on non-blocking error.
        Raises RateLimitError on floodDetected / 429.
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(1, retries + 1):
            try:
                resp = await self.client.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=timeout,
                )
                resp.raise_for_status()
                return resp.json()

            except httpx.HTTPStatusError as exc:
                data: Dict[str, Any] = {}
                try:
                    data = exc.response.json()
                except ValueError:
                    pass

                err = data.get("error")
                desc = data.get("error_description", "") or ""

                # floodDetected / rate limit
                if err == "floodDetected" or exc.response.status_code == 429:
                    logger.error(f"⚠️ floodDetected: {desc or 'API rate limit reached'}")
                    # example uses 1 hour
                    raise RateLimitError(3600)

                # 503 → retry
                if exc.response.status_code == 503 and attempt < retries:
                    wait = 2 * attempt
                    logger.warning(
                        f"⏳ HTTP 503 on {url} – attempt {attempt}/{retries}, retry in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue

                logger.error(
                    f"⚠️ HTTP {exc.response.status_code} on {url}: {desc or str(exc)}"
                )
                return None

            except httpx.RequestError as exc:
                logger.error(f"⚠️ Network error to {url}: {exc}")
                return None

        return None

    async def list_files(
        self,
        folder_id: str,
        cur_path: str = "",
        *,
        per_page: int = 100,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[DbdFile]:
        """
        Recursively list all files in a folder.

        Uses pagination.next when available, otherwise per_page.
        """
        page = 0

        while True:
            params: Dict[str, Any] = {"page": page, "perPage": per_page}
            if extra_params:
                params.update(extra_params)

            logger.debug(f"DEBUG: request page={page} folder_id={folder_id} params={params}")

            data = await self.get(f"/files/{folder_id}/list", params=params)
            if not data or not data.get("success"):
                logger.debug(
                    f"DEBUG: no data or success=False (page={page}, folder={folder_id})"
                )
                break

            items = data.get("value", []) or []
            logger.debug(f"DEBUG: received {len(items)} items (page {page})")

            if not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue

                item_type = item.get("type")
                name = item.get("name", "")
                id_ = item.get("id", "")
                size = int(item.get("size", 0))
                download_url = item.get("downloadUrl", "")
                full_path = os.path.join(cur_path, name) if cur_path else name

                if item_type == "file":
                    ext = Path(name).suffix.lower()
                    media_type = (
                        "video"
                        if ext in VIDEO_EXTS
                        else "audio"
                        if ext in AUDIO_EXTS
                        else "unknown"
                    )
                    if media_type != "unknown":
                        yield DbdFile(
                            id=id_,
                            name=name,
                            url=download_url,
                            media_type=media_type,
                            size=size,
                            path=full_path,
                        )

                elif item_type == "folder":
                    # recurse
                    async for subfile in self.list_files(
                        id_,
                        full_path,
                        per_page=per_page,
                        extra_params=extra_params,
                    ):
                        yield subfile

                elif item_type == "movie":
                    # Some APIs expose movies directly
                    yield DbdFile(
                        id=id_,
                        name=name,
                        url=download_url,
                        media_type="video",
                        size=size,
                        path=full_path,
                    )

            # Pagination handling
            pagination = data.get("pagination") or {}
            next_page = pagination.get("next", -1)

            logger.debug(
                f"DEBUG: pagination raw={pagination} page={page} next={next_page}"
            )

            if isinstance(next_page, int):
                if next_page == -1:
                    break
                page = next_page
            else:
                # fallback: last page if fewer than per_page
                if len(items) < per_page:
                    break
                page += 1

            await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# Helpers adapted from rename_and_symlink.py
# ---------------------------------------------------------------------------

def clean_filename(filename: str) -> str:
    """
    Remove common release tags like 'www.something - ' from the beginning.
    """
    cleaned = re.sub(
        r'^(?:\[\s*www\.[\w.-]+\s*\]|\(\s*www\.[\w.-]+\s*\)|www\.[\w.-]+)\s*-\s*',
        '',
        filename,
        flags=re.IGNORECASE,
    )
    return cleaned.strip()


def create_nfo_file(
    target_folder: Path,
    title: str,
    year: int | str | None = None,
    season: int | None = None,
    episode_list: Optional[List[int]] = None,
    release_date: Optional[str] = None,
) -> None:
    """
    Create a simple NFO file for Jellyfin.
    If season+episode_list are provided → episodes; otherwise movie.
    """
    os.makedirs(target_folder, exist_ok=True)

    if season is not None and episode_list:
        # TV episodes
        for episode in episode_list:
            episode_str = f"E{episode:02}"
            nfo_path = target_folder / f"{title} - S{season:02}{episode_str}.nfo"

            # minimal content, adjust if needed
            nfo_content = "\n"
            nfo_content += "\n"
            nfo_content += f" {title}\n"
            nfo_content += f" {season}\n"
            nfo_content += f" {episode}\n"
            if release_date or year:
                date_to_use = release_date or f"{year}-01-01"
                nfo_content += f" {date_to_use}\n"
            nfo_content += "\n"

            try:
                with open(nfo_path, "w", encoding="utf-8") as f:
                    f.write(nfo_content)
                logger.info(f"[NFO] Created episode NFO: {nfo_path}")
            except Exception as e:
                logger.error(f"[ERROR] Could not create NFO file: {e}")
    else:
        # Movie
        year_str = str(year) if year else ""
        nfo_path = target_folder / f"{title} ({year_str}).nfo"

        nfo_content = "\n"
        nfo_content += "\n"
        nfo_content += f" {title}\n"
        if year:
            nfo_content += f" {year}\n"
        if release_date or year:
            date_to_use = release_date or f"{year}-01-01"
            nfo_content += f" {date_to_use}\n"
            nfo_content += f" {date_to_use}\n"
        nfo_content += "\n"

        try:
            with open(nfo_path, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            logger.info(f"[NFO] Created movie NFO: {nfo_path}")
        except Exception as e:
            logger.error(f"[ERROR] Could not create NFO file: {e}")


def extract_release_date(info: dict) -> str:
    """
    Try to extract release date from guessit info or use current date.
    """
    date_val = info.get("date")
    if date_val:
        return str(date_val)
    return datetime.now().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# .strm management
# ---------------------------------------------------------------------------

class StrmManager:
    """Create/update/delete .strm files and normalize paths."""

    @staticmethod
    def normalize_path(path: str | Path) -> str:
        s = str(path).lower().replace("\\", "/").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    @staticmethod
    def save_strm(
        path: Path,
        url: str,
        dry_run: bool = False,
        stats: Optional[Dict[str, int]] = None,
    ) -> bool:
        def bump(key: str) -> None:
            if stats is not None:
                stats[key] = stats.get(key, 0) + 1

        url_text = url.strip()
        try:
            existing = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            existing = None
        except OSError as exc:
            logger.warning(f"⚠️ Cannot read {path}: {exc}")
            existing = None

        if existing is not None and existing.strip() == url_text:
            bump("skipped")
            logger.debug(f"✅ Up to date: {path}")
            return True

        if dry_run:
            bump("dry_run")
            logger.info(f"📝 (dry-run) {path}")
            return True

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(url_text, encoding="utf-8")
            bump("written")
            logger.debug(f"📝 {path}")
            return True
        except OSError as exc:
            bump("errors")
            logger.error(f"❌ Cannot write {path}: {exc}")
            return False

    @staticmethod
    def delete_strm(path: Path, dry_run: bool = False) -> bool:
        if dry_run:
            logger.info(f"🗑️ (dry-run) {path}")
            return True
    
        try:
            path.unlink()
            logger.info(f"🗑️ Deleted {path}")
            parent = path.parent
    
            try:
                # Look at what's left in the movie/episode folder
                items = list(parent.iterdir())
    
                # If only NFO files remain, delete them as orphaned metadata
                only_nfo_left = (
                    len(items) > 0
                    and all(i.is_file() and i.suffix.lower() == ".nfo" for i in items)
                )
                if only_nfo_left:
                    for i in items:
                        logger.info(f"🗑️ Deleting orphaned NFO: {i}")
                        i.unlink()
                    items = []
    
                # If folder is now empty, remove it
                if not items:
                    parent.rmdir()
                    logger.debug(f"📂 Removed empty folder: {parent}")
    
            except OSError:
                # ignore if we can't inspect or remove
                pass
    
            return True
        except OSError as exc:
            logger.error(f"❌ Cannot delete {path}: {exc}")
            return False


# ---------------------------------------------------------------------------
# Mapping Debrid files → Jellyfin paths (logic from rename_and_symlink)
# ---------------------------------------------------------------------------

def map_dbd_file_to_target(file: DbdFile, dest_base: Path) -> tuple[Path, str, Dict[str, Any]]:
    """
    Convert a Debrid file into (target_folder, target_basename, meta).
    target_basename is without extension; .strm will be added.
    """
    # Use logical path inside Debrid-Link
    filename = Path(file.path).name or file.name
    cleaned_name = clean_filename(filename)
    info = guessit(cleaned_name)

    release_date = extract_release_date(info)

    if info.get("type") == "movie":
        title = info.get("title")
        year = info.get("year", "")
        if not title:
            raise SkipFile(f"No title for movie: {filename}")

        target_folder = dest_base / "movies" / f"{title} ({year})"
        target_name = f"{title} ({year})"
        meta = {
            "type": "movie",
            "title": title,
            "year": year,
            "season": None,
            "episode_list": None,
            "release_date": release_date,
        }
        return target_folder, target_name, meta

    if info.get("type") == "episode":
        # Derive show title:
        # if path has a parent folder, use its name (cleaned), otherwise guessit title
        parent_name = Path(file.path).parent.name
        if parent_name:
            title = clean_filename(parent_name)
            # remove common patterns like "S01 - EP(01-09)" and "(2023)"
            title = re.sub(r"\s*S\d{2}\s*-\s*EP\([^)]+\).*$", "", title, flags=re.IGNORECASE)
            title = re.sub(r"\s*\(\d{4}\).*$", "", title)
            title = title.strip()
        else:
            title = info.get("title")

        if not title:
            raise SkipFile(f"No show title for episode: {filename}")

        season = info.get("season")
        episode = info.get("episode")

        # Normalize season/episode
        if isinstance(season, List) and season:
            season = season[0]

        if isinstance(episode, List) and episode:
            episode_list = episode
        elif isinstance(episode, int):
            episode_list = [episode]
        else:
            episode_list = []

        if not (season and episode_list):
            raise SkipFile(f"Incomplete episode info: {filename}")

        episode_str = "-".join(f"E{e:02}" for e in episode_list)
        target_folder = dest_base / "tvshows" / title / f"Season {season:02}"
        target_name = f"{title} - S{season:02}{episode_str}"
        meta = {
            "type": "episode",
            "title": title,
            "year": info.get("year"),
            "season": season,
            "episode_list": episode_list,
            "release_date": release_date,
        }
        return target_folder, target_name, meta

    # Other media types can be skipped
    raise SkipFile(f"Unsupported media type for: {filename}")


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

class DbdStrmGenerator:
    def __init__(
        self,
        api: DbdAPI,
        base_dir: Path,
        sdbx_id: str,
        min_size: int = DEFAULT_MIN_SIZE,
        dry_run: bool = False,
    ) -> None:
        self.api = api
        self.base_dir = base_dir
        self.sdbx_id = sdbx_id
        self.min_size = min_size
        self.dry_run = dry_run
        self.strm_manager = StrmManager()

    async def verify_connection(self) -> bool:
        """Simple connection test."""
        data = await self.api.get(
            f"/files/{self.sdbx_id}/list", params={"page": 0, "perPage": 1}
        )
        if not data or not data.get("success"):
            logger.error("❌ Cannot fetch seedbox list (connection test)")
            return False
        return True

    async def fetch_files(
        self,
        per_page: int = 100,
        oldest_first: bool = False,
    ) -> List[DbdFile]:
        extra_params: Dict[str, Any] = {}
        if oldest_first:
            extra_params["order"] = "asc"
            logger.debug("DEBUG: sorting by oldest (order=asc)")

        logger.info("🔗 Querying Debrid-Link…")

        files: List[DbdFile] = []
        async for f in self.api.list_files(
            self.sdbx_id,
            per_page=per_page,
            extra_params=extra_params,
        ):
            if f.size >= self.min_size:
                files.append(f)

        logger.info(f"📊 {len(files)} eligible files (≥ {self.min_size} bytes)")
        return files

    async def generate_strm_files(self, files: List[DbdFile]) -> Set[str]:
        normalized_created: Set[str] = set()
        normalized_to_actual: Dict[str, Path] = {}
        stats: Dict[str, int] = {"written": 0, "skipped": 0, "errors": 0, "dry_run": 0}

        for f in files:
            try:
                target_folder, target_name, meta = map_dbd_file_to_target(f, self.base_dir)
            except SkipFile as e:
                logger.info(f"SKIP: {e}")
                continue

            full_path = target_folder / f"{target_name}.strm"
            rel_path = full_path.relative_to(self.base_dir)

            key = self.strm_manager.normalize_path(rel_path)
            existing = normalized_to_actual.get(key)
            if existing and str(existing) != str(rel_path):
                logger.warning(f"⚠️ Path conflict: '{rel_path}' ↔ '{existing}'")
                logger.warning(f" Ignored: {f.name}")
                continue

            norm_full = self.strm_manager.normalize_path(full_path)
            if norm_full in normalized_created:
                logger.warning(f"⚠️ File already created under another form: {rel_path}")
                logger.warning(f" Ignored: {f.name}")
                continue

            if not f.url:
                logger.warning(f"⚠️ No URL for {f.name}")
                continue

            # Write/update .strm with Debrid-Link URL
            self.strm_manager.save_strm(
                full_path,
                f.url,
                self.dry_run,
                stats=stats,
            )

            # Create NFO (movie or episode)
            if meta["type"] == "movie":
                create_nfo_file(
                    target_folder,
                    meta["title"],
                    year=meta["year"],
                    release_date=meta["release_date"],
                )
            elif meta["type"] == "episode":
                create_nfo_file(
                    target_folder,
                    meta["title"],
                    year=meta["year"],
                    season=meta["season"],
                    episode_list=meta["episode_list"],
                    release_date=meta["release_date"],
                )

            normalized_to_actual[key] = rel_path
            normalized_created.add(norm_full)

        if self.dry_run:
            logger.info(
                "📝 (dry-run) .strm: %d to write, %d already up to date, %d errors",
                stats["dry_run"],
                stats["skipped"],
                stats["errors"],
            )
        else:
            logger.info(
                "📝 .strm: %d written/updated, %d already up to date, %d errors",
                stats["written"],
                stats["skipped"],
                stats["errors"],
            )

        return {self.strm_manager.normalize_path(p) for p in normalized_to_actual.values()}

    async def cleanup_old_strm_files(self, keep_normalized: Set[str]) -> None:
        logger.info("🔍 Searching for obsolete .strm files…")

        existing = list(self.base_dir.glob("**/*.strm"))
        to_delete: List[Path] = []
        kept = 0

        for path in existing:
            try:
                rel = path.relative_to(self.base_dir)
            except ValueError:
                continue

            key = self.strm_manager.normalize_path(rel)
            if key in keep_normalized:
                kept += 1
                logger.debug(f"Kept: {rel}")
            else:
                to_delete.append(path)
                logger.debug(f"To delete: {rel}")

        logger.info(f"📊 {kept} files kept, {len(to_delete)} to delete")

        for path in to_delete:
            self.strm_manager.delete_strm(path, self.dry_run)

    async def run(self, per_page: int = 100, oldest_first: bool = False) -> None:
        if not await self.verify_connection():
            raise DbdAPIError("Cannot connect to seedbox, aborting.")
    
        files = await self.fetch_files(per_page=per_page, oldest_first=oldest_first)
    
        if not files:
            logger.warning("⚠️ No files retrieved from Debrid-Link.")
            # No files = we should remove ALL existing .strm files
            keep_normalized: Set[str] = set()
        else:
            keep_normalized = await self.generate_strm_files(files)
    
        # Always run cleanup; if keep_normalized is empty, this deletes all .strm
        await self.cleanup_old_strm_files(keep_normalized)
    
        logger.info("✅ Done")



# ---------------------------------------------------------------------------
# Async one-shot runner
# ---------------------------------------------------------------------------

async def run_once(target: Path, args) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        api = DbdAPI(API_KEY, API_URL, client)
        app = DbdStrmGenerator(
            api=api,
            base_dir=target,
            sdbx_id=SDBX_ID,
            min_size=args.min_size,
            dry_run=args.dry_run,
        )
        await app.run(per_page=args.per_page, oldest_first=args.oldest)


async def amain() -> None:
    parser = argparse.ArgumentParser(
        description="Generate .strm files from Debrid-Link using Jellyfin-style naming",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Simulate operations without writing/deleting files",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose mode",
    )
    parser.add_argument(
        "-m",
        "--min-size",
        type=int,
        default=DEFAULT_MIN_SIZE,
        help="Minimum file size in bytes",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Number of items per page in API",
    )
    parser.add_argument(
        "--oldest",
        action="store_true",
        help="Process oldest files first",
    )
    parser.add_argument(
        "target",
        type=Path,
        nargs="?",
        default=Path.cwd(),
        help="Root directory where .strm files will be created/updated",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    missing = [
        name
        for name, val in [
            ("DBD_API_KEY", API_KEY),
            ("DBD_API_URL", API_URL),
            ("DBD_SDBX_ID", SDBX_ID),
        ]
        if not val
    ]
    if missing:
        logger.error(f"❌ Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    await run_once(args.target, args)


# ---------------------------------------------------------------------------
# Main loop with time.sleep()
# ---------------------------------------------------------------------------

def main() -> None:
    while True:
        try:
            asyncio.run(amain())
        except RateLimitError as e:
            logger.error(f"❌ {e}")
        except DbdAPIError as e:
            logger.error(f"❌ {e}")
        except (httpx.ConnectError, httpx.TimeoutException):
            logger.error(f"❌ Critical network error: cannot reach {API_URL}")
            logger.error(" Check your internet connection and API URL")
        except KeyboardInterrupt:
            logger.warning("\n⚠️ Interrupted")
            sys.exit(130)

        # If interval <= 0, run once and exit
        if RESCAN_INTERVAL_MIN <= 0:
            break

        logger.info(
            f"⏲️ Sleeping {RESCAN_INTERVAL_MIN} minutes before next scan..."
        )
        try:
            time.sleep(RESCAN_INTERVAL_MIN * 60)
        except KeyboardInterrupt:
            logger.warning("\n⚠️ Interrupted during sleep")
            sys.exit(130)


if __name__ == "__main__":
    main()
