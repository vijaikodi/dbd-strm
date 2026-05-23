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
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._client = client

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self._base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self._api_key}"}
        try:
            response = await self._client.get(url, headers=headers, params=params or {})
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as exc:
            raise DbdAPIError(f"HTTP {exc.response.status_code} for {url}") from exc
        except httpx.RequestError as exc:
            raise DbdAPIError(f"Request error for {url}: {exc}") from exc

        if not data.get("success", True):
            error = data.get("error", "unknown")
            if error == "floodDetected":
                reset_after = int(data.get("reset_after", 3600))
                raise RateLimitError(reset_after)
            raise DbdAPIError(f"API error: {error}")

        return data

    async def list_files(
        self,
        folder_id: str,
        base_path: str = "",
        per_page: int = 100,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[DbdFile]:
        page = 0
        while True:
            params: Dict[str, Any] = {"page": page, "perPage": per_page}
            if extra_params:
                params.update(extra_params)

            data = await self.get(f"/files/{folder_id}/list", params=params)
            items = data.get("value", [])

            logger.debug(f"DEBUG: folder={folder_id} page={page} items={len(items)}")

            for item in items:
                id_ = item.get("id", "")
                name = item.get("name", "")
                size = item.get("size", 0)
                download_url = item.get("downloadUrl", "") or item.get("url", "")
                item_type = item.get("type", "file")
                full_path = f"{base_path}/{name}".lstrip("/") if base_path else name

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
                    async for subfile in self.list_files(
                        id_,
                        full_path,
                        per_page=per_page,
                        extra_params=extra_params,
                    ):
                        yield subfile

                elif item_type == "movie":
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
                if len(items) < per_page:
                    break
                page += 1

            await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def fsync_dir(path: Path) -> None:
    """
    Open a directory and fsync it so the kernel flushes all pending metadata
    changes (unlink / rmdir / creat) through the bind-mount to the host
    filesystem immediately.
    """
    if not path.exists():
        return
    try:
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
            logger.debug(f"🔄 fsynced dir: {path}")
        finally:
            os.close(fd)
    except OSError as e:
        logger.warning(f"⚠️ fsync({path}) failed: {e}")


def force_delete_file(path: Path) -> bool:
    """
    Delete a single file and fsync its parent directory to flush the
    unlink() through the bind-mount immediately.
    Returns True on success.
    """
    try:
        path.unlink(missing_ok=True)
        logger.info(f"🗑️  Deleted file : {path}")
        fsync_dir(path.parent)
        return True
    except OSError as exc:
        logger.error(f"❌ Cannot delete {path}: {exc}")
        return False


def force_delete_dir(path: Path) -> bool:
    """
    Recursively delete *everything* inside a directory (all files regardless
    of extension, then all sub-directories), then remove the directory itself.
    Every removal is fsynced to the parent so changes are visible on the host
    side of the bind-mount immediately.
    Returns True if the directory no longer exists afterwards.
    """
    if not path.exists():
        return True

    if not path.is_dir():
        return force_delete_file(path)

    logger.info(f"🗑️  Force-deleting directory: {path}")

    # Walk bottom-up so we can rmdir leaf directories first
    for root, dirs, files in os.walk(str(path), topdown=False):
        root_path = Path(root)

        # Delete every file in this directory
        for fname in files:
            fpath = root_path / fname
            try:
                fpath.unlink(missing_ok=True)
                logger.debug(f"  deleted file: {fpath}")
            except OSError as e:
                logger.warning(f"  ⚠️ Cannot delete {fpath}: {e}")

        # fsync the directory after all its files are removed
        fsync_dir(root_path)

        # Remove sub-directories (they should be empty now)
        for dname in dirs:
            dpath = root_path / dname
            try:
                dpath.rmdir()
                logger.debug(f"  removed subdir: {dpath}")
                fsync_dir(root_path)
            except OSError as e:
                logger.warning(f"  ⚠️ Cannot rmdir {dpath}: {e}")

    # Finally remove the top-level directory itself
    try:
        path.rmdir()
        logger.info(f"🗑️  Removed directory  : {path}")
        fsync_dir(path.parent)
        return True
    except OSError as e:
        logger.warning(f"⚠️ Cannot remove top dir {path}: {e}")
        return False


def purge_dir_if_stale(path: Path) -> None:
    """
    Remove a directory and ALL its contents (strm, nfo, anything) if it
    contains no .strm files at all.  This catches the case where only .nfo
    files are left behind after .strm deletion.
    After removal every parent directory change is fsynced.
    """
    if not path.is_dir():
        return

    strm_files = list(path.rglob("*.strm"))
    if strm_files:
        # Still has valid content – leave it alone
        return

    logger.info(f"🧹 Purging stale directory (no .strm files): {path}")
    force_delete_dir(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_filename(filename: str) -> str:
    """Remove common release tags like 'www.something - ' from the beginning."""
    cleaned = re.sub(
        r'^(?:\[\s*www\.[\w.-]+\s*\]|\(\s*www\.[\w.-]+\s*\)|www\.[\w.-]+)\s*-\s*',
        '',
        filename,
        flags=re.IGNORECASE,
    )
    return cleaned.strip()


def extract_release_date(info: dict) -> str:
    """Try to extract release date from guessit info or use current date."""
    date_val = info.get("date")
    if date_val:
        return str(date_val)
    return datetime.now().strftime("%Y-%m-%d")


def create_nfo_file(
    folder: Path,
    title: str,
    year: Optional[int] = None,
    season: Optional[int] = None,
    episode_list: Optional[List[int]] = None,
    release_date: Optional[str] = None,
) -> None:
    """
    Create a minimal NFO file alongside the .strm to assist Jellyfin
    metadata scraping.
    - Movie:   movie.nfo
    - Episode: <Show> - S01E01.nfo
    """
    if season is not None and episode_list:
        episode_str = "-".join(f"E{e:02}" for e in episode_list)
        nfo_filename = f"{title} - S{season:02}{episode_str}.nfo"
        nfo_path = folder / nfo_filename

        if nfo_path.exists():
            return

        nfo_content = '<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n'
        nfo_content += "<episodedetails>\n"
        nfo_content += f"  <title>{title}</title>\n"
        nfo_content += f"  <season>{season}</season>\n"
        for ep in episode_list:
            nfo_content += f"  <episode>{ep}</episode>\n"
        if release_date or year:
            date_to_use = release_date or f"{year}-01-01"
            nfo_content += f"  <aired>{date_to_use}</aired>\n"
            nfo_content += f"  <premiered>{date_to_use}</premiered>\n"
        nfo_content += "</episodedetails>\n"
    else:
        nfo_filename = "movie.nfo"
        nfo_path = folder / nfo_filename

        if nfo_path.exists():
            return

        nfo_content = '<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n'
        nfo_content += "<movie>\n"
        nfo_content += f"  <title>{title}</title>\n"
        if year:
            nfo_content += f"  <year>{year}</year>\n"
        if release_date or year:
            date_to_use = release_date or f"{year}-01-01"
            nfo_content += f"  <releasedate>{date_to_use}</releasedate>\n"
            nfo_content += f"  <premiered>{date_to_use}</premiered>\n"
        nfo_content += "</movie>\n"

    try:
        folder.mkdir(parents=True, exist_ok=True)
        with open(nfo_path, "w", encoding="utf-8") as f:
            f.write(nfo_content)
            f.flush()
            os.fsync(f.fileno())
        fsync_dir(folder)
        logger.info(f"[NFO] Created NFO: {nfo_path}")
    except Exception as e:
        logger.error(f"[ERROR] Could not create NFO file: {e}")


# ---------------------------------------------------------------------------
# .strm management
# ---------------------------------------------------------------------------

class StrmManager:
    """Create / update / delete .strm files and normalise paths."""

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
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(url_text)
                fh.flush()
                os.fsync(fh.fileno())
            # Flush the new directory entry to the host
            fsync_dir(path.parent)
            bump("written")
            logger.debug(f"📝 Written: {path}")
            return True
        except OSError as exc:
            bump("errors")
            logger.error(f"❌ Cannot write {path}: {exc}")
            return False

    @staticmethod
    def delete_strm(path: Path, dry_run: bool = False) -> bool:
        """
        Delete a single .strm file.
        After deletion the parent directory is inspected:
          • if it still has .strm files  → leave it alone
          • if it has only .nfo files    → purge everything and rmdir
          • if it is empty               → rmdir
        Every change is fsynced so it is immediately visible on the host.
        """
        if dry_run:
            logger.info(f"🗑️  (dry-run) would delete {path}")
            return True

        if not force_delete_file(path):
            return False

        parent = path.parent

        # Re-check what remains in the parent after the deletion
        try:
            remaining = list(parent.iterdir())
        except OSError:
            return True

        # Check if any .strm files still exist in this folder
        remaining_strm = [f for f in remaining if f.suffix.lower() == ".strm"]

        if remaining_strm:
            # Other episodes / files still here – nothing more to do
            return True

        # No .strm left → purge the whole parent directory (catches .nfo leftovers)
        # For TV shows this is a Season XX folder; for movies it is the title folder.
        purge_dir_if_stale(parent)

        # For TV shows: also check the show root (parent of Season XX)
        # If all seasons are gone the show folder itself should be removed too.
        grandparent = parent.parent
        if grandparent.is_dir() and grandparent != path.parents[2]:
            purge_dir_if_stale(grandparent)

        return True


# ---------------------------------------------------------------------------
# Mapping Debrid files → Jellyfin paths
# ---------------------------------------------------------------------------

def map_dbd_file_to_target(
    file: DbdFile, dest_base: Path
) -> tuple[Path, str, Dict[str, Any]]:
    """
    Convert a Debrid file into (target_folder, target_basename, meta).
    target_basename is without extension; .strm will be appended by the caller.
    """
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
        parent_name = Path(file.path).parent.name
        if parent_name:
            title = clean_filename(parent_name)
            title = re.sub(r"\s*S\d{2}\s*-\s*EP\([^)]+\).*$", "", title, flags=re.IGNORECASE)
            title = re.sub(r"\s*\(\d{4}\).*$", "", title)
            title = title.strip()
        else:
            title = info.get("title")

        if not title:
            raise SkipFile(f"No show title for episode: {filename}")

        season = info.get("season")
        episode = info.get("episode")

        if isinstance(season, list) and season:
            season = season[0]

        if isinstance(episode, list) and episode:
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
                logger.warning(f"  Ignored: {f.name}")
                continue

            norm_full = self.strm_manager.normalize_path(full_path)
            if norm_full in normalized_created:
                logger.warning(f"⚠️ File already created under another form: {rel_path}")
                logger.warning(f"  Ignored: {f.name}")
                continue

            if not f.url:
                logger.warning(f"⚠️ No URL for {f.name}")
                continue

            self.strm_manager.save_strm(
                full_path,
                f.url,
                self.dry_run,
                stats=stats,
            )

            # Create NFO (movie or episode)
            if not self.dry_run:
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
                logger.debug(f"✅ Kept: {rel}")
            else:
                to_delete.append(path)
                logger.debug(f"🗑️  To delete: {rel}")

        logger.info(f"📊 {kept} files kept, {len(to_delete)} to delete")

        for path in to_delete:
            self.strm_manager.delete_strm(path, self.dry_run)

        # Final pass: purge any directory under movies/ or tvshows/ that has
        # no .strm files left (catches stubborn .nfo-only or empty folders).
        if not self.dry_run:
            self._final_purge_stale_dirs()

    def _final_purge_stale_dirs(self) -> None:
        """
        Walk every immediate child of movies/ and tvshows/.
        Any subtree that contains zero .strm files is completely removed and
        every removal is fsynced so it is instantly visible on the host.
        """
        logger.info("🔍 Final pass: purging stale directories…")

        for media_root in ["movies", "tvshows"]:
            media_dir = self.base_dir / media_root
            if not media_dir.is_dir():
                continue

            # For movies: children are title folders  (movies/<Title (Year)>/)
            # For tvshows: children are show folders  (tvshows/<Show>/)
            #              grandchildren are season folders
            # We check at the show/title level so the whole tree is removed
            # in one shot when nothing is left.
            for child in sorted(media_dir.iterdir()):
                if not child.is_dir():
                    continue
                strm_count = len(list(child.rglob("*.strm")))
                if strm_count == 0:
                    logger.info(
                        f"🧹 No .strm files left under '{child.name}' – removing entire tree"
                    )
                    force_delete_dir(child)

            # After children are processed, fsync the media root itself
            fsync_dir(media_dir)


# ---------------------------------------------------------------------------
# Single run
# ---------------------------------------------------------------------------

async def run_once(target: Path, args: argparse.Namespace) -> None:
    target.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(timeout=30) as client:
        api = DbdAPI(API_KEY, API_URL, client)
        generator = DbdStrmGenerator(
            api=api,
            base_dir=target,
            sdbx_id=SDBX_ID,
            min_size=args.min_size,
            dry_run=args.dry_run,
        )

        logger.info(f"📁 Output directory : {target}")
        logger.info(f"🔑 API URL          : {API_URL}")

        files = await generator.fetch_files(
            per_page=args.per_page,
            oldest_first=args.oldest,
        )

        keep = await generator.generate_strm_files(files)
        await generator.cleanup_old_strm_files(keep)

    logger.info("✅ Done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def amain() -> None:
    parser = argparse.ArgumentParser(
        description="Generate .strm files from Debrid-Link using Jellyfin-style naming",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d", "--dry-run",
        action="store_true",
        help="Simulate operations without writing/deleting files",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose mode",
    )
    parser.add_argument(
        "-m", "--min-size",
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
            logger.error("  Check your internet connection and API URL")
        except KeyboardInterrupt:
            logger.warning("\n⚠️ Interrupted")
            sys.exit(130)

        if RESCAN_INTERVAL_MIN <= 0:
            break

        logger.info(f"⏳ Next scan in {RESCAN_INTERVAL_MIN} minutes…")
        time.sleep(RESCAN_INTERVAL_MIN * 60)


if __name__ == "__main__":
    main()
