@staticmethod
def delete_strm(path: Path, dry_run: bool = False) -> bool:
    if dry_run:
        logger.info(f"🗑️ (dry-run) {path}")
        return True

    try:
        path.unlink()
        logger.info(f"🗑️ Deleted {path}")
        
        # Clean up parent folder if it only contains .nfo files or is empty
        parent = path.parent
        try:
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
            # Note: We only remove the immediate parent here.
            # The full recursive cleanup happens in _cleanup_empty_folders()
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