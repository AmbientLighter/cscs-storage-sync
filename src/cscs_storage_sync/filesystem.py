import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import List

from .models import QuotaItem

logger = logging.getLogger(__name__)


class FilesystemDriver:
    def __init__(self, root_path: str, dry_run: bool = False):
        self.root_path = Path(root_path)
        self.dry_run = dry_run

    def _run_cmd(self, cmd: List[str], check=True):
        cmd_str = " ".join(cmd)
        if self.dry_run:
            logger.info(f"[DRY-RUN] Executing: {cmd_str}")
            return

        try:
            logger.debug(f"Executing: {cmd_str}")
            subprocess.run(cmd, check=check, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.stderr}")
            if check:
                raise

    def ensure_directory(self, rel_path: str, gid: int, mode: str = "775"):
        """Creates directory, sets ownership and permissions."""
        # Remove leading slash to join correctly with root
        clean_rel_path = rel_path.lstrip("/")
        full_path = self.root_path / clean_rel_path

        # 1. Create Directory
        if not full_path.exists():
            logger.info(f"Creating directory: {full_path}")
            if not self.dry_run:
                full_path.mkdir(parents=True, exist_ok=True)

        # 2. Set Ownership (root:gid)
        # 0 is root uid. gid comes from Waldur (or 0 for tenants)
        current_stat = full_path.stat() if full_path.exists() else None

        if not self.dry_run and current_stat:
            if current_stat.st_gid != gid:
                logger.info(f"Chowning {full_path} to 0:{gid}")
                os.chown(full_path, 0, gid)

        # 3. Set Permissions
        if mode:
            mode_int = int(mode, 8)
            if not self.dry_run and current_stat:
                if (current_stat.st_mode & 0o777) != mode_int:
                    logger.info(f"Chmoding {full_path} to {mode}")
                    os.chmod(full_path, mode_int)

    def set_lustre_quota(self, rel_path: str, gid: int, quotas: List[QuotaItem]):
        """Applies quotas using lfs setquota."""
        if gid == 0:
            logger.warning("Skipping quota application for GID 0 (root).")
            return

        block_soft = 0
        block_hard = 0
        inode_soft = 0
        inode_hard = 0

        for q in quotas:
            val = float(q.quota)
            if q.type == "space":
                # Waldur sends TB, Lustre expects KB
                kb_val = int(val * 1024 * 1024 * 1024)
                if q.enforcementType == "soft":
                    block_soft = kb_val
                if q.enforcementType == "hard":
                    block_hard = kb_val
            elif q.type == "inodes":
                if q.enforcementType == "soft":
                    inode_soft = int(val)
                if q.enforcementType == "hard":
                    inode_hard = int(val)

        full_path = self.root_path / rel_path.lstrip("/")

        cmd = [
            "lfs",
            "setquota",
            "-g",
            str(gid),
            "-b",
            str(block_soft),
            "-B",
            str(block_hard),
            "-i",
            str(inode_soft),
            "-I",
            str(inode_hard),
            str(full_path),
        ]

        logger.info(f"Setting quota for GID {gid} on {full_path}")
        self._run_cmd(cmd, check=False)

    def archive_directory(self, rel_path: str, archive_root: str):
        """Moves a directory to the archive location."""
        full_path = self.root_path / rel_path.lstrip("/")
        if not full_path.exists():
            logger.warning(f"Directory {full_path} not found, skipping archive.")
            return

        # Create unique archive name
        archive_name = f"{full_path.name}_archived_{os.urandom(4).hex()}"
        archive_dest = Path(archive_root) / archive_name

        logger.info(f"Archiving {full_path} -> {archive_dest}")
        if not self.dry_run:
            Path(archive_root).mkdir(parents=True, exist_ok=True)
            shutil.move(str(full_path), str(archive_dest))
