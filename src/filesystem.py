import logging
import os
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class FilesystemDriver:
    def __init__(self, root_path: str, dry_run: bool = False):
        self.root_path = Path(root_path)
        self.dry_run = dry_run

    def _run_cmd(self, cmd: list, check=True):
        if self.dry_run:
            logger.info(f"[DRY-RUN] Executing: {' '.join(cmd)}")
            return

        try:
            logger.debug(f"Executing: {' '.join(cmd)}")
            subprocess.run(cmd, check=check, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.stderr}")
            raise

    def ensure_directory(self, rel_path: str, gid: int, mode: str = "770"):
        """Creates directory, sets ownership and permissions."""
        full_path = self.root_path / rel_path.lstrip("/")

        if not full_path.exists():
            logger.info(f"Creating directory: {full_path}")
            if not self.dry_run:
                full_path.mkdir(parents=True, exist_ok=True)

        # Set Ownership (root:gid)
        # Note: uid 0 is root. We strictly enforce GID from Waldur.
        logger.info(f"Setting ownership root:{gid} on {full_path}")
        if not self.dry_run:
            os.chown(full_path, 0, gid)

        # Set Permissions
        # Parse octal string "770" -> int
        mode_int = int(mode, 8)
        logger.info(f"Setting mode {mode} on {full_path}")
        if not self.dry_run:
            os.chmod(full_path, mode_int)

    def set_lustre_quota(self, rel_path: str, gid: int, quotas: list):
        """Applies quotas using lfs setquota."""
        # Defaults
        block_soft = 0
        block_hard = 0
        inode_soft = 0
        inode_hard = 0

        # Parse the JSON quota list
        for q in quotas:
            # Note: Input unit is usually 'tera' for space, we need kilobytes for lfs
            val = float(q.quota)
            if q.type == "space":
                # Convert TB to KB (1024^3)
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

        logger.info(f"Applying Quota on {full_path} for GID {gid}")
        self._run_cmd(
            cmd, check=False
        )  # check=False to prevent crashing on transient Lustre errors

    def archive_directory(self, rel_path: str, archive_root: str):
        """Moves a directory to the trash/archive location instead of rm -rf."""
        full_path = self.root_path / rel_path.lstrip("/")
        if not full_path.exists():
            logger.warning(f"Cannot archive {full_path}, does not exist.")
            return

        target_name = f"{full_path.name}_archived_{os.urandom(4).hex()}"
        archive_path = Path(archive_root) / target_name

        logger.info(f"Archiving {full_path} -> {archive_path}")
        if not self.dry_run:
            # Ensure archive root exists
            Path(archive_root).mkdir(parents=True, exist_ok=True)
            shutil.move(str(full_path), str(archive_path))
