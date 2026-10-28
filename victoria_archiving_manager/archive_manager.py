"""
BESS Historian Archive & Retention Manager (VictoriaMetrics)
============================================================
Enterprise-grade data retention, cold storage archiving, and safe purging
engine for VictoriaMetrics in Battery Energy Storage System (BESS) EMS environments.

Classes:
1. ArchiveConfig          - Loads and validates archive_config.json (supports inline // comments).
2. AlertNotifier          - Sends failure alert cards to Slack / Teams / Discord webhooks.
3. ArchiveVerifier        - Validates SHA256 checksums and Gzip payload integrity.
4. VictoriaMetricsClient  - Encapsulates VM HTTP APIs (export, delete, query, watchdog metrics, restore import).
5. BESSArchiveManager     - Core orchestrator driving retention cutoff calculation, export, and purge.
"""

import argparse
import datetime
import gzip
import hashlib
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv

# Setup self-contained date-named logging: victoria_archiving_manager/logs/<DD-MM-YYYY>.log
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
# EMS uses the repository .env; do not depend on the old sibling victoria-stack.
load_dotenv()
_parent_dir = str(CURRENT_DIR.parent)
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)

from victoria_archiving_manager.log_setup import setup_logging


def env_path(name):
    """Resolve relative paths from the project root."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing {name} in .env")

    path = Path(value).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


logger = setup_logging(
    service_name="archive_manager_victoria",
    level="INFO",
    log_dir=env_path("ARCHIVE_LOG_DIR")
)


class ArchiveConfig:
    """Loads and validates the archiving & retention configuration."""

    def __init__(self, config_path: Path):
        self.config_path = config_path.resolve()
        self.data = self._load_and_clean(self.config_path)
        self._validate()

    def _validate(self):
        """Reject policy typos before exporting or deleting any VM series."""
        if not isinstance(self.data, dict):
            raise ValueError("Archive policy must be a JSON object")
        for key in ("enable_purge",):
            if not isinstance(self.data.get(key, True), bool):
                raise ValueError(f"{key} must be true or false")
        buffer = self.data.get("safety_buffer_months", 1)
        if type(buffer) is not int or buffer < 0:
            raise ValueError("safety_buffer_months must be a non-negative integer")
        for key, default in (("partition_label", "record_month"), ("resolution_label", "resolution")):
            if not isinstance(self.data.get(key, default), str) or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", self.data.get(key, default)):
                raise ValueError(f"Invalid {key}")
        tiers = self.data.get("tiers", [])
        if not isinstance(tiers, list):
            raise ValueError("tiers must be a list")
        names = set()
        for tier in tiers:
            if not isinstance(tier, dict) or not re.fullmatch(r"[a-zA-Z0-9_]+", str(tier.get("name", ""))) or tier["name"] in names:
                raise ValueError("Tier names must be valid and unique")
            names.add(tier["name"])
            if not isinstance(tier.get("enabled", True), bool):
                raise ValueError("Tier enabled must be true or false")
            if type(tier.get("retention_days")) is not int or tier["retention_days"] <= 0:
                raise ValueError("Tier retention_days must be a positive integer")
            if not isinstance(tier.get("resolution"), str) or not re.fullmatch(r"[a-zA-Z0-9_]+", tier["resolution"]):
                raise ValueError("Tier resolution must be a label value such as 1_min or 15m")

    @staticmethod
    def _load_and_clean(path: Path) -> dict:
        if not path.exists():
            raise FileNotFoundError(f"Archiving config file not found: {path}")
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read()
        # Strip inline // comments to allow developer-friendly commented JSON
        # Preserve quoted strings such as http:// while removing JSON comments.
        content_clean = re.sub(r'"(?:\\.|[^"\\])*"|//[^\r\n]*',
                               lambda match: "" if match.group().startswith("//") else match.group(), content)
        return json.loads(content_clean)

    @property
    def storage_path(self) -> Path:
        # Archive location is a deployment setting, never a retention JSON value.
        return env_path("ARCHIVE_STORAGE_DIR")

    @property
    def partition_label(self) -> str:
        return self.data.get("partition_label", "record_month")

    @property
    def resolution_label(self) -> str:
        return self.data.get("resolution_label", "resolution")

    @property
    def safety_buffer_months(self) -> int:
        return self.data.get("safety_buffer_months", 1)

    @property
    def enable_purge(self) -> bool:
        return self.data.get("enable_purge", True)

    @property
    def retry_policy(self) -> dict:
        return self.data.get("retry", {"max_attempts": 3, "backoff_factor": 2, "initial_delay_seconds": 10})

    @property
    def tiers(self) -> List[dict]:
        return self.data.get("tiers", [])


class AlertNotifier:
    """Dispatches webhook incident alerts for failed archiving operations."""

    def __init__(self, webhook_url: str = ""):
        self.webhook_url = webhook_url.strip()

    def send_failure_alert(self, tier: str, period: str, reason: str):
        if not self.webhook_url:
            return

        payload = {
            "text": "🚨 *CRITICAL: BESS VictoriaMetrics Archiving Job Failed!*",
            "attachments": [
                {
                    "color": "#FF0000",
                    "title": f"Archiving Failed: Tier [{tier}] for Period [{period}]",
                    "fields": [
                        {"title": "Tier", "value": tier, "short": True},
                        {"title": "Target Period", "value": period, "short": True},
                        {"title": "Reason", "value": reason, "short": False},
                        {"title": "Safety State", "value": "Active VictoriaMetrics DB was NOT modified. Zero data lost.", "short": False},
                    ],
                    "footer": "BESS Archive Manager Watchdog",
                    "ts": int(time.time()),
                }
            ],
        }
        try:
            requests.post(self.webhook_url, json=payload, timeout=10)
        except Exception as err:
            logger.error(f"Failed to deliver webhook alert: {err}")


class ArchiveVerifier:
    """Performs integrity verification gates on cold storage archive files."""

    @staticmethod
    def calculate_sha256(file_path: Path) -> str:
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @classmethod
    def verify_checksum(cls, gz_path: Path) -> bool:
        sha_file = Path(str(gz_path) + ".sha256")
        if not sha_file.exists():
            logger.error(f"Checksum verification failed: Missing checksum file {sha_file.name}")
            return False

        with open(sha_file, "r", encoding="utf-8") as f:
            expected_hash = f.read().strip().split()[0]

        actual_hash = cls.calculate_sha256(gz_path)
        if expected_hash.lower() == actual_hash.lower():
            logger.info(f"Checksum OK for {gz_path.name} (SHA256: {actual_hash[:12]}...)")
            return True

        logger.error(f"CHECKSUM MISMATCH for {gz_path.name}! Expected {expected_hash}, got {actual_hash}")
        return False

    @classmethod
    def verify_gzip_content(cls, gz_path: Path) -> Tuple[bool, int]:
        """Decompresses and counts lines to ensure the gzip payload is valid JSONL."""
        if not gz_path.exists() or gz_path.stat().st_size == 0:
            logger.error(f"Archive file is empty or does not exist: {gz_path}")
            return False, 0

        line_count = 0
        try:
            with gzip.open(gz_path, "rt", encoding="utf-8") as gz:
                for line in gz:
                    if line.strip():
                        line_count += 1
                        json.loads(line)  # Validate every JSONL record, not only the first.
            logger.info(f"Content Verified: {gz_path.name} contains {line_count:,} valid metric records.")
            return True, line_count
        except Exception as err:
            logger.error(f"CORRUPT ARCHIVE! Decompression validation failed for {gz_path.name}: {err}")
            return False, 0


class VictoriaMetricsClient:
    """Manages all HTTP communication with VictoriaMetrics Single-Node."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        # Scope the approved archive flow to this EMS database when configured.
        self.database = os.getenv("PRIMARY_DB_NAME", "")

    def query_series_count(self, match_expr: str) -> int:
        """Use VM's supported series API over the historical range, not a missing count endpoint."""
        response = requests.get(f"{self.base_url}/api/v1/series",
                                params={"match[]": match_expr, "start": "1970-01-02T00:00:00Z", "end": "now"}, timeout=30)
        response.raise_for_status()
        body = response.json()
        if body.get("status") != "success":
            raise ValueError("Victoria series lookup failed")
        return len(body["data"])


    def query_data_points_count(self, match_expr: str, start: str = "-15y", end: str = "now") -> int:
        """Count actual exported observations for dry-run; do not ignore the supplied range."""
        points = 0
        with requests.get(f"{self.base_url}/api/v1/export",
                          params={"match[]": match_expr, "start": start, "end": end, "max_rows_per_line": "10000"},
                          stream=True, timeout=120) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line.strip():
                    points += len(json.loads(line)["values"])
        return points


    def discover_partition_labels(self, label_name: str) -> List[str]:
        """Discovers all partition values across a 15-year historical horizon."""
        url = f"{self.base_url}/api/v1/label/{label_name}/values"
        now = datetime.datetime.now(datetime.timezone.utc)
        start_15y = (now - datetime.timedelta(days=15 * 365)).strftime("%Y-%m-%dT00:00:00Z")
        end_now = now.strftime("%Y-%m-%dT23:59:59Z")

        try:
            resp = requests.get(url, params={"start": start_15y, "end": end_now,
                                        **({"match[]": '{database=' + json.dumps(self.database) + '}'} if self.database else {})}, timeout=10)
            if resp.status_code == 200:
                return resp.json().get("data", [])
            resp.raise_for_status()
        except Exception as err:
            logger.error(f"Failed to query label values from VictoriaMetrics: {err}")
            raise  # A connection failure is not proof that retention is up to date.

    def export_data(self, match_expr: str, output_file: Path, retry_cfg: dict) -> bool:
        """Streams /api/v1/export directly into a gzip compressed archive with retry logic."""
        url = f"{self.base_url}/api/v1/export"
        max_attempts = retry_cfg.get("max_attempts", 3)
        backoff = retry_cfg.get("backoff_factor", 2)
        delay = retry_cfg.get("initial_delay_seconds", 5)

        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Exporting stream from {self.base_url} [Attempt {attempt}/{max_attempts}]...")
                with requests.get(url, params={"match[]": match_expr, "start": "1970-01-02T00:00:00Z", "end": "now"}, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    with gzip.open(output_file, "wb") as gz_out:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                gz_out.write(chunk)

                if output_file.exists() and output_file.stat().st_size > 0:
                    logger.info(f"Stream export written to {output_file.name} ({output_file.stat().st_size:,} bytes).")
                    return True
            except Exception as err:
                logger.warning(f"Export attempt {attempt} failed: {err}")
                if output_file.exists():
                    output_file.unlink(missing_ok=True)
                if attempt < max_attempts:
                    time.sleep(delay)
                    delay *= backoff

        return False

    def purge_partition(self, match_expr: str) -> bool:
        """Deletes series matching the expression via /api/v1/admin/tsdb/delete_series."""
        url = f"{self.base_url}/api/v1/admin/tsdb/delete_series"
        try:
            resp = requests.post(url, params={"match[]": match_expr}, timeout=30)
            if resp.status_code in (200, 204):
                logger.info(f"Purge API successfully executed for: {match_expr}")
                return True
            logger.error(f"Purge API returned HTTP {resp.status_code}: {resp.text}")
            return False
        except Exception as err:
            logger.error(f"Purge request failed: {err}")
            return False

    def push_watchdog_metrics(self, tier: str, status: int, records: int = 0, bytes_written: int = 0):
        """Pushes self-monitoring operational metrics into VictoriaMetrics."""
        import_url = f"{self.base_url}/api/v1/import/prometheus"
        now_ts = int(time.time())
        lines = [
            f'bess_archive_status{{tier="{tier}"}} {status}',
            f'bess_archive_last_run_timestamp_seconds{{tier="{tier}"}} {now_ts}',
        ]
        if status == 1:
            lines.extend([
                f'bess_archive_records_exported_total{{tier="{tier}"}} {records}',
                f'bess_archive_bytes_written_total{{tier="{tier}"}} {bytes_written}',
            ])
        try:
            requests.post(
                import_url,
                data="\n".join(lines) + "\n",
                headers={"Content-Type": "text/plain"},
                timeout=5
            )
        except Exception as err:
            logger.debug(f"Could not push watchdog metrics: {err}")

    def restore_archive(self, archive_file: Path) -> bool:
        """Disaster recovery: streams decompressed JSONL into VictoriaMetrics via /api/v1/import."""
        url = f"{self.base_url}/api/v1/import"
        if not archive_file.exists():
            logger.error(f"Restore archive file does not exist: {archive_file}")
            return False
        if archive_file.name.endswith(".gz") and not ArchiveVerifier.verify_checksum(archive_file):
            logger.error("Restore blocked: archive checksum missing or invalid")
            return False
        if archive_file.name.endswith(".gz"):
            valid, records = ArchiveVerifier.verify_gzip_content(archive_file)
            if not valid or not records:
                logger.error("Restore blocked: archive content is invalid or empty")
                return False

        logger.info(f"Restoring {archive_file.name} into VictoriaMetrics via {url}...")
        try:
            with open(archive_file, "rb") as f_in:
                # If gzipped, decompress stream
                if archive_file.name.endswith(".gz"):
                    with gzip.open(archive_file, "rb") as gz_in:
                        # Chunk the decompressed body instead of loading a month into RAM.
                        resp = requests.post(url, data=iter(lambda: gz_in.read(65536), b""), headers={"Content-Type": "application/json"}, timeout=120)
                else:
                    resp = requests.post(url, data=f_in, headers={"Content-Type": "application/json"}, timeout=120)

            if resp.status_code in (200, 204):
                logger.info(f"Disaster Recovery Restore Successful for {archive_file.name}!")
                return True
            logger.error(f"Restore failed with HTTP {resp.status_code}: {resp.text}")
            return False
        except Exception as err:
            logger.error(f"Restore request failed: {err}")
            return False


class BESSArchiveManager:
    """Core Orchestrator for VictoriaMetrics Data Retention and Archiving."""

    def __init__(
        self,
        config_path: Path,
        storage_path: Optional[Path] = None,
        vm_url: str = "http://localhost:8428",
        webhook_url: str = ""
    ):
        self.config = ArchiveConfig(config_path)
        self.storage_path = storage_path.resolve() if storage_path else self.config.storage_path
        self.vm_client = VictoriaMetricsClient(vm_url)
        self.notifier = AlertNotifier(webhook_url)
        self.verifier = ArchiveVerifier()

        # Ensure base archive directory exists
        self.storage_path.mkdir(parents=True, exist_ok=True)

    def reload_config(self) -> bool:
        """Validate the saved policy before swapping it between archive cycles."""
        candidate = ArchiveConfig(self.config.config_path)
        if candidate.data == self.config.data:
            return False
        self.config = candidate
        logger.info("Archive policy changed; applying validated settings to the next cycle")
        return True

    def calculate_expired_periods(self, retention_days: int) -> Tuple[str, List[str]]:
        now = datetime.datetime.now(datetime.timezone.utc)
        total_retention_days = retention_days + (self.config.safety_buffer_months * 30)
        cutoff_date = now - datetime.timedelta(days=total_retention_days)
        cutoff_str = cutoff_date.strftime("%Y-%m")

        all_periods = self.vm_client.discover_partition_labels(self.config.partition_label)
        # Exclude the cutoff month: its newest readings have not all expired yet.
        expired = sorted([p for p in all_periods if re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", p) and p < cutoff_str])
        return cutoff_str, expired

    def display_status(self):
        """Displays status overview of configured tiers and discovered partition labels."""
        print("\n" + "=" * 80)
        print("          VICTORIAMETRICS HISTORIAN ARCHIVING & RETENTION STATUS")
        print("=" * 80)
        print(f"VictoriaMetrics URL : {self.vm_client.base_url}")
        print(f"Storage Directory   : {self.storage_path}")
        print(f"Safety Buffer       : {self.config.safety_buffer_months} month(s)")
        print("-" * 80)

        all_partitions = self.vm_client.discover_partition_labels(self.config.partition_label)
        print(f"Discovered Partition Labels ({self.config.partition_label}): {all_partitions or 'None found'}")
        print("-" * 80)

        header = f"{'Tier Name':<16} | {'Resolution':<12} | {'Retention':<10} | {'Cutoff':<10} | {'Expired Partitions'}"
        print(header)
        print("-" * 80)

        for tier in self.config.tiers:
            name = tier["name"]
            res = tier.get("resolution", "1m")
            days = tier.get("retention_days", 365)
            cutoff, expired = self.calculate_expired_periods(days)
            exp_str = f"{len(expired)} ({', '.join(expired)})" if expired else "0 (Up to date)"
            print(f"{name:<16} | {res:<12} | {days:<6} days | {cutoff:<10} | {exp_str}")

        print("=" * 80 + "\n")

    def run_tier(
        self,
        tier_cfg: dict,
        dry_run: bool = False,
        force_purge: bool = False,
        skip_purge: bool = False,
        verify_only: bool = False,
        specific_month: Optional[str] = None
    ) -> bool:
        tier_name = tier_cfg["name"]
        resolution = tier_cfg.get("resolution", "1m")
        retention_days = tier_cfg.get("retention_days", 365)
        logger.info(f"\n--- Processing Tier: {tier_name} (Resolution: {resolution}) ---")

        if specific_month:
            if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", specific_month):
                raise ValueError("Month must use YYYY-MM")
            cutoff_str = specific_month
            expired_periods = [specific_month]
        elif verify_only:
            # Verification is local and must work after source deletion or VM outage.
            prefix = f"bess_{tier_name}_"
            expired_periods = sorted(path.name[len(prefix):-len(".jsonl.gz")]
                                     for path in (self.storage_path / tier_name).glob(f"{prefix}*.jsonl.gz"))
            cutoff_str = "local archives"
            if not expired_periods:
                logger.error("No local archives found for tier %s", tier_name)
                return False
        else:
            cutoff_str, expired_periods = self.calculate_expired_periods(retention_days)

        logger.info(f"Retention Cutoff: {cutoff_str} | Target Partitions: {len(expired_periods)}")

        if not expired_periods:
            logger.info(f"No expired partitions for tier [{tier_name}]. Database is within retention policy.")
            if not dry_run and not verify_only:
                self.vm_client.push_watchdog_metrics(tier_name, status=1)
            return True

        overall_success = True
        tier_dir = self.storage_path / tier_name
        tier_dir.mkdir(parents=True, exist_ok=True)

        for period in expired_periods:
            database_filter = (",database=" + json.dumps(self.vm_client.database)) if self.vm_client.database else ""
            match_expr = f'{{{self.config.partition_label}="{period}",{self.config.resolution_label}="{resolution}"{database_filter}}}'

            gz_path = tier_dir / f"bess_{tier_name}_{period}.jsonl.gz"
            sha_path = gz_path.with_suffix(".gz.sha256")

            if verify_only:
                chk_ok = self.verifier.verify_checksum(gz_path)
                cont_ok, lines = self.verifier.verify_gzip_content(gz_path)
                if not (chk_ok and cont_ok):
                    overall_success = False
                continue

            active_series = self.vm_client.query_series_count(match_expr)
            logger.info(f"Partition [{period}]: {active_series} series matching {match_expr}")
            if active_series == 0:
                logger.info(f"Zero active series for [{period}]. Skipping.")
                continue

            if dry_run:
                pts = self.vm_client.query_data_points_count(match_expr)
                logger.info(f"[DRY-RUN] Would export ~{pts:,} points ({active_series} series) to {gz_path.name} and purge.")
                continue

            # Step 1: Export stream to .tmp file
            tmp_gz = gz_path.with_name(f"{gz_path.name}.tmp")
            export_ok = self.vm_client.export_data(match_expr, tmp_gz, self.config.retry_policy)
            if not export_ok:
                err_msg = f"Failed to export data stream for {period}"
                logger.error(err_msg)
                self.notifier.send_failure_alert(tier_name, period, err_msg)
                self.vm_client.push_watchdog_metrics(tier_name, status=0)
                overall_success = False
                continue

            # Step 2: Generate Checksum
            sha256_hash = self.verifier.calculate_sha256(tmp_gz)
            tmp_sha = Path(str(tmp_gz) + ".sha256")
            with open(tmp_sha, "w", encoding="utf-8") as f:
                f.write(f"{sha256_hash}  {gz_path.name}\n")
            logger.info(f"Checksum generated: {sha256_hash[:16]}...")

            # Step 3: Two-Phase Verification Gate
            chk_ok = self.verifier.verify_checksum(tmp_gz)
            cont_ok, record_count = self.verifier.verify_gzip_content(tmp_gz)

            if not (chk_ok and cont_ok and record_count > 0):
                err_msg = f"Verification Gate FAILED for {tmp_gz.name}! Active database will NOT be purged."
                logger.critical(err_msg)
                tmp_gz.unlink(missing_ok=True)
                tmp_sha.unlink(missing_ok=True)
                self.notifier.send_failure_alert(tier_name, period, err_msg)
                self.vm_client.push_watchdog_metrics(tier_name, status=0)
                overall_success = False
                continue

            # Never overwrite an archive from a previous purge with late-arriving data.
            # Gzip metadata can differ between exports, so compare decompressed bytes.
            if gz_path.exists():
                def content_hash(path):
                    digest = hashlib.sha256()
                    with gzip.open(path, "rb") as stream:
                        for chunk in iter(lambda: stream.read(65536), b""):
                            digest.update(chunk)
                    return digest.digest()
                same = self.verifier.verify_checksum(gz_path) and content_hash(gz_path) == content_hash(tmp_gz)
                tmp_gz.unlink(missing_ok=True)
                tmp_sha.unlink(missing_ok=True)
                if not same:
                    logger.error("Existing archive differs; retained original and blocked purge for %s. Reconcile late data separately.", period)
                    overall_success = False
                    continue
            else:
                tmp_gz.replace(gz_path)
                tmp_sha.replace(sha_path)
            logger.info(f"Committed archive: {gz_path.name}")

            # Step 4: Gated Safe Purge
            should_purge = (self.config.enable_purge or force_purge) and not skip_purge
            if should_purge:
                logger.info(f"Verification PASSED. Purging active database for [{period}]...")
                purge_ok = self.vm_client.purge_partition(match_expr)
                if not purge_ok:
                    logger.error("Purge failed; reporting the archive cycle as failed.")
                    overall_success = False
                    self.vm_client.push_watchdog_metrics(tier_name, status=0)
                    continue
            else:
                logger.info("Purge disabled by configuration. Cold storage archive saved successfully.")

            self.vm_client.push_watchdog_metrics(
                tier_name,
                status=1,
                records=record_count,
                bytes_written=gz_path.stat().st_size
            )

        return overall_success

    def run_all(
        self,
        specific_tier: Optional[str] = None,
        specific_month: Optional[str] = None,
        dry_run: bool = False,
        force_purge: bool = False,
        skip_purge: bool = False,
        verify_only: bool = False
    ) -> bool:
        # Invalid edits raise before any export/delete; never silently use an old
        # purge policy when the operator may be trying to disable it.
        self.reload_config()
        all_passed = True
        tiers_to_process = self.config.tiers

        if specific_tier:
            tiers_to_process = [t for t in tiers_to_process if t["name"] == specific_tier]
            if not tiers_to_process:
                logger.error(f"Specified tier '{specific_tier}' not found in configuration.")
                return False

        for tier in tiers_to_process:
            if not tier.get("enabled", True):
                logger.info(f"Tier [{tier['name']}] is disabled. Skipping.")
                continue
            res = self.run_tier(
                tier,
                dry_run=dry_run,
                force_purge=force_purge,
                skip_purge=skip_purge,
                verify_only=verify_only,
                specific_month=specific_month
            )
            if not res:
                all_passed = False

        return all_passed


def wait_for_policy_change(manager, seconds):
    """Watch JSON every 60s while waiting for the normal schedule or outage retry."""
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(60, remaining))
        try:
            if manager.reload_config():
                logger.info("Policy change detected; starting an archive cycle without waiting for the daily schedule")
                return
        except Exception:
            # A partially saved/invalid JSON is not activated. run_all validates
            # again before doing any work, even when this wait expires.
            logger.exception("Archive policy is invalid/unavailable; waiting for a valid saved configuration")


def main(service_default=False):
    """Keep maintenance commands; the root launcher defaults to scheduled operation."""
    default_cfg = env_path("ARCHIVE_CONFIG_PATH")
    default_storage = env_path("ARCHIVE_STORAGE_DIR")

    # Archive and generator may target different deployments; no sibling .env.
    default_vm = os.getenv("VMALERT_DATASOURCE_URL")
    webhook_url = os.getenv("ARCHIVE_ALERT_WEBHOOK").strip()

    parser = argparse.ArgumentParser(description="VictoriaMetrics BESS Historian Archive & Retention Manager")
    parser.add_argument("--config", default=str(default_cfg), help="Path to archive_config.json")
    parser.add_argument("--storage", default=str(default_storage), help="Directory to store archives")
    parser.add_argument("--vm-url", default=default_vm, help="VictoriaMetrics endpoint URL")
    parser.add_argument("--tier", default=None, help="Process only a specific tier (e.g., raw_1m)")
    parser.add_argument("--month", default=None, help="Process a specific month partition (e.g., 2025-08)")
    parser.add_argument("--status", action="store_true", help="Display overview of tiers and partitions")
    parser.add_argument("--dry-run", action="store_true", help="Simulate run without writing archives or purging")
    parser.add_argument("--force-purge", action="store_true", help="Force purge even if enable_purge is false in config")
    parser.add_argument("--skip-purge", action="store_true", help="Export and verify archives but do NOT purge database")
    parser.add_argument("--verify-only", action="store_true", help="Check existing archive files and checksums")
    parser.add_argument("--restore", default=None, help="Disaster Recovery: Path to .jsonl.gz archive file to restore into VM")
    parser.add_argument("--daemon", action="store_true", help="Run continuously, waking up on the configured interval")
    parser.add_argument("--once", action="store_true", help="Run one cycle instead of the root launcher's service loop")
    parser.add_argument("--interval-hours", type=int, default=int(os.getenv("ARCHIVE_INTERVAL_HOURS", "24")), help="Hours between archive cycles")

    args = parser.parse_args()
    # Maintenance operations must finish instead of unexpectedly looping forever.
    if service_default and not (args.once or args.status or args.restore or args.dry_run or args.verify_only or args.month):
        args.daemon = True

    manager = BESSArchiveManager(
        config_path=Path(args.config),
        storage_path=Path(args.storage),
        vm_url=args.vm_url,
        webhook_url=webhook_url
    )

    if args.interval_hours <= 0:
        parser.error("--interval-hours must be positive")

    # 1. Restore Mode
    if args.restore:
        ok = manager.vm_client.restore_archive(Path(args.restore))
        sys.exit(0 if ok else 1)

    # 2. Status Mode
    if args.status:
        manager.display_status()
        sys.exit(0)

    # 3. Standard Execution
    print("\n" + "=" * 66)
    print("      BESS HISTORIAN RETENTION & ARCHIVE MANAGER (VICTORIAMETRICS)")
    print("=" * 66)
    print(f"VictoriaMetrics URL : {manager.vm_client.base_url}")
    print(f"Config File         : {manager.config.config_path}")
    print(f"Storage Directory   : {manager.storage_path}")
    print(f"Daemon Mode         : {args.daemon}")
    print(f"Dry Run Mode        : {args.dry_run}")
    print(f"Verify Only Mode    : {args.verify_only}")
    print("=" * 66 + "\n")

    if args.daemon:
        logger.info(f"Starting BESS Archive Manager in DAEMON mode (Interval: {args.interval_hours}h)...")
        retry_seconds = 30
        try:
            while True:
                logger.info("Executing scheduled retention & archive cycle...")
                success = False
                try:
                    success = manager.run_all(
                        specific_tier=args.tier,
                        specific_month=args.month,
                        dry_run=args.dry_run,
                        force_purge=args.force_purge,
                        skip_purge=args.skip_purge,
                        verify_only=args.verify_only
                    )
                    if not success:
                        logger.error("Archive cycle failed; retrying without waiting for the daily schedule")
                except Exception:
                    # A temporary VM outage must not permanently stop the service.
                    logger.exception("Archive cycle failed; a short retry will rerun the cycle")
                if success:
                    # Only a successful complete cycle advances the normal schedule.
                    sleep_seconds = args.interval_hours * 3600
                    retry_seconds = 30
                    logger.info("Cycle succeeded. Next scheduled cycle in %s hours", args.interval_hours)
                else:
                    # 30s -> 60s -> 120s; keep trying at 120s until recovery.
                    sleep_seconds = retry_seconds
                    retry_seconds = min(retry_seconds * 2, 120)
                    logger.warning("Retrying archive cycle in %s seconds", sleep_seconds)
                wait_for_policy_change(manager, sleep_seconds)
        except KeyboardInterrupt:
            logger.info("Daemon mode stopped by user.")
            sys.exit(0)
    else:
        success = manager.run_all(
            specific_tier=args.tier,
            specific_month=args.month,
            dry_run=args.dry_run,
            force_purge=args.force_purge,
            skip_purge=args.skip_purge,
            verify_only=args.verify_only
        )
        exit_code = 0 if success else 1
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
