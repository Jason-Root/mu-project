from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
from importlib.metadata import PackageNotFoundError, version as package_version
import urllib.error
import urllib.parse
import urllib.request
import webbrowser

from packaging.version import InvalidVersion, Version

from mu_unscramble_bot import __version__
from mu_unscramble_bot.paths import is_frozen


@dataclass(frozen=True, slots=True)
class UpdateCheckResult:
    current_version: str
    latest_version: str = ""
    available: bool = False
    release_url: str = ""
    notes: str = ""
    asset_name: str = ""
    asset_url: str = ""
    error: str = ""


def get_app_version() -> str:
    try:
        return package_version("mu-unscramble-bot")
    except PackageNotFoundError:
        return __version__


def check_for_updates(repository: str, *, timeout_seconds: float = 8.0) -> UpdateCheckResult:
    current = get_app_version()
    repository = repository.strip().strip("/")
    if not repository:
        return UpdateCheckResult(
            current_version=current,
            error="Update repository is not configured yet.",
        )

    url = f"https://api.github.com/repos/{repository}/releases/latest"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "mu-unscramble-bot",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return UpdateCheckResult(
            current_version=current,
            error=f"GitHub update check failed: HTTP {exc.code}",
        )
    except Exception as exc:
        return UpdateCheckResult(
            current_version=current,
            error=f"GitHub update check failed: {type(exc).__name__}: {exc}",
        )

    latest = str(payload.get("tag_name", "") or payload.get("name", "") or "").strip()
    latest = latest.removeprefix("v")
    current_key = _safe_version(current)
    latest_key = _safe_version(latest)
    available = bool(latest and latest_key is not None and current_key is not None and latest_key > current_key)
    asset_name, asset_url = _pick_release_asset(payload)
    return UpdateCheckResult(
        current_version=current,
        latest_version=latest,
        available=available,
        release_url=str(payload.get("html_url", "") or ""),
        notes=str(payload.get("body", "") or "").strip(),
        asset_name=asset_name,
        asset_url=asset_url,
        error="" if latest else "No release version was returned from GitHub.",
    )


def open_release_page(url: str) -> None:
    if not url.strip():
        return
    webbrowser.open(url.strip())


def download_release_asset(
    result: UpdateCheckResult,
    *,
    destination_dir: str | Path | None = None,
    timeout_seconds: float = 45.0,
) -> Path:
    asset_url = result.asset_url.strip()
    if not asset_url:
        raise RuntimeError("This release does not expose a downloadable Windows package yet.")

    if destination_dir is None:
        destination_root = Path(tempfile.mkdtemp(prefix="mu-unscramble-update-"))
    else:
        destination_root = Path(destination_dir)
        destination_root.mkdir(parents=True, exist_ok=True)

    file_name = result.asset_name.strip() or Path(urllib.parse.urlparse(asset_url).path).name or "update.zip"
    destination = destination_root / file_name
    request = urllib.request.Request(
        asset_url,
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": "mu-unscramble-bot",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response, destination.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 256)
            if not chunk:
                break
            handle.write(chunk)
    return destination


def stage_windows_update(zip_path: str | Path) -> None:
    if not is_frozen():
        raise RuntimeError("Automatic update install only works from the packaged app build.")

    zip_path = Path(zip_path).resolve()
    executable = Path(sys.executable).resolve()
    install_root = executable.parent.parent
    relaunch_target = install_root / "Start MU Unscramble Bot.vbs"
    if not relaunch_target.exists():
        relaunch_target = executable

    stage_root = Path(tempfile.mkdtemp(prefix="mu-unscramble-apply-"))
    script_path = stage_root / "apply_update.ps1"
    script_path.write_text(
        _build_apply_update_script(
            current_pid=os.getpid(),
            zip_path=zip_path,
            install_root=install_root,
            relaunch_target=relaunch_target,
        ),
        encoding="utf-8",
    )

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    subprocess.Popen(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-WindowStyle",
            "Hidden",
            "-File",
            str(script_path),
        ],
        creationflags=creationflags,
    )


def _pick_release_asset(payload: dict[str, object]) -> tuple[str, str]:
    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        return "", ""

    preferred: tuple[str, str] = ("", "")
    fallback: tuple[str, str] = ("", "")
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = str(asset.get("name", "") or "").strip()
        url = str(asset.get("browser_download_url", "") or "").strip()
        if not name or not url:
            continue
        lowered = name.lower()
        if lowered.endswith(".zip") and "win64" in lowered:
            return name, url
        if lowered.endswith(".zip") and not fallback[0]:
            fallback = (name, url)
        if not preferred[0]:
            preferred = (name, url)
    return fallback if fallback[0] else preferred


def _build_apply_update_script(
    *,
    current_pid: int,
    zip_path: Path,
    install_root: Path,
    relaunch_target: Path,
) -> str:
    zip_literal = str(zip_path)
    install_root_literal = str(install_root)
    relaunch_literal = str(relaunch_target)
    return textwrap.dedent(
        f"""
        $ErrorActionPreference = "Stop"
        $targetPid = {current_pid}
        $zipPath = "{zip_literal}"
        $installRoot = "{install_root_literal}"
        $relaunchPath = "{relaunch_literal}"
        $stageRoot = Split-Path -Parent $PSCommandPath
        $extractRoot = Join-Path $stageRoot "extracted"
        $bundleSource = Join-Path $extractRoot "MU Unscramble Bot"
        $bundleTarget = Join-Path $installRoot "MU Unscramble Bot"
        $launcherSource = Join-Path $extractRoot "Start MU Unscramble Bot.vbs"
        $launcherTarget = Join-Path $installRoot "Start MU Unscramble Bot.vbs"

        while (Get-Process -Id $targetPid -ErrorAction SilentlyContinue) {{
            Start-Sleep -Milliseconds 500
        }}
        Start-Sleep -Milliseconds 700

        if (Test-Path -LiteralPath $extractRoot) {{
            Remove-Item -LiteralPath $extractRoot -Recurse -Force
        }}
        New-Item -ItemType Directory -Path $extractRoot | Out-Null
        Expand-Archive -LiteralPath $zipPath -DestinationPath $extractRoot -Force

        if (Test-Path -LiteralPath $bundleTarget) {{
            Remove-Item -LiteralPath $bundleTarget -Recurse -Force
        }}
        Copy-Item -LiteralPath $bundleSource -Destination $bundleTarget -Recurse -Force

        if (Test-Path -LiteralPath $launcherSource) {{
            Copy-Item -LiteralPath $launcherSource -Destination $launcherTarget -Force
        }}

        if (Test-Path -LiteralPath $zipPath) {{
            Remove-Item -LiteralPath $zipPath -Force
        }}
        if (Test-Path -LiteralPath $extractRoot) {{
            Remove-Item -LiteralPath $extractRoot -Recurse -Force
        }}

        Start-Process -FilePath $relaunchPath
        Start-Sleep -Seconds 1
        Remove-Item -LiteralPath $PSCommandPath -Force
        """
    ).strip()


def _safe_version(value: str) -> Version | None:
    try:
        return Version(value)
    except InvalidVersion:
        return None
