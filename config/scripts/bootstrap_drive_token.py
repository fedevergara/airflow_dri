#!/usr/bin/env python3
"""Bootstrap and validate OAuth token for Google Drive/Sheets access.

Usage examples:
  python bootstrap_drive_token.py \
    --token-path /opt/airflow/config/secrets/agreements_token.pickle \
    --client-secrets /opt/airflow/config/secrets/credentials.json \
    --file-id TU_DRIVE_FILE_ID

  # If browser callback is not possible in your environment:
  python bootstrap_drive_token.py --use-console ...
"""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Keep both scopes so one token can serve Drive file download + Sheets API reads.
DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def _normalize_scopes(scopes: list[str] | None) -> set[str]:
    return {scope.strip() for scope in (scopes or []) if scope and scope.strip()}


def _has_required_scopes(creds: Credentials | None, required_scopes: list[str]) -> bool:
    if not creds:
        return False
    granted = _normalize_scopes(getattr(creds, "scopes", None) or getattr(creds, "granted_scopes", None))
    return set(required_scopes).issubset(granted)


def _load_token(token_path: Path) -> Credentials | None:
    if not token_path.exists():
        return None
    with token_path.open("rb") as token_file:
        loaded = pickle.load(token_file)
    if not isinstance(loaded, Credentials):
        raise TypeError(f"Token at '{token_path}' is not google.oauth2.credentials.Credentials")
    return loaded


def _save_token(token_path: Path, creds: Credentials) -> None:
    token_path.parent.mkdir(parents=True, exist_ok=True)
    with token_path.open("wb") as token_file:
        pickle.dump(creds, token_file)


def _authorize_once(
    *,
    client_secrets: Path,
    scopes: list[str],
    use_console: bool,
) -> Credentials:
    flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets), scopes)
    if use_console:
        # Required for manual copy/paste flow; browser will redirect to localhost with ?code=...
        flow.redirect_uri = "http://localhost"
        auth_url, _ = flow.authorization_url(
            access_type="offline",
            prompt="consent",
            include_granted_scopes="true",
        )
        print("Open this URL in your browser and authorize access:")
        print(auth_url)
        print("After Google redirects to localhost, copy the `code` parameter from the URL bar.")
        code = input("Paste the authorization code here: ").strip()
        flow.fetch_token(code=code)
        return flow.credentials
    return flow.run_local_server(port=0)


def ensure_oauth_token(
    *,
    token_path: Path,
    client_secrets: Path | None,
    scopes: list[str],
    use_console: bool,
) -> Credentials:
    creds = _load_token(token_path)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_token(token_path, creds)

    if creds and creds.valid and _has_required_scopes(creds, scopes):
        return creds

    if client_secrets is None:
        raise RuntimeError(
            "Token missing/invalid/insufficient scopes. Provide --client-secrets for one-time OAuth consent."
        )
    if not client_secrets.exists():
        raise FileNotFoundError(f"Client secrets file not found: {client_secrets}")

    creds = _authorize_once(
        client_secrets=client_secrets,
        scopes=scopes,
        use_console=use_console,
    )
    _save_token(token_path, creds)
    return creds


def print_drive_file_info(file_id: str, creds: Credentials) -> None:
    drive = build("drive", "v3", credentials=creds)
    info = drive.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    print("Drive file:")
    print(f"- id: {info.get('id')}")
    print(f"- name: {info.get('name')}")
    print(f"- mimeType: {info.get('mimeType')}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap Google OAuth token for Drive/Sheets.")
    parser.add_argument("--token-path", required=True, help="Path to token.pickle to read/write.")
    parser.add_argument(
        "--client-secrets",
        default="",
        help="Path to OAuth client secrets JSON (needed first time or when scopes change).",
    )
    parser.add_argument(
        "--scopes",
        nargs="*",
        default=DEFAULT_SCOPES,
        help="OAuth scopes (default: drive.readonly + spreadsheets.readonly).",
    )
    parser.add_argument(
        "--use-console",
        action="store_true",
        help="Use console-based OAuth flow instead of local webserver callback.",
    )
    parser.add_argument(
        "--file-id",
        default="",
        help="Optional Drive file ID to validate access after token bootstrap.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    token_path = Path(args.token_path).expanduser()
    client_secrets = Path(args.client_secrets).expanduser() if args.client_secrets else None

    creds = ensure_oauth_token(
        token_path=token_path,
        client_secrets=client_secrets,
        scopes=list(args.scopes),
        use_console=bool(args.use_console),
    )
    print(f"OAuth token ready at: {token_path}")
    print(f"Token valid: {creds.valid}")
    print(f"Scopes: {list(_normalize_scopes(getattr(creds, 'scopes', None) or getattr(creds, 'granted_scopes', None)))}")

    if args.file_id:
        print_drive_file_info(args.file_id, creds)


if __name__ == "__main__":
    main()
