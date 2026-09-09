#!/usr/bin/env python3
import base64, json, re, urllib.parse, urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta

SOURCES_FILE = Path("proxy_sources.txt")
OUTPUT_FILE = Path("proxies.txt")
MAX_BYTES = 20 * 1024 * 1024

URI_RE = re.compile(
    r"(?i)\b(?:vless|vmess|trojan|ss|ssr|hysteria2?|hy2|tuic|anytls|socks5?|http)://[^\s<>\"']+"
)

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 kafka-sub-proxy-parser/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("source is larger than 20 MiB")
    return data.decode("utf-8", errors="replace")

def extract_uris(text, depth=0):
    if depth > 3:
        return []
    found = [x.rstrip(".,;:)]}>\"'") for x in URI_RE.findall(text)]
    compact = re.sub(r"\s+", "", text)
    if depth < 3 and len(compact) >= 16 and re.fullmatch(r"[A-Za-z0-9+/=_-]+", compact):
        try:
            decoded = base64.urlsafe_b64decode(compact + "=" * (-len(compact) % 4)).decode("utf-8", errors="replace")
            if decoded != text:
                found.extend(extract_uris(decoded, depth + 1))
        except Exception:
            pass
    if depth < 2:
        try:
            found.extend(extract_uris(json.dumps(json.loads(text), ensure_ascii=False), depth + 1))
        except Exception:
            pass
    return found

RUSSIAN_SNI_SUFFIXES = (".ru", ".рф")
RUSSIAN_SNI_DOMAINS = {
    "vk.com", "vk.ru", "ok.ru", "ya.ru", "yandex.ru", "mail.ru",
    "dzen.ru", "rutube.ru", "sber.ru", "tbank.ru", "tinkoff.ru",
    "ozon.ru", "wildberries.ru", "gosuslugi.ru", "mos.ru", "nalog.ru",
    "government.ru", "kremlin.ru", "mil.ru", "mvd.ru", "rbc.ru",
    "ria.ru", "lenta.ru", "rambler.ru", "habr.com", "avito.ru",
}

def _clean_hostname(value):
    value = urllib.parse.unquote(str(value or "")).strip().lower().rstrip(".")
    if "://" in value:
        try:
            value = urllib.parse.urlsplit(value).hostname or value
        except Exception:
            pass
    return value.strip("[]")

def _is_russian_sni(host):
    host = _clean_hostname(host)
    return bool(host) and (
        host in RUSSIAN_SNI_DOMAINS
        or host.endswith(RUSSIAN_SNI_SUFFIXES)
    )

def _sni_from_uri(uri):
    """Return TLS SNI/server-name from a supported proxy URI when present."""
    raw = uri.split("#", 1)[0]
    try:
        parsed = urllib.parse.urlsplit(raw)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        for key in ("sni", "serverName", "server_name", "servername", "peer"):
            values = query.get(key)
            if values and values[0].strip():
                return values[0].strip()

        if parsed.scheme.lower() == "vmess":
            payload = parsed.netloc + parsed.path
            payload = urllib.parse.unquote(payload).strip()
            decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)).decode("utf-8", errors="replace")
            obj = json.loads(decoded)
            if isinstance(obj, dict):
                tls = str(obj.get("tls") or "").lower()
                for key in ("sni", "serverName", "servername"):
                    if obj.get(key):
                        return str(obj[key]).strip()
                if tls and obj.get("host"):
                    return str(obj["host"]).strip()
    except Exception:
        pass
    return ""

def is_whitelist_proxy(uri):
    return _is_russian_sni(_sni_from_uri(uri))

def is_mlkem_encryption_proxy(uri):
    """Return True for VLESS/VMess configs using VLESS ML-KEM encryption.

    Karing may fail to initialize these nodes with errors such as
    `mlkem: invalid polynomial encoding`, so they are excluded from the
    generated proxy list. This targets the VLESS encryption parameter
    `mlkem768x25519plus...`, not ordinary REALITY/ML-KEM key-exchange fields.
    """
    raw = uri.split("#", 1)[0]
    try:
        parsed = urllib.parse.urlsplit(raw)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        for key, values in query.items():
            if key.lower() == "encryption":
                for value in values:
                    value = urllib.parse.unquote(str(value or "")).strip().lower()
                    if value.startswith("mlkem768x25519plus"):
                        return True

        if parsed.scheme.lower() == "vmess":
            payload = parsed.netloc + parsed.path
            payload = urllib.parse.unquote(payload).strip()
            decoded = base64.urlsafe_b64decode(
                payload + "=" * (-len(payload) % 4)
            ).decode("utf-8", errors="replace")
            obj = json.loads(decoded)
            if isinstance(obj, dict):
                value = str(obj.get("encryption") or "").strip().lower()
                if value.startswith("mlkem768x25519plus"):
                    return True
    except Exception:
        pass
    return False

def rename_uri(uri, number):
    label = f"🇨🇾 Cyprus | Кипр #{number}"
    return uri.split("#", 1)[0] + "#" + urllib.parse.quote(label, safe="")

def main():
    if not SOURCES_FILE.exists():
        raise SystemExit("proxy_sources.txt not found")
    sources = [
        x.strip() for x in SOURCES_FILE.read_text(encoding="utf-8").splitlines()
        if x.strip() and not x.lstrip().startswith("#")
    ]
    unique, seen = [], set()
    mlkem_skipped = 0
    for source in sources:
        try:
            items = extract_uris(fetch(source))
            added = 0
            source_mlkem = 0
            for uri in items:
                key = uri.split("#", 1)[0]
                if key in seen:
                    continue
                seen.add(key)

                if is_mlkem_encryption_proxy(uri):
                    mlkem_skipped += 1
                    source_mlkem += 1
                    continue

                unique.append(uri)
                added += 1
            if source_mlkem:
                print(f"[SOURCE] {source} -> {added} new proxy URLs (skipped {source_mlkem} ML-KEM)")
            else:
                print(f"[SOURCE] {source} -> {added} new proxy URLs")
        except Exception as e:
            print(f"[SOURCE ERROR] {source}: {e}")

    print(f"[FILTER] skipped {mlkem_skipped} ML-KEM encryption proxies")

    # Configs whose TLS SNI points to a Russian domain are treated as
    # whitelist configs and are placed first. Numbering is assigned only
    # after this ordering so the first whitelist config is always #1.
    whitelist = [uri for uri in unique if is_whitelist_proxy(uri)]
    regular = [uri for uri in unique if not is_whitelist_proxy(uri)]
    ordered = whitelist + regular

    print(f"[WHITELIST] {len(whitelist)} configs with Russian SNI")
    output = [rename_uri(uri, index) for index, uri in enumerate(ordered, start=1)]

    updated = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M")

    header = (
        "#subscription-userinfo: upload=1073741824000; download=0; total=1073741824000; expire=2524608000\n"
        "#profile-title: rudy and kafka\n"
        "#profile-update-interval: 1\n"
        f"#announce: авторы @rudy_bd @kafka_def // обновлено: {updated} | конфигов: {len(output)}"
    )

    result = header + "\n" + "\n".join(output)

    tmp = OUTPUT_FILE.with_suffix(".tmp")
    tmp.write_text(result + ("\n" if output else ""), encoding="utf-8")
    tmp.replace(OUTPUT_FILE)
    print(f"[TOTAL] {len(output)} unique proxy URLs")
    print(f"[DONE] wrote {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
