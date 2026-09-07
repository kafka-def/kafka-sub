#!/usr/bin/env python3
import base64, json, re, urllib.parse, urllib.request
from pathlib import Path
from datetime import datetime

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

def rename_uri(uri):
    label = "🇨🇾 Cyprus | Кипр"
    return uri.split("#", 1)[0] + "#" + urllib.parse.quote(label, safe="")

def main():
    if not SOURCES_FILE.exists():
        raise SystemExit("proxy_sources.txt not found")
    sources = [
        x.strip() for x in SOURCES_FILE.read_text(encoding="utf-8").splitlines()
        if x.strip() and not x.lstrip().startswith("#")
    ]
    unique, seen = [], set()
    for source in sources:
        try:
            items = extract_uris(fetch(source))
            added = 0
            for uri in items:
                key = uri.split("#", 1)[0]
                if key not in seen:
                    seen.add(key)
                    unique.append(uri)
                    added += 1
            print(f"[SOURCE] {source} -> {added} new proxy URLs")
        except Exception as e:
            print(f"[SOURCE ERROR] {source}: {e}")
    output = [rename_uri(uri) for uri in unique]

    updated = datetime.now().strftime("%d.%m.%Y %H:%M")

    header = (
        "#subscription-userinfo: upload=1073741824000; download=0; total=1073741824000; expire=2524608000\\n"
        "#profile-title: rudy and kafka\\n"
        "#profile-update-interval: 1\\n"
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
