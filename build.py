from pathlib import Path

code = r'''#!/usr/bin/env python3
import base64
import concurrent.futures
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

import yaml


# ============================================================
# CONFIG
# ============================================================

TEST_URLS = [
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/",
    "https://www.google.com/generate_204",
]

MAX_LATENCY_MS = 5000

# Each URL is tested this many times.
CHECK_ATTEMPTS = 3

# A URL is considered successful for a node when this many
# attempts succeed.
REQUIRED_SUCCESSES_PER_URL = 2

# A node must successfully answer at least this many different
# test URLs.
REQUIRED_GOOD_URLS = 2

MAX_WORKERS = 10
REQUEST_TIMEOUT_MS = 5000
STARTUP_TIMEOUT = 20

# Do not replace the public config if the result looks suspicious.
MIN_ALIVE_PERCENT = 0.05
MIN_ALIVE_ABSOLUTE = 10

MIHOMO_PATH = "./mihomo"
OUTPUT_CONFIG = "config.yaml"
SOURCES_FILE = "sources.txt"

COUNTRY_NAMES = [
    ("🇩🇪 Germany", ["germany", "de", "ger", "frankfurt", "berlin", "munich"]),
    ("🇳🇱 Netherlands", ["netherlands", "nl", "amsterdam", "rotterdam"]),
    ("🇫🇷 France", ["france", "fr", "paris"]),
    ("🇬🇧 United Kingdom", ["uk", "united kingdom", "england", "london"]),
    ("🇺🇸 United States", ["united states", "usa", "us", "america", "new york", "los angeles"]),
    ("🇨🇦 Canada", ["canada", "ca", "toronto", "montreal"]),
    ("🇫🇮 Finland", ["finland", "fi", "helsinki"]),
    ("🇸🇪 Sweden", ["sweden", "se", "stockholm"]),
    ("🇳🇴 Norway", ["norway", "no", "oslo"]),
    ("🇩🇰 Denmark", ["denmark", "dk", "copenhagen"]),
    ("🇵🇱 Poland", ["poland", "pl", "warsaw", "gdansk", "krakow"]),
    ("🇨🇿 Czech Republic", ["czech", "czechia", "cz", "prague"]),
    ("🇦🇹 Austria", ["austria", "at", "vienna"]),
    ("🇨🇭 Switzerland", ["switzerland", "ch", "zurich"]),
    ("🇪🇸 Spain", ["spain", "es", "madrid", "barcelona"]),
    ("🇮🇹 Italy", ["italy", "it", "rome", "milan"]),
    ("🇵🇹 Portugal", ["portugal", "pt", "lisbon"]),
    ("🇷🇴 Romania", ["romania", "ro", "bucharest"]),
    ("🇧🇬 Bulgaria", ["bulgaria", "bg", "sofia"]),
    ("🇹🇷 Turkey", ["turkey", "tr", "istanbul"]),
    ("🇷🇺 Russia", ["russia", "ru", "moscow", "spb", "petersburg"]),
    ("🇺🇦 Ukraine", ["ukraine", "ua", "kyiv"]),
    ("🇯🇵 Japan", ["japan", "jp", "tokyo", "osaka"]),
    ("🇸🇬 Singapore", ["singapore", "sg"]),
    ("🇭🇰 Hong Kong", ["hong kong", "hk"]),
    ("🇹🇼 Taiwan", ["taiwan", "tw"]),
    ("🇰🇷 South Korea", ["korea", "kr", "seoul"]),
    ("🇮🇳 India", ["india", "in", "mumbai", "delhi"]),
    ("🇮🇱 Israel", ["israel", "il", "tel aviv"]),
    ("🇦🇪 UAE", ["uae", "emirates", "dubai"]),
    ("🇧🇷 Brazil", ["brazil", "br", "sao paulo"]),
    ("🇦🇷 Argentina", ["argentina", "ar", "buenos aires"]),
    ("🇲🇽 Mexico", ["mexico", "mx"]),
    ("🇦🇺 Australia", ["australia", "au", "sydney", "melbourne"]),
]

VLESS_RE = re.compile(r"vless://[^\s\"'<>]+")

BOOL_TRUE = {"1", "true", "yes", "on"}
BOOL_FALSE = {"0", "false", "no", "off"}


# ============================================================
# UTILS
# ============================================================

def log(message):
    print(message, flush=True)


def parse_bool(value, default=None):
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()

    if text in BOOL_TRUE:
        return True
    if text in BOOL_FALSE:
        return False

    return default


def first_value(query, *keys):
    for key in keys:
        values = query.get(key)
        if values:
            return values[0]
    return None


def all_values(query, *keys):
    result = []
    for key in keys:
        result.extend(query.get(key, []))
    return result


def split_csv(value):
    if value is None:
        return []

    if isinstance(value, list):
        result = []
        for item in value:
            result.extend(split_csv(item))
        return result

    return [x.strip() for x in str(value).split(",") if x.strip()]


def decode_repeated(value, max_rounds=4):
    if value is None:
        return None

    current = str(value)

    for _ in range(max_rounds):
        try:
            decoded = urllib.parse.unquote(current)
        except Exception:
            break

        if decoded == current:
            break

        current = decoded

    return current


def decode_json_value(value):
    if value is None:
        return None

    candidates = [str(value)]

    current = str(value)
    for _ in range(4):
        current = urllib.parse.unquote(current)
        if current not in candidates:
            candidates.append(current)

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except Exception:
            pass

    return None


def safe_name(value):
    value = str(value or "").strip()
    value = re.sub(r"\s+", " ", value)
    value = value.replace("\x00", "")
    return value[:180]


def unique_name(base, used):
    base = safe_name(base) or "VLESS"

    if base not in used:
        used.add(base)
        return base

    i = 2
    while f"{base} {i}" in used:
        i += 1

    name = f"{base} {i}"
    used.add(name)
    return name


def country_from_text(text):
    value = str(text or "").lower()

    # Avoid the old dangerous behaviour where "us" matched
    # arbitrary substrings inside hostnames.
    tokens = set(re.findall(r"[a-z]{2,}", value))

    for country, keywords in COUNTRY_NAMES:
        for keyword in keywords:
            keyword = keyword.lower()

            if " " in keyword:
                if keyword in value:
                    return country
            elif len(keyword) <= 2:
                if keyword in tokens:
                    return country
            elif keyword in value:
                return country

    return "🌐 Unknown"


def clean_vless_uri(uri):
    uri = uri.strip().strip("'\"<>")

    while uri.endswith((".", ",", ";", ")")):
        uri = uri[:-1]

    return uri


# ============================================================
# DOWNLOAD / SOURCE PARSING
# ============================================================

def http_get(url, timeout=30, max_bytes=20 * 1024 * 1024):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "kafka-sub-builder/2.0",
            "Accept": "*/*",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        chunks = []
        total = 0

        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break

            total += len(chunk)

            if total > max_bytes:
                raise ValueError(f"source is larger than {max_bytes} bytes")

            chunks.append(chunk)

    return b"".join(chunks)


def maybe_decode_base64(text):
    stripped = re.sub(r"\s+", "", text)

    if not stripped:
        return text

    # Plain VLESS content should not be decoded.
    if "vless://" in text.lower():
        return text

    candidates = [
        stripped,
        stripped + "=" * (-len(stripped) % 4),
    ]

    for candidate in candidates:
        for decoder in (base64.b64decode, base64.urlsafe_b64decode):
            try:
                raw = decoder(candidate, validate=False)
                decoded = raw.decode("utf-8", errors="ignore")

                if "vless://" in decoded.lower():
                    return decoded
            except Exception:
                pass

    return text


def extract_vless_from_object(obj, result):
    if isinstance(obj, str):
        result.extend(VLESS_RE.findall(obj))
        return

    if isinstance(obj, list):
        for item in obj:
            extract_vless_from_object(item, result)
        return

    if isinstance(obj, dict):
        for value in obj.values():
            extract_vless_from_object(value, result)


def xray_vless_to_uri(outbound):
    """
    Convert common Xray JSON VLESS outbound objects into a VLESS URI.
    This is deliberately conservative: unsupported structures are skipped
    instead of being silently downgraded.
    """

    if not isinstance(outbound, dict):
        return None

    if str(outbound.get("protocol", "")).lower() != "vless":
        return None

    settings = outbound.get("settings") or {}
    vnext = settings.get("vnext") or []

    if not vnext:
        return None

    server = vnext[0]
    if not isinstance(server, dict):
        return None

    address = server.get("address")
    port = server.get("port")

    users = server.get("users") or []
    if not address or not port or not users:
        return None

    user = users[0]
    uuid = user.get("id")

    if not uuid:
        return None

    query = {}

    encryption = user.get("encryption")
    if encryption:
        query["encryption"] = encryption

    flow = user.get("flow")
    if flow:
        query["flow"] = flow

    stream = outbound.get("streamSettings") or {}
    network = stream.get("network")
    security = stream.get("security")

    if network:
        query["type"] = network

    if security:
        query["security"] = security

    tls = stream.get("tlsSettings") or {}

    if tls:
        if tls.get("serverName"):
            query["sni"] = tls["serverName"]

        if tls.get("fingerprint"):
            query["fp"] = tls["fingerprint"]

        if tls.get("alpn"):
            query["alpn"] = ",".join(map(str, tls["alpn"]))

        if tls.get("allowInsecure") is not None:
            query["allowInsecure"] = "1" if tls["allowInsecure"] else "0"

    reality = stream.get("realitySettings") or {}

    if reality:
        if reality.get("serverName"):
            query["sni"] = reality["serverName"]

        if reality.get("fingerprint"):
            query["fp"] = reality["fingerprint"]

        if reality.get("publicKey"):
            query["pbk"] = reality["publicKey"]

        if reality.get("shortId"):
            query["sid"] = reality["shortId"]

        if reality.get("spiderX"):
            query["spx"] = reality["spiderX"]

    ws = stream.get("wsSettings") or {}
    if ws:
        if ws.get("path") is not None:
            query["path"] = ws["path"]

        headers = ws.get("headers") or {}
        if headers.get("Host"):
            query["host"] = headers["Host"]

    grpc = stream.get("grpcSettings") or {}
    if grpc:
        if grpc.get("serviceName"):
            query["serviceName"] = grpc["serviceName"]

        if grpc.get("authority"):
            query["authority"] = grpc["authority"]

        if grpc.get("multiMode") is True:
            query["mode"] = "multi"

    xhttp = stream.get("xhttpSettings") or stream.get("splithttpSettings") or {}
    if xhttp:
        for source_key, query_key in (
            ("path", "path"),
            ("host", "host"),
            ("mode", "mode"),
        ):
            if xhttp.get(source_key) is not None:
                query[query_key] = xhttp[source_key]

        extra = {}
        for key, value in xhttp.items():
            if key not in {"path", "host", "mode"}:
                extra[key] = value

        if extra:
            query["extra"] = json.dumps(extra, separators=(",", ":"))

    fragment = outbound.get("tag") or ""

    query_string = urllib.parse.urlencode(query, doseq=True)

    uri = f"vless://{urllib.parse.quote(str(uuid), safe='')}@{address}:{port}"

    if query_string:
        uri += "?" + query_string

    if fragment:
        uri += "#" + urllib.parse.quote(str(fragment), safe="")

    return uri


def extract_sources_from_text(text):
    result = []

    # Direct URI extraction.
    result.extend(VLESS_RE.findall(text))

    # JSON/YAML.
    try:
        parsed = json.loads(text)
        extract_vless_from_object(parsed, result)

        if isinstance(parsed, dict):
            outbounds = parsed.get("outbounds") or []
            for outbound in outbounds:
                uri = xray_vless_to_uri(outbound)
                if uri:
                    result.append(uri)
    except Exception:
        pass

    try:
        parsed = yaml.safe_load(text)
        extract_vless_from_object(parsed, result)
    except Exception:
        pass

    # Base64 subscription.
    decoded = maybe_decode_base64(text)

    if decoded != text:
        result.extend(VLESS_RE.findall(decoded))

        try:
            parsed = json.loads(decoded)
            extract_vless_from_object(parsed, result)

            if isinstance(parsed, dict):
                for outbound in parsed.get("outbounds") or []:
                    uri = xray_vless_to_uri(outbound)
                    if uri:
                        result.append(uri)
        except Exception:
            pass

    # De-duplicate while preserving order.
    seen = set()
    final = []

    for item in result:
        item = clean_vless_uri(item)

        if item not in seen:
            seen.add(item)
            final.append(item)

    return final


def load_sources():
    if not os.path.exists(SOURCES_FILE):
        raise FileNotFoundError(f"{SOURCES_FILE} not found")

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = [
            line.strip()
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        ]

    if not sources:
        raise RuntimeError("sources.txt is empty")

    all_uris = []

    for source in sources:
        try:
            log(f"[SOURCE] {source}")

            data = http_get(source)
            text = data.decode("utf-8", errors="ignore")

            uris = extract_sources_from_text(text)

            log(f"         found {len(uris)} VLESS")

            all_uris.extend(uris)

        except Exception as e:
            log(f"         ERROR: {e}")

    return all_uris


# ============================================================
# VLESS PARSER
# ============================================================

def parse_vless(uri, source_name=""):
    try:
        parsed = urllib.parse.urlparse(uri)

        if parsed.scheme.lower() != "vless":
            return None

        if not parsed.hostname or not parsed.username:
            return None

        uuid = urllib.parse.unquote(parsed.username)

        try:
            port = parsed.port
        except ValueError:
            return None

        if not port:
            return None

        query = urllib.parse.parse_qs(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=False,
        )

        proxy = {
            "name": safe_name(
                urllib.parse.unquote(parsed.fragment)
                or parsed.hostname
            ),
            "type": "vless",
            "server": parsed.hostname,
            "port": int(port),
            "uuid": uuid,
            "udp": True,
        }

        encryption = first_value(query, "encryption")
        if encryption:
            proxy["encryption"] = decode_repeated(encryption)

        flow = first_value(query, "flow")
        if flow:
            proxy["flow"] = decode_repeated(flow)

        udp = first_value(query, "udp")
        if udp is not None:
            parsed_udp = parse_bool(udp)
            if parsed_udp is not None:
                proxy["udp"] = parsed_udp

        packet_encoding = first_value(
            query,
            "packet-encoding",
            "packetEncoding",
        )
        if packet_encoding:
            proxy["packet-encoding"] = decode_repeated(packet_encoding)

        security = (first_value(query, "security") or "").lower()
        network = (
            first_value(query, "type", "network")
            or "tcp"
        ).lower()

        # ----------------------------------------------------
        # TLS / REALITY
        # ----------------------------------------------------

        if security in {"tls", "reality"}:
            proxy["tls"] = True

        sni = first_value(query, "sni", "servername", "serverName")
        if sni:
            proxy["servername"] = decode_repeated(sni)

        fingerprint = first_value(
            query,
            "fp",
            "fingerprint",
            "client-fingerprint",
        )
        if fingerprint:
            proxy["client-fingerprint"] = decode_repeated(fingerprint)

        alpn = first_value(query, "alpn")
        if alpn:
            values = split_csv(decode_repeated(alpn))
            if values:
                proxy["alpn"] = values

        skip_cert = first_value(
            query,
            "allowInsecure",
            "allow-insecure",
            "skip-cert-verify",
            "skipCertVerify",
        )
        if skip_cert is not None:
            value = parse_bool(skip_cert)
            if value is not None:
                proxy["skip-cert-verify"] = value

        name_cert_verify = first_value(
            query,
            "nameCertVerify",
            "name-cert-verify",
        )
        if name_cert_verify is not None:
            value = parse_bool(name_cert_verify)
            if value is not None:
                proxy["name-cert-verify"] = value

        certificate = first_value(query, "certificate")
        if certificate:
            proxy["certificate"] = decode_repeated(certificate)

        private_key = first_value(query, "privateKey", "private-key")
        if private_key:
            proxy["private-key"] = decode_repeated(private_key)

        if security == "reality":
            public_key = first_value(query, "pbk", "publicKey", "public-key")
            short_id = first_value(query, "sid", "shortId", "short-id")
            spider_x = first_value(query, "spx", "spiderX", "spider-x")

            reality_opts = {}

            if public_key:
                reality_opts["public-key"] = decode_repeated(public_key)

            if short_id:
                reality_opts["short-id"] = decode_repeated(short_id)

            if spider_x:
                reality_opts["spider-x"] = decode_repeated(spider_x)

            if reality_opts:
                proxy["reality-opts"] = reality_opts

        # ----------------------------------------------------
        # TRANSPORT
        # ----------------------------------------------------

        supported_networks = {
            "tcp",
            "ws",
            "grpc",
            "xhttp",
            "splithttp",
            "httpupgrade",
            "h2",
        }

        if network not in supported_networks:
            return None

        if network == "splithttp":
            network = "xhttp"

        proxy["network"] = network

        # ---------------- TCP ----------------

        if network == "tcp":
            header_type = first_value(query, "headerType", "header-type")

            if header_type and header_type.lower() not in {"none", ""}:
                if header_type.lower() != "http":
                    return None

                path = first_value(query, "path")
                host = first_value(query, "host")

                header = {
                    "type": "http",
                }

                request = {}

                if path:
                    request["path"] = [decode_repeated(path)]

                if host:
                    request["headers"] = {
                        "Host": [decode_repeated(host)]
                    }

                if request:
                    header["request"] = request

                proxy["tcp-opts"] = {
                    "header": header
                }

        # ---------------- WebSocket ----------------

        elif network == "ws":
            ws_opts = {}

            path = first_value(query, "path")
            host = first_value(query, "host")

            if path:
                ws_opts["path"] = decode_repeated(path)

            if host:
                ws_opts["headers"] = {
                    "Host": decode_repeated(host)
                }

            ed = first_value(query, "ed", "max-early-data")
            eh = first_value(query, "eh", "early-data-header-name")

            if ed:
                try:
                    ws_opts["max-early-data"] = int(ed)
                except ValueError:
                    pass

            if eh:
                ws_opts["early-data-header-name"] = decode_repeated(eh)

            if ws_opts:
                proxy["ws-opts"] = ws_opts

        # ---------------- gRPC ----------------

        elif network == "grpc":
            grpc_opts = {}

            service_name = first_value(
                query,
                "serviceName",
                "service-name",
            )

            authority = first_value(query, "authority")

            mode = first_value(query, "mode", "grpc-mode")

            if service_name:
                grpc_opts["grpc-service-name"] = decode_repeated(service_name)

            if authority:
                grpc_opts["grpc-authority"] = decode_repeated(authority)

            if mode:
                grpc_opts["grpc-mode"] = decode_repeated(mode)

            if grpc_opts:
                proxy["grpc-opts"] = grpc_opts

        # ---------------- H2 ----------------

        elif network == "h2":
            h2_opts = {}

            path = first_value(query, "path")
            host = first_value(query, "host")

            if path:
                h2_opts["path"] = decode_repeated(path)

            if host:
                h2_opts["host"] = [
                    decode_repeated(x)
                    for x in split_csv(decode_repeated(host))
                ]

            if h2_opts:
                proxy["h2-opts"] = h2_opts

        # ---------------- HTTP Upgrade ----------------

        elif network == "httpupgrade":
            opts = {}

            path = first_value(query, "path")
            host = first_value(query, "host")

            if path:
                opts["path"] = decode_repeated(path)

            headers = {}
            if host:
                headers["Host"] = decode_repeated(host)

            if headers:
                opts["headers"] = headers

            if opts:
                proxy["httpupgrade-opts"] = opts

        # ---------------- XHTTP ----------------

        elif network == "xhttp":
            xhttp_opts = {}

            # Explicit top-level query fields.
            for source_key, target_key in (
                ("path", "path"),
                ("host", "host"),
                ("mode", "mode"),
            ):
                value = first_value(query, source_key)
                if value is not None:
                    xhttp_opts[target_key] = decode_repeated(value)

            # Xray's XHTTP `extra` is JSON and is frequently
            # URL-encoded one or more times.
            extra_raw = first_value(query, "extra")

            if extra_raw:
                extra = decode_json_value(extra_raw)

                if isinstance(extra, dict):
                    mappings = {
                        "headers": "headers",
                        "noGRPCHeader": "no-grpc-header",
                        "xPaddingBytes": "x-padding-bytes",
                        "xPaddingObfsMode": "x-padding-obfs-mode",
                        "xPaddingKey": "x-padding-key",
                        "xPaddingHeader": "x-padding-header",
                        "xPaddingPlacement": "x-padding-placement",
                        "xPaddingMethod": "x-padding-method",
                        "uplinkHttpMethod": "uplink-http-method",
                        "sessionPlacement": "session-placement",
                        "seqPlacement": "seq-placement",
                        "scMaxEachPostBytes": "sc-max-each-post-bytes",
                        "scMinPostsIntervalMs": "sc-min-posts-interval-ms",
                        "reuseSettings": "reuse-settings",
                        "downloadSettings": "download-settings",
                        "mode": "mode",
                        "path": "path",
                        "host": "host",
                    }

                    for source_key, target_key in mappings.items():
                        if source_key in extra:
                            xhttp_opts[target_key] = extra[source_key]

                    # Some Xray configs use snake/kebab-case already.
                    for key in (
                        "headers",
                        "no-grpc-header",
                        "x-padding-bytes",
                        "x-padding-obfs-mode",
                        "x-padding-key",
                        "x-padding-header",
                        "x-padding-placement",
                        "x-padding-method",
                        "uplink-http-method",
                        "session-placement",
                        "seq-placement",
                        "sc-max-each-post-bytes",
                        "sc-min-posts-interval-ms",
                        "reuse-settings",
                        "download-settings",
                    ):
                        if key in extra:
                            xhttp_opts[key] = extra[key]

            if xhttp_opts:
                proxy["xhttp-opts"] = xhttp_opts

        # ----------------------------------------------------
        # FINAL NORMALIZATION
        # ----------------------------------------------------

        # Remove fields that are empty or None.
        cleaned = {}

        for key, value in proxy.items():
            if value is None:
                continue

            if isinstance(value, str) and value == "":
                continue

            cleaned[key] = value

        return cleaned

    except Exception:
        return None


# ============================================================
# DEDUP / NAMES
# ============================================================

def proxy_identity(proxy):
    data = dict(proxy)

    data.pop("name", None)
    data.pop("_source_name", None)

    return json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def deduplicate(proxies):
    seen = set()
    result = []

    for proxy in proxies:
        identity = proxy_identity(proxy)

        if identity in seen:
            continue

        seen.add(identity)
        result.append(proxy)

    return result


def assign_names(proxies):
    used = set()

    # Stable ordering makes names deterministic between runs.
    proxies.sort(
        key=lambda p: (
            str(p.get("server", "")),
            int(p.get("port", 0)),
            str(p.get("uuid", "")),
        )
    )

    counters = {}

    for proxy in proxies:
        base = safe_name(proxy.get("name"))

        if not base or base == proxy.get("server"):
            country = country_from_text(
                f"{proxy.get('server', '')} {proxy.get('_source_name', '')}"
            )
            base = f"{country} {proxy.get('server')}"

        # Keep names readable but avoid collisions.
        if base not in counters:
            counters[base] = 1
        else:
            counters[base] += 1

        candidate = base

        if candidate in used:
            candidate = f"{base} {counters[base]}"

        proxy["name"] = unique_name(candidate, used)


# ============================================================
# MIHOMO CONFIG
# ============================================================

def build_config(proxies, external_controller=None):
    names = [p["name"] for p in proxies]

    group = {
        "name": "🚀 Freedom Rudy",
        "type": "select",
        "proxies": names + ["DIRECT"],
    }

    config = {
        "mixed-port": 7890,
        "mode": "rule",
        "proxies": proxies,
        "proxy-groups": [group],
        "rules": [
            "MATCH,🚀 Freedom Rudy"
        ],
    }

    if external_controller:
        config["external-controller"] = external_controller

    return config


def validate_structure(config):
    if not isinstance(config, dict):
        raise ValueError("config is not a mapping")

    proxies = config.get("proxies")

    if not isinstance(proxies, list):
        raise ValueError("proxies is not a list")

    if not proxies:
        raise ValueError("proxies is empty")

    for proxy in proxies:
        if not isinstance(proxy, dict):
            raise ValueError("proxy is not a mapping")

        if proxy.get("type") != "vless":
            raise ValueError("non-VLESS proxy found")

        for field in ("name", "server", "port", "uuid"):
            if not proxy.get(field):
                raise ValueError(f"proxy missing {field}")

    groups = config.get("proxy-groups")
    if not isinstance(groups, list) or not groups:
        raise ValueError("proxy-groups missing")

    return True


def mihomo_validate(config_path):
    if not os.path.exists(MIHOMO_PATH):
        raise FileNotFoundError(f"{MIHOMO_PATH} not found")

    log("[VALIDATE] mihomo -t")

    process = subprocess.run(
        [
            MIHOMO_PATH,
            "-t",
            "-f",
            config_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
    )

    output = process.stdout.strip()

    if output:
        print(output)

    if process.returncode != 0:
        raise RuntimeError(
            f"Mihomo rejected config with exit code {process.returncode}"
        )


# ============================================================
# HEALTH CHECK
# ============================================================

def wait_for_mihomo(controller, process):
    deadline = time.time() + STARTUP_TIMEOUT

    while time.time() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(
                "Mihomo exited during startup:\n" + output[-5000:]
            )

        try:
            request = urllib.request.Request(
                f"http://{controller}/version"
            )

            with urllib.request.urlopen(request, timeout=2) as response:
                if response.status == 200:
                    return
        except Exception:
            pass

        time.sleep(0.25)

    raise TimeoutError("Mihomo controller did not become ready")


def api_get(path, timeout=10):
    url = f"http://127.0.0.1:9090{path}"

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "kafka-sub-builder/2.0",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = response.read()

    return json.loads(data.decode("utf-8"))


def test_proxy_url(proxy_name, test_url):
    encoded_name = urllib.parse.quote(proxy_name, safe="")

    query = urllib.parse.urlencode({
        "url": test_url,
        "timeout": REQUEST_TIMEOUT_MS,
        "expected": "200/204",
    })

    path = f"/proxies/{encoded_name}/delay?{query}"

    try:
        data = api_get(path, timeout=(REQUEST_TIMEOUT_MS / 1000) + 5)

        delay = data.get("delay")

        if delay is None:
            return None

        delay = int(delay)

        if delay <= 0 or delay > MAX_LATENCY_MS:
            return None

        return delay

    except Exception:
        return None


def check_one_proxy(proxy_name):
    """
    Strict health check.

    A node is accepted only when:
      - at least REQUIRED_GOOD_URLS different HTTPS endpoints work;
      - each successful URL has REQUIRED_SUCCESSES_PER_URL successes
        out of CHECK_ATTEMPTS;
      - every successful measurement is <= MAX_LATENCY_MS.

    This is intentionally much stricter than a single 204 probe.
    """

    good_urls = 0
    measurements = []

    for test_url in TEST_URLS:
        successes = 0
        delays = []

        # Run attempts sequentially for the same node/URL.
        # This avoids hammering a questionable node with three
        # simultaneous connections.
        for _ in range(CHECK_ATTEMPTS):
            delay = test_proxy_url(proxy_name, test_url)

            if delay is not None:
                successes += 1
                delays.append(delay)

            if successes >= REQUIRED_SUCCESSES_PER_URL:
                break

        if successes >= REQUIRED_SUCCESSES_PER_URL:
            good_urls += 1
            measurements.extend(delays)

        if good_urls >= REQUIRED_GOOD_URLS:
            # Enough independent endpoints succeeded.
            break

    if good_urls < REQUIRED_GOOD_URLS:
        return None

    average = round(sum(measurements) / len(measurements))

    return average


def health_check(proxies):
    log("")
    log("[HEALTH] starting strict multi-endpoint health check")
    log(
        f"[HEALTH] urls={len(TEST_URLS)}, "
        f"attempts={CHECK_ATTEMPTS}, "
        f"required_per_url={REQUIRED_SUCCESSES_PER_URL}, "
        f"required_good_urls={REQUIRED_GOOD_URLS}, "
        f"max_latency={MAX_LATENCY_MS}ms"
    )

    check_config = build_config(
        proxies,
        external_controller="127.0.0.1:9090",
    )

    fd, check_path = tempfile.mkstemp(
        prefix="mihomo-check-",
        suffix=".yaml",
    )
    os.close(fd)

    process = None

    try:
        with open(check_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                check_config,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        # Validate exactly the config that will be used by the checker.
        mihomo_validate(check_path)

        process = subprocess.Popen(
            [
                MIHOMO_PATH,
                "-f",
                check_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        wait_for_mihomo("127.0.0.1:9090", process)

        alive = []
        completed = 0

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            futures = {
                executor.submit(
                    check_one_proxy,
                    proxy["name"],
                ): proxy
                for proxy in proxies
            }

            for future in concurrent.futures.as_completed(futures):
                proxy = futures[future]
                completed += 1

                try:
                    delay = future.result()
                except Exception as e:
                    delay = None
                    log(
                        f"[HEALTH] {completed}/{len(proxies)} "
                        f"{proxy['name']} ERROR: {e}"
                    )

                if delay is not None:
                    proxy["_delay"] = delay
                    alive.append(proxy)

                    log(
                        f"[HEALTH] {completed}/{len(proxies)} "
                        f"OK {delay}ms {proxy['name']}"
                    )
                else:
                    log(
                        f"[HEALTH] {completed}/{len(proxies)} "
                        f"DEAD {proxy['name']}"
                    )

        return alive

    finally:
        if process is not None:
            try:
                process.terminate()
                process.wait(timeout=5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass

        try:
            os.unlink(check_path)
        except OSError:
            pass


# ============================================================
# OUTPUT
# ============================================================

def write_atomic(config):
    validate_structure(config)

    fd, temp_path = tempfile.mkstemp(
        prefix="config-",
        suffix=".yaml",
        dir=".",
    )
    os.close(fd)

    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                config,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        # Most important safety check:
        # do not publish anything Mihomo itself rejects.
        mihomo_validate(temp_path)

        os.replace(temp_path, OUTPUT_CONFIG)

    finally:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass


def remove_internal_fields(proxies):
    for proxy in proxies:
        proxy.pop("_delay", None)
        proxy.pop("_source_name", None)


def main():
    started = time.time()

    log("==============================================")
    log(" kafka-sub builder")
    log(" strict Mihomo health check")
    log("==============================================")

    uris = load_sources()

    log("")
    log(f"[PARSE] total raw VLESS: {len(uris)}")

    proxies = []

    for uri in uris:
        proxy = parse_vless(uri)

        if proxy is not None:
            proxies.append(proxy)

    log(f"[PARSE] valid VLESS: {len(proxies)}")

    proxies = deduplicate(proxies)

    log(f"[DEDUP] unique VLESS: {len(proxies)}")

    if not proxies:
        raise RuntimeError("No valid VLESS proxies found")

    assign_names(proxies)

    alive = health_check(proxies)

    alive.sort(
        key=lambda p: (
            int(p.get("_delay", 999999)),
            str(p.get("name", "")),
        )
    )

    alive_count = len(alive)
    total_count = len(proxies)

    required = max(
        MIN_ALIVE_ABSOLUTE,
        math.ceil(total_count * MIN_ALIVE_PERCENT),
    )

    log("")
    log(f"[RESULT] total: {total_count}")
    log(f"[RESULT] alive: {alive_count}")
    log(f"[RESULT] safety minimum: {required}")

    if alive_count < required:
        raise RuntimeError(
            f"Suspicious update refused: only "
            f"{alive_count}/{total_count} nodes passed strict health check; "
            f"minimum is {required}. Existing config.yaml was preserved."
        )

    remove_internal_fields(alive)

    final_config = build_config(alive)

    write_atomic(final_config)

    elapsed = round(time.time() - started, 1)

    log("")
    log("==============================================")
    log(f" DONE")
    log(f" nodes published: {len(alive)}")
    log(f" elapsed: {elapsed}s")
    log(" config.yaml updated successfully")
    log("==============================================")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
    except Exception as e:
        print(f"\n[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
'''

path = Path("/mnt/data/build.py")
path.write_text(code, encoding="utf-8")
print(f"Готово: {path}")
