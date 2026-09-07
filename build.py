import base64
import concurrent.futures
import json
import math
import os
import re
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

SOURCES_FILE = "sources.txt"
CONFIG_FILE = "config.yaml"

MIHOMO_BINARY = "./mihomo"

API_HOST = "127.0.0.1"
API_PORT = 9090

TEST_URL = "https://www.gstatic.com/generate_204"
EXPECTED_STATUS = "204"

MAX_LATENCY_MS = 5000

CHECK_ATTEMPTS = 3
REQUIRED_SUCCESSES = 2

REQUEST_TIMEOUT_MS = 5000

MAX_WORKERS = 10

STARTUP_TIMEOUT = 20

MIN_ALIVE_PERCENT = 0.05
MIN_ALIVE_ABSOLUTE = 10


# ============================================================
# COUNTRIES
# ============================================================

COUNTRIES = [
    ("Нидерланды", ["nl", "netherlands", "amsterdam", "rotterdam"]),
    ("Германия", ["de", "germany", "berlin", "frankfurt"]),
    ("США", ["us", "usa", "united states", "new york", "los angeles"]),
    ("Финляндия", ["fi", "finland", "helsinki"]),
    ("Польша", ["pl", "poland", "warsaw"]),
    ("Россия", ["ru", "russia", "moscow", "spb"]),
    ("Великобритания", ["gb", "uk", "england", "london"]),
    ("Австрия", ["at", "austria", "vienna"]),
    ("Япония", ["jp", "japan", "tokyo"]),
    ("Сингапур", ["sg", "singapore"]),
    ("Франция", ["fr", "france", "paris"]),
    ("Эстония", ["ee", "estonia", "tallinn"]),
    ("Италия", ["it", "italy", "rome", "milan"]),
    ("Латвия", ["lv", "latvia", "riga"]),
    ("Швеция", ["se", "sweden", "stockholm"]),
    ("Испания", ["es", "spain", "madrid"]),
    ("Чехия", ["cz", "czech", "prague"]),
    ("Болгария", ["bg", "bulgaria"]),
    ("Гонконг", ["hk", "hong kong"]),
    ("Канада", ["ca", "canada", "toronto"]),
    ("Молдова", ["md", "moldova"]),
    ("Турция", ["tr", "turkey", "istanbul"]),
    ("Ирландия", ["ie", "ireland"]),
]


# ============================================================
# VLESS
# ============================================================

VLESS_RE = re.compile(
    r"vless://[^\s\"'<>]+",
    re.IGNORECASE,
)

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)


# ============================================================
# HELPERS
# ============================================================

def load_sources():
    if not os.path.exists(SOURCES_FILE):
        print(f"[!] {SOURCES_FILE} not found")
        sys.exit(1)

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = []

        for line in f:
            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            sources.append(line)

    return sources


def download_source(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "KafkaSubBuilder/3.0"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=30,
    ) as response:
        data = response.read()

    return data


def decode_base64_candidates(text):
    candidates = [text]

    compact = re.sub(r"\s+", "", text.strip())

    if len(compact) < 16:
        return candidates

    variants = [compact]

    if compact.startswith("base64,"):
        variants.append(compact[7:])

    for value in variants:
        try:
            padding = "=" * (-len(value) % 4)

            decoded = base64.b64decode(
                value + padding,
                validate=False,
            )

            decoded_text = decoded.decode(
                "utf-8",
                errors="ignore",
            ).strip()

            if decoded_text:
                candidates.append(decoded_text)

        except Exception:
            continue

    return candidates


def decode_possible_base64(text):
    text = text.strip()

    if not text:
        return text

    if "vless://" in text.lower():
        return text

    candidates = decode_base64_candidates(text)

    for candidate in candidates[1:]:
        if "vless://" in candidate.lower():
            return candidate

    return text


def extract_vless(text):
    found = []

    for candidate in decode_base64_candidates(text):
        for match in VLESS_RE.findall(candidate):
            value = match.rstrip("),]}")

            if value not in found:
                found.append(value)

    return found


def extract_from_json(value):
    result = []

    if isinstance(value, str):
        result.extend(extract_vless(value))

    elif isinstance(value, dict):
        for item in value.values():
            result.extend(
                extract_from_json(item)
            )

    elif isinstance(value, list):
        for item in value:
            result.extend(
                extract_from_json(item)
            )

    return result


def parse_source(data):
    text = data.decode(
        "utf-8",
        errors="ignore",
    )

    result = []

    result.extend(
        extract_vless(text)
    )

    decoded = decode_possible_base64(text)

    if decoded != text:
        result.extend(
            extract_vless(decoded)
        )

    # JSON
    try:
        parsed = json.loads(text)

        result.extend(
            extract_from_json(parsed)
        )

    except Exception:
        pass

    # YAML
    try:
        parsed = yaml.safe_load(text)

        if parsed is not None:
            result.extend(
                extract_from_json(parsed)
            )

    except Exception:
        pass

    return list(
        dict.fromkeys(result)
    )


# ============================================================
# VALUE HELPERS
# ============================================================

def first_query_value(query, *keys):
    for key in keys:
        values = query.get(key)

        if not values:
            continue

        value = values[0]

        if value is None:
            continue

        value = urllib.parse.unquote(
            str(value)
        )

        return value

    return None


def parse_bool(value):
    if value is None:
        return None

    value = str(value).strip().lower()

    if value in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return True

    if value in (
        "0",
        "false",
        "no",
        "off",
    ):
        return False

    return None


def parse_int(value):
    if value is None:
        return None

    try:
        return int(str(value).strip())

    except Exception:
        return None


def parse_float(value):
    if value is None:
        return None

    try:
        return float(
            str(value).strip()
        )

    except Exception:
        return None


def parse_json_value(value):
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return value

    value = str(value).strip()

    if not value:
        return None

    try:
        return json.loads(value)

    except Exception:
        return None


def split_alpn(value):
    if not value:
        return None

    if isinstance(value, list):
        return [
            str(x).strip()
            for x in value
            if str(x).strip()
        ]

    result = []

    for item in str(value).split(","):
        item = item.strip()

        if item:
            result.append(item)

    return result or None


# ============================================================
# FAKE / INVALID CONFIG FILTER
# ============================================================

def is_fake_proxy(
    server,
    port,
    uuid,
    name="",
):
    if not server:
        return True

    server_lower = str(server).strip().lower()
    name_lower = str(name).strip().lower()

    bad_exact_hosts = {
        "localhost",
        "0.0.0.0",
        "127.0.0.1",
        "::1",
        "example.com",
        "example.org",
        "example.net",
    }

    if server_lower in bad_exact_hosts:
        return True

    bad_words = [
        "localhost",
        "example.com",
        "example.org",
        "example.net",
        "invalid.example",
    ]

    combined = (
        f"{server_lower} "
        f"{name_lower}"
    )

    for word in bad_words:
        if word in combined:
            return True

    if port is None:
        return True

    if port < 1 or port > 65535:
        return True

    uuid_compact = uuid.replace("-", "").lower()

    if not uuid_compact:
        return True

    # Очевидные тестовые UUID
    if len(set(uuid_compact)) <= 2:
        return True

    if uuid_compact in {
        "00000000000000000000000000000000",
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
        "33333333333333333333333333333333",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    }:
        return True

    return False


# ============================================================
# XHTTP
# ============================================================

XHTTP_STRING_MAP = {
    "path": "path",
    "host": "host",
    "mode": "mode",
    "xPaddingObfsMode": "x-padding-obfs-mode",
    "xPaddingKey": "x-padding-key",
    "xPaddingHeader": "x-padding-header",
    "xPaddingPlacement": "x-padding-placement",
    "xPaddingMethod": "x-padding-method",
    "uplinkHttpMethod": "uplink-http-method",
    "sessionPlacement": "session-placement",
    "seqPlacement": "seq-placement",
    "reuseSettings": "reuse-settings",
}

XHTTP_INT_MAP = {
    "xPaddingBytes": "x-padding-bytes",
    "scMaxEachPostBytes": "sc-max-each-post-bytes",
    "scMinPostsIntervalMs": "sc-min-posts-interval-ms",
}


def build_xhttp_opts(query):
    opts = {}

    for source_key, target_key in XHTTP_STRING_MAP.items():
        value = first_query_value(
            query,
            source_key,
            target_key,
        )

        if value is not None and value != "":
            opts[target_key] = value

    for source_key, target_key in XHTTP_INT_MAP.items():
        value = first_query_value(
            query,
            source_key,
            target_key,
        )

        number = parse_int(value)

        if number is not None:
            if source_key == "xPaddingBytes":
                if number >= 0:
                    opts[target_key] = number
            else:
                if number > 0:
                    opts[target_key] = number

    extra_raw = first_query_value(
        query,
        "extra",
    )

    if extra_raw:
        extra = parse_json_value(
            extra_raw
        )

        if isinstance(extra, dict):
            # Значения из extra имеют приоритет
            # только там, где они действительно
            # являются XHTTP options.
            for key, value in extra.items():
                if value is None:
                    continue

                opts[key] = value

        else:
            # Если extra не является JSON,
            # сохраняем исходное значение.
            opts["extra"] = extra_raw

    return opts


# ============================================================
# PARSE VLESS
# ============================================================

def parse_vless(url):
    try:
        parsed = urllib.parse.urlparse(url)

        if parsed.scheme.lower() != "vless":
            return None, "invalid scheme"

        if not parsed.hostname:
            return None, "missing server"

        try:
            port = parsed.port

        except ValueError:
            return None, "invalid port"

        if port is None:
            return None, "missing port"

        uuid = urllib.parse.unquote(
            parsed.username or ""
        ).strip()

        if not UUID_RE.match(uuid):
            return None, "invalid UUID"

        name = urllib.parse.unquote(
            parsed.fragment or ""
        ).strip()

        if is_fake_proxy(
            parsed.hostname,
            port,
            uuid,
            name,
        ):
            return None, "dummy/test"

        query = urllib.parse.parse_qs(
            parsed.query,
            keep_blank_values=True,
        )

        proxy = {
            "type": "vless",
            "server": parsed.hostname,
            "port": port,
            "uuid": uuid,
        }

        # --------------------------------------------------------
        # UDP
        # --------------------------------------------------------

        udp = parse_bool(
            first_query_value(
                query,
                "udp",
            )
        )

        if udp is not None:
            proxy["udp"] = udp
        else:
            proxy["udp"] = True

        # --------------------------------------------------------
        # Encryption
        # --------------------------------------------------------

        encryption = first_query_value(
            query,
            "encryption",
        )

        if encryption:
            proxy["encryption"] = encryption

        # --------------------------------------------------------
        # Flow
        # --------------------------------------------------------

        flow = first_query_value(
            query,
            "flow",
        )

        if flow:
            proxy["flow"] = flow

        # --------------------------------------------------------
        # Packet encoding
        # --------------------------------------------------------

        packet_encoding = first_query_value(
            query,
            "packet-encoding",
            "packetEncoding",
        )

        if packet_encoding:
            proxy[
                "packet-encoding"
            ] = packet_encoding

        # --------------------------------------------------------
        # Security
        # --------------------------------------------------------

        security = (
            first_query_value(
                query,
                "security",
            )
            or ""
        ).lower()

        if security in (
            "tls",
            "reality",
        ):
            proxy["tls"] = True

            servername = first_query_value(
                query,
                "sni",
                "servername",
            )

            if servername:
                proxy[
                    "servername"
                ] = servername

            fingerprint = first_query_value(
                query,
                "fp",
                "fingerprint",
            )

            if fingerprint:
                proxy[
                    "client-fingerprint"
                ] = fingerprint

            alpn = split_alpn(
                first_query_value(
                    query,
                    "alpn",
                )
            )

            if alpn:
                proxy["alpn"] = alpn

            skip_cert = parse_bool(
                first_query_value(
                    query,
                    "allowInsecure",
                    "allow-insecure",
                    "skip-cert-verify",
                    "skipCertVerify",
                )
            )

            if skip_cert is not None:
                proxy[
                    "skip-cert-verify"
                ] = skip_cert

            ech = first_query_value(
                query,
                "ech",
            )

            if ech:
                proxy["ech"] = ech

            name_cert_verify = parse_bool(
                first_query_value(
                    query,
                    "name-cert-verify",
                    "nameCertVerify",
                )
            )

            if name_cert_verify is not None:
                proxy[
                    "name-cert-verify"
                ] = name_cert_verify

        # --------------------------------------------------------
        # Reality
        # --------------------------------------------------------

        if security == "reality":
            public_key = first_query_value(
                query,
                "pbk",
                "public-key",
                "publicKey",
            )

            if not public_key:
                return None, (
                    "missing Reality public key"
                )

            if len(public_key) < 40:
                return None, (
                    "invalid Reality public key"
                )

            short_id = first_query_value(
                query,
                "sid",
                "short-id",
                "shortId",
            )

            reality = {
                "public-key": public_key,
            }

            if short_id:
                reality[
                    "short-id"
                ] = short_id

            proxy[
                "reality-opts"
            ] = reality

        # --------------------------------------------------------
        # Network
        # --------------------------------------------------------

        network = (
            first_query_value(
                query,
                "type",
                "network",
            )
            or "tcp"
        ).lower()

        supported_networks = {
            "tcp",
            "ws",
            "grpc",
            "xhttp",
            "http",
            "h2",
        }

        if network not in supported_networks:
            return None, (
                f"unsupported network: "
                f"{network}"
            )

        proxy[
            "network"
        ] = network

        # --------------------------------------------------------
        # TCP
        # --------------------------------------------------------

        if network == "tcp":
            header_type = first_query_value(
                query,
                "headerType",
                "header-type",
            )

            if header_type:
                tcp_opts = {
                    "header": {
                        "type": header_type
                    }
                }

                proxy[
                    "tcp-opts"
                ] = tcp_opts

        # --------------------------------------------------------
        # WebSocket
        # --------------------------------------------------------

        elif network == "ws":
            ws_opts = {}

            path = first_query_value(
                query,
                "path",
            )

            if path:
                ws_opts["path"] = path

            host = first_query_value(
                query,
                "host",
            )

            if host:
                ws_opts["headers"] = {
                    "Host": host
                }

            max_early_data = parse_int(
                first_query_value(
                    query,
                    "ed",
                    "max-early-data",
                )
            )

            if max_early_data is not None:
                ws_opts[
                    "max-early-data"
                ] = max_early_data

            early_data_header = first_query_value(
                query,
                "eh",
                "early-data-header-name",
            )

            if early_data_header:
                ws_opts[
                    "early-data-header-name"
                ] = early_data_header

            if ws_opts:
                proxy[
                    "ws-opts"
                ] = ws_opts

        # --------------------------------------------------------
        # gRPC
        # --------------------------------------------------------

        elif network == "grpc":
            grpc_opts = {}

            service = first_query_value(
                query,
                "serviceName",
                "service-name",
            )

            if service:
                grpc_opts[
                    "grpc-service-name"
                ] = service

            mode = first_query_value(
                query,
                "mode",
            )

            if mode:
                grpc_opts["grpc-mode"] = mode

            if grpc_opts:
                proxy[
                    "grpc-opts"
                ] = grpc_opts

        # --------------------------------------------------------
        # XHTTP
        # --------------------------------------------------------

        elif network == "xhttp":
            xhttp_opts = build_xhttp_opts(
                query
            )

            if not xhttp_opts:
                return None, (
                    "xhttp without options"
                )

            proxy[
                "xhttp-opts"
            ] = xhttp_opts

        # --------------------------------------------------------
        # HTTP / H2
        # --------------------------------------------------------

        elif network in (
            "http",
            "h2",
        ):
            http_opts = {}

            path = first_query_value(
                query,
                "path",
            )

            if path:
                http_opts["path"] = path

            host = first_query_value(
                query,
                "host",
            )

            if host:
                http_opts["headers"] = {
                    "Host": host
                }

            if http_opts:
                proxy[
                    "http-opts"
                ] = http_opts

        # --------------------------------------------------------
        # Name
        # --------------------------------------------------------

        if name:
            proxy[
                "_source_name"
            ] = name

        return proxy, None

    except Exception as e:
        return None, str(e)


# ============================================================
# DEDUPLICATION
# ============================================================

def proxy_identity(proxy):
    copy = dict(proxy)

    copy.pop("name", None)
    copy.pop("_source_name", None)

    return json.dumps(
        copy,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def deduplicate(proxies):
    result = []
    seen = set()
    duplicates = 0

    for proxy in proxies:
        identity = proxy_identity(
            proxy
        )

        if identity in seen:
            duplicates += 1
            continue

        seen.add(identity)
        result.append(proxy)

    return result, duplicates


# ============================================================
# COUNTRY
# ============================================================

def detect_country(proxy):
    server = str(
        proxy.get("server", "")
    ).lower()

    servername = str(
        proxy.get("servername", "")
    ).lower()

    source_name = str(
        proxy.get("_source_name", "")
    ).lower()

    # Сначала пытаемся определить страну
    # по имени узла — оно обычно надёжнее.
    for country, keywords in COUNTRIES:
        for keyword in keywords:
            if keyword in source_name:
                return country

    # Затем servername.
    for country, keywords in COUNTRIES:
        for keyword in keywords:
            if keyword in servername:
                return country

    # В последнюю очередь server.
    for country, keywords in COUNTRIES:
        for keyword in keywords:
            if keyword in server:
                return country

    return "Европа"


def assign_names(proxies):
    counters = {}

    for proxy in proxies:
        country = detect_country(
            proxy
        )

        counters[country] = (
            counters.get(country, 0) + 1
        )

        proxy["name"] = (
            f"{country} "
            f"{counters[country]}"
        )

    return proxies


# ============================================================
# MIHOMO API
# ============================================================

def api_url(path, params=None):
    url = (
        f"http://{API_HOST}:{API_PORT}"
        f"{path}"
    )

    if params:
        url += "?" + urllib.parse.urlencode(
            params
        )

    return url


def api_get(path, params=None):
    request = urllib.request.Request(
        api_url(path, params),
        headers={
            "User-Agent": "KafkaSubBuilder/3.0"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=10,
    ) as response:
        return response.read().decode(
            "utf-8",
            errors="replace",
        )


# ============================================================
# TEMPORARY MIHOMO CONFIG
# ============================================================

def create_check_config(proxies):
    config = {
        "mixed-port": 7890,

        "mode": "rule",

        "external-controller":
            f"{API_HOST}:{API_PORT}",

        "profile": {
            "store-selected": False,
            "store-fake-ip": False,
        },

        "proxies": proxies,

        "proxy-groups": [
            {
                "name": "CHECK",
                "type": "select",
                "proxies": [
                    proxy["name"]
                    for proxy in proxies
                ],
            }
        ],

        "rules": [
            "MATCH,DIRECT"
        ],
    }

    fd, path = tempfile.mkstemp(
        prefix="kafka-check-",
        suffix=".yaml",
    )

    os.close(fd)

    with open(
        path,
        "w",
        encoding="utf-8",
    ) as f:
        yaml.safe_dump(
            config,
            f,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )

    return path


# ============================================================
# MIHOMO VALIDATION
# ============================================================

def validate_with_mihomo(
    config,
    label="config",
):
    if not os.path.exists(
        MIHOMO_BINARY
    ):
        print(
            f"[!] Mihomo binary not found: "
            f"{MIHOMO_BINARY}"
        )

        return False, (
            "mihomo binary not found"
        )

    fd, path = tempfile.mkstemp(
        prefix="kafka-validate-",
        suffix=".yaml",
    )

    os.close(fd)

    try:
        with open(
            path,
            "w",
            encoding="utf-8",
        ) as f:
            yaml.safe_dump(
                config,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        result = subprocess.run(
            [
                MIHOMO_BINARY,
                "-t",
                "-f",
                path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=30,
        )

        output = (
            result.stdout or ""
        ).strip()

        if result.returncode != 0:
            print()
            print(
                f"[!] Mihomo validation failed "
                f"for {label}:"
            )
            print(output[-5000:])

            return False, output

        return True, None

    except subprocess.TimeoutExpired:
        return False, (
            "mihomo validation timeout"
        )

    except Exception as e:
        return False, str(e)

    finally:
        if os.path.exists(path):
            os.remove(path)


def validate_proxies_with_mihomo(
    proxies,
):
    """
    Проверяет весь набор через Mihomo.
    Mihomo либо принимает конфиг целиком,
    либо отклоняет его.

    Если весь набор не проходит,
    выполняется бинарное деление:
    проблемный узел постепенно локализуется.
    """

    if not proxies:
        return [], []

    print()
    print(
        "=== Mihomo syntax validation ==="
    )
    print()

    test_config = {
        "mixed-port": 7890,
        "mode": "rule",
        "proxies": proxies,
        "proxy-groups": [
            {
                "name": "CHECK",
                "type": "select",
                "proxies": [
                    proxy["name"]
                    for proxy in proxies
                ],
            }
        ],
        "rules": [
            "MATCH,DIRECT"
        ],
    }

    ok, error = validate_with_mihomo(
        test_config,
        "all proxies",
    )

    if ok:
        print(
            f"[+] Mihomo accepted all "
            f"{len(proxies)} proxies."
        )

        return proxies, []

    print(
        "[!] Mihomo rejected the complete "
        "proxy set."
    )

    print(
        "[+] Locating invalid proxies..."
    )

    valid = []
    rejected = []

    def validate_subset(subset):
        if not subset:
            return True

        config = {
            "mixed-port": 7890,
            "mode": "rule",
            "proxies": subset,
            "proxy-groups": [
                {
                    "name": "CHECK",
                    "type": "select",
                    "proxies": [
                        proxy["name"]
                        for proxy in subset
                    ],
                }
            ],
            "rules": [
                "MATCH,DIRECT"
            ],
        }

        ok, _ = validate_with_mihomo(
            config,
            "subset",
        )

        return ok

    def locate(subset):
        if not subset:
            return

        if validate_subset(subset):
            valid.extend(subset)
            return

        if len(subset) == 1:
            rejected.append(
                (
                    subset[0],
                    "mihomo validation",
                )
            )
            return

        middle = len(subset) // 2

        locate(
            subset[:middle]
        )

        locate(
            subset[middle:]
        )

    locate(proxies)

    print(
        f"[+] Mihomo valid: "
        f"{len(valid)}"
    )

    print(
        f"[+] Mihomo rejected: "
        f"{len(rejected)}"
    )

    return valid, rejected


# ============================================================
# MIHOMO PROCESS
# ============================================================

def start_mihomo(proxies):
    check_config = create_check_config(
        proxies
    )

    print(
        "[+] Starting Mihomo..."
    )

    process = subprocess.Popen(
        [
            MIHOMO_BINARY,
            "-d",
            ".",
            "-f",
            check_config,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    start = time.time()

    while (
        time.time() - start
        < STARTUP_TIMEOUT
    ):
        if process.poll() is not None:
            output = ""

            try:
                output = (
                    process.stdout.read()
                )
            except Exception:
                pass

            if os.path.exists(
                check_config
            ):
                os.remove(
                    check_config
                )

            print()
            print(
                "[!] Mihomo exited during "
                "startup:"
            )
            print(
                output[-5000:]
            )

            sys.exit(1)

        try:
            api_get(
                "/proxies"
            )

            print(
                "[+] Mihomo API is ready."
            )

            return (
                process,
                check_config,
            )

        except Exception:
            time.sleep(0.5)

    print(
        "[!] Mihomo API did not start."
    )

    process.kill()

    if os.path.exists(
        check_config
    ):
        os.remove(
            check_config
        )

    sys.exit(1)


# ============================================================
# HEALTH CHECK
# ============================================================

def check_once(name):
    encoded_name = urllib.parse.quote(
        name,
        safe="",
    )

    try:
        data = api_get(
            f"/proxies/{encoded_name}/delay",
            {
                "url": TEST_URL,
                "timeout":
                    REQUEST_TIMEOUT_MS,
                "expected":
                    EXPECTED_STATUS,
            },
        )

        result = json.loads(
            data
        )

        delay = result.get(
            "delay"
        )

        if not isinstance(
            delay,
            int,
        ):
            return False, None

        if delay <= 0:
            return False, delay

        if delay > MAX_LATENCY_MS:
            return False, delay

        return True, delay

    except Exception:
        return False, None


def check_proxy(proxy):
    name = proxy["name"]

    successes = 0
    failures = 0

    delays = []

    attempts_done = 0

    for attempt in range(
        1,
        CHECK_ATTEMPTS + 1,
    ):
        attempts_done += 1

        ok, delay = check_once(
            name
        )

        if ok:
            successes += 1

            if delay is not None:
                delays.append(
                    delay
                )

        else:
            failures += 1

        remaining = (
            CHECK_ATTEMPTS
            - attempt
        )

        # Уже набрали необходимое
        if successes >= REQUIRED_SUCCESSES:
            break

        # Даже при идеальном результате
        # оставшихся попыток недостаточно.
        if (
            successes + remaining
            < REQUIRED_SUCCESSES
        ):
            break

    alive = (
        successes
        >= REQUIRED_SUCCESSES
    )

    average_delay = None

    if delays:
        average_delay = (
            sum(delays)
            // len(delays)
        )

    return {
        "proxy": proxy,
        "alive": alive,
        "successes": successes,
        "failures": failures,
        "attempts": attempts_done,
        "delay": average_delay,
    }


def health_check(proxies):
    print()
    print(
        "=== Health Check ==="
    )
    print()

    print(
        f"[+] Endpoint: {TEST_URL}"
    )

    print(
        f"[+] Attempts: "
        f"{CHECK_ATTEMPTS}"
    )

    print(
        f"[+] Required: "
        f"{REQUIRED_SUCCESSES}/"
        f"{CHECK_ATTEMPTS}"
    )

    print(
        f"[+] Max latency: "
        f"{MAX_LATENCY_MS} ms"
    )

    print()

    process, check_config = (
        start_mihomo(proxies)
    )

    alive = []
    dead = []

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            futures = [
                executor.submit(
                    check_proxy,
                    proxy,
                )
                for proxy in proxies
            ]

            total = len(futures)

            for index, future in enumerate(
                concurrent.futures.as_completed(
                    futures
                ),
                start=1,
            ):
                result = (
                    future.result()
                )

                proxy = result[
                    "proxy"
                ]

                name = proxy[
                    "name"
                ]

                if result["alive"]:
                    alive.append(
                        proxy
                    )

                    delay_text = (
                        f"{result['delay']} ms"
                        if result["delay"]
                        is not None
                        else "n/a"
                    )

                    print(
                        f"[{index}/{total}] "
                        f"OK   {name} "
                        f"({result['successes']}/"
                        f"{result['attempts']}) "
                        f"{delay_text}"
                    )

                else:
                    dead.append(
                        proxy
                    )

                    print(
                        f"[{index}/{total}] "
                        f"FAIL {name} "
                        f"({result['successes']}/"
                        f"{result['attempts']})"
                    )

    finally:
        print()
        print(
            "[+] Stopping Mihomo..."
        )

        process.terminate()

        try:
            process.wait(
                timeout=5
            )

        except subprocess.TimeoutExpired:
            process.kill()

        if os.path.exists(
            check_config
        ):
            os.remove(
                check_config
            )

    return alive, dead


# ============================================================
# FINAL CONFIG
# ============================================================

def generate_config(proxies):
    return {
        "mixed-port": 7890,

        "mode": "rule",

        "proxies": proxies,

        "proxy-groups": [
            {
                "name": "🚀 Freedom Rudy",
                "type": "select",
                "proxies": [
                    proxy["name"]
                    for proxy in proxies
                ] + [
                    "DIRECT"
                ],
            }
        ],

        "rules": [
            "MATCH,🚀 Freedom Rudy"
        ],
    }


def validate_config_structure(config):
    if not isinstance(
        config,
        dict,
    ):
        return False, (
            "config is not a mapping"
        )

    proxies = config.get(
        "proxies"
    )

    if not isinstance(
        proxies,
        list,
    ):
        return False, (
            "proxies is not a list"
        )

    if not proxies:
        return False, (
            "no proxies"
        )

    names = set()

    for proxy in proxies:
        if not isinstance(
            proxy,
            dict,
        ):
            return False, (
                "invalid proxy object"
            )

        name = proxy.get(
            "name"
        )

        if not name:
            return False, (
                "proxy without name"
            )

        if name in names:
            return False, (
                f"duplicate proxy name: "
                f"{name}"
            )

        names.add(name)

        if proxy.get(
            "type"
        ) != "vless":
            return False, (
                "unsupported proxy type: "
                f"{proxy.get('type')}"
            )

        server = proxy.get(
            "server"
        )

        if not server:
            return False, (
                f"{name}: missing server"
            )

        port = proxy.get(
            "port"
        )

        if not isinstance(
            port,
            int,
        ):
            return False, (
                f"{name}: invalid port"
            )

        if not (
            1 <= port <= 65535
        ):
            return False, (
                f"{name}: invalid port"
            )

    groups = config.get(
        "proxy-groups"
    )

    if not isinstance(
        groups,
        list,
    ):
        return False, (
            "proxy-groups missing"
        )

    return True, None


def atomic_write_config(config):
    directory = os.path.dirname(
        os.path.abspath(
            CONFIG_FILE
        )
    ) or "."

    fd, temp_path = (
        tempfile.mkstemp(
            prefix="config-",
            suffix=".yaml",
            dir=directory,
        )
    )

    os.close(fd)

    try:
        with open(
            temp_path,
            "w",
            encoding="utf-8",
        ) as f:
            yaml.safe_dump(
                config,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        with open(
            temp_path,
            "r",
            encoding="utf-8",
        ) as f:
            reloaded = yaml.safe_load(
                f
            )

        valid, error = (
            validate_config_structure(
                reloaded
            )
        )

        if not valid:
            raise RuntimeError(
                "Final config validation "
                f"failed: {error}"
            )

        # Последняя проверка именно
        # того YAML, который собираемся
        # опубликовать.
        mihomo_valid, mihomo_error = (
            validate_with_mihomo(
                reloaded,
                "final config",
            )
        )

        if not mihomo_valid:
            raise RuntimeError(
                "Final Mihomo validation "
                f"failed: {mihomo_error}"
            )

        os.replace(
            temp_path,
            CONFIG_FILE,
        )

    finally:
        if os.path.exists(
            temp_path
        ):
            os.remove(
                temp_path
            )


# ============================================================
# STATISTICS
# ============================================================

def print_statistics(
    total_found,
    valid_count,
    mihomo_valid_count,
    mihomo_rejected_count,
    rejected,
    duplicates,
    alive,
    dead,
):
    print()
    print(
        "=== Statistics ==="
    )
    print()

    print(
        f"Found VLESS:          "
        f"{total_found}"
    )

    print(
        f"Valid after parsing:  "
        f"{valid_count}"
    )

    print(
        f"Mihomo accepted:      "
        f"{mihomo_valid_count}"
    )

    print(
        f"Mihomo rejected:      "
        f"{mihomo_rejected_count}"
    )

    print(
        f"Alive:                "
        f"{len(alive)}"
    )

    print(
        f"Dead:                 "
        f"{len(dead)}"
    )

    print(
        f"Duplicates removed:   "
        f"{duplicates}"
    )

    print()

    if rejected:
        print(
            "Parser rejected:"
        )

        for reason, count in sorted(
            rejected.items()
        ):
            print(
                f"  {reason}: {count}"
            )

        print()

    countries = {}

    for proxy in alive:
        country = detect_country(
            proxy
        )

        countries[country] = (
            countries.get(
                country,
                0
            ) + 1
        )

    if countries:
        print(
            "Countries:"
        )

        for country, count in sorted(
            countries.items(),
            key=lambda x: (
                -x[1],
                x[0],
            ),
        ):
            print(
                f"  {country}: {count}"
            )


# ============================================================
# MAIN
# ============================================================

def main():
    print(
        "=== Kafka Sub Builder 3.0 ==="
    )

    print()

    # --------------------------------------------------------
    # Check binary
    # --------------------------------------------------------

    if not os.path.exists(
        MIHOMO_BINARY
    ):
        print(
            f"[!] Mihomo binary not found: "
            f"{MIHOMO_BINARY}"
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Sources
    # --------------------------------------------------------

    sources = load_sources()

    print(
        f"[+] Sources: {len(sources)}"
    )

    print()

    all_vless = []

    for source in sources:
        print(
            f"[+] Downloading: {source}"
        )

        try:
            data = download_source(
                source
            )

            found = parse_source(
                data
            )

            print(
                f"[+] Found VLESS: "
                f"{len(found)}"
            )

            all_vless.extend(
                found
            )

        except Exception as e:
            print(
                f"[!] Failed: {e}"
            )

    all_vless = list(
        dict.fromkeys(
            all_vless
        )
    )

    print()

    print(
        f"[+] Total unique VLESS: "
        f"{len(all_vless)}"
    )

    print()

    if not all_vless:
        print(
            "[!] No VLESS configurations "
            "found."
        )

        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Parse
    # --------------------------------------------------------

    valid = []
    rejected = {}

    for url in all_vless:
        proxy, error = parse_vless(
            url
        )

        if proxy is None:
            rejected[error] = (
                rejected.get(
                    error,
                    0,
                ) + 1
            )

            continue

        valid.append(
            proxy
        )

    print(
        f"[+] Valid after parsing: "
        f"{len(valid)}"
    )

    # --------------------------------------------------------
    # Deduplicate
    # --------------------------------------------------------

    valid, duplicates = (
        deduplicate(valid)
    )

    print(
        f"[+] Duplicates removed: "
        f"{duplicates}"
    )

    # --------------------------------------------------------
    # Names
    # --------------------------------------------------------

    valid = assign_names(
        valid
    )

    if not valid:
        print()
        print(
            "[!] No valid proxies."
        )

        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Mihomo validation
    # --------------------------------------------------------

    mihomo_valid, mihomo_rejected = (
        validate_proxies_with_mihomo(
            valid
        )
    )

    if not mihomo_valid:
        print()
        print(
            "[!] No proxies survived "
            "Mihomo validation."
        )

        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Health check
    # --------------------------------------------------------

    alive, dead = health_check(
        mihomo_valid
    )

    # --------------------------------------------------------
    # Safety threshold
    # --------------------------------------------------------

    minimum_alive = max(
        MIN_ALIVE_ABSOLUTE,
        math.ceil(
            len(mihomo_valid)
            * MIN_ALIVE_PERCENT
        ),
    )

    print()

    print(
        f"[+] Alive: "
        f"{len(alive)}/"
        f"{len(mihomo_valid)}"
    )

    print(
        f"[+] Minimum required: "
        f"{minimum_alive}"
    )

    if len(alive) < minimum_alive:
        print()
        print(
            "[!] Too few healthy proxies."
        )

        print(
            "[!] This update looks "
            "suspicious."
        )

        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Rebuild names only for final set
    # --------------------------------------------------------

    alive = assign_names(
        alive
    )

    # --------------------------------------------------------
    # Final config
    # --------------------------------------------------------

    final_config = generate_config(
        alive
    )

    valid_structure, error = (
        validate_config_structure(
            final_config
        )
    )

    if not valid_structure:
        print()
        print(
            f"[!] Final structure "
            f"validation failed: {error}"
        )

        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Atomic write + final Mihomo test
    # --------------------------------------------------------

    try:
        atomic_write_config(
            final_config
        )

    except Exception as e:
        print()
        print(
            f"[!] Failed to write "
            f"config.yaml: {e}"
        )

        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    print_statistics(
        total_found=len(all_vless),
        valid_count=len(valid),
        mihomo_valid_count=len(
            mihomo_valid
        ),
        mihomo_rejected_count=len(
            mihomo_rejected
        ),
        rejected=rejected,
        duplicates=duplicates,
        alive=alive,
        dead=dead,
    )

    print()

    print(
        "=== Build complete ==="
    )

    print(
        f"Proxies: {len(alive)}"
    )

    print(
        f"Output:  {CONFIG_FILE}"
    )


if __name__ == "__main__":
    main()
