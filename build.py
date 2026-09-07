import base64
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, unquote

import yaml


# ============================================================
# CONFIG
# ============================================================

SOURCES_FILE = "sources.txt"
OUTPUT_FILE = "config.yaml"
CHECK_CONFIG_FILE = "check-config.yaml"

GROUP_NAME = "🚀 Freedom Rudy"

MIHOMO_BINARY = "./mihomo"

API_HOST = "127.0.0.1"
API_PORT = 9090

# Несколько независимых HTTPS-проверок.
TEST_URLS = [
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/generate_204",
    "https://www.google.com/generate_204",
]

# Таймаут одного запроса к endpoint.
TIMEOUT_MS = 5000

# Максимальная допустимая задержка.
# Всё выше этого считается слишком медленным.
MAX_LATENCY_MS = 1500

# Одновременно проверяем столько нод.
CHECK_WORKERS = 10

# Сколько полноценных раундов проверки.
STABILITY_ROUNDS = 2

# Пауза между раундами.
STABILITY_DELAY = 3

# В каждом раунде минимум 2 endpoint из 3 должны пройти.
MIN_SUCCESSFUL_ENDPOINTS = 2

# Защита от массового сбоя.
MIN_ALIVE_ABSOLUTE = 10
MIN_ALIVE_PERCENT = 0.05

STARTUP_TIMEOUT = 20
REQUEST_TIMEOUT = 10


# ============================================================
# COUNTRY MAP
# ============================================================

COUNTRY_MAP = {
    "🇺🇸": "США",
    "🇳🇱": "Нидерланды",
    "🇩🇪": "Германия",
    "🇫🇮": "Финляндия",
    "🇵🇱": "Польша",
    "🇷🇺": "Россия",
    "🇬🇧": "Великобритания",
    "🇸🇪": "Швеция",
    "🇦🇹": "Австрия",
    "🇫🇷": "Франция",
    "🇯🇵": "Япония",
    "🇧🇬": "Болгария",
    "🇹🇷": "Турция",
    "🇪🇪": "Эстония",
    "🇭🇰": "Гонконг",
    "🇮🇪": "Ирландия",
    "🇪🇸": "Испания",
    "🇮🇹": "Италия",
    "🇱🇻": "Латвия",
    "🇨🇿": "Чехия",
    "🇨🇦": "Канада",
    "🇸🇬": "Сингапур",
    "🇱🇹": "Литва",
    "🇦🇲": "Армения",
    "🇲🇩": "Молдова",
}


DUMMY_VALUES = {
    "test",
    "testing",
    "example",
    "example.com",
    "localhost",
    "null",
    "none",
    "undefined",
    "1",
    "123",
    "1234",
    "12344",
    "abcd",
    "abcd1234",
    "changeme",
}


VALID_NETWORKS = {
    "ws",
    "grpc",
    "xhttp",
    "http",
    "h2",
}


VALID_SECURITY = {
    "tls",
    "reality",
}


# ============================================================
# SOURCE HANDLING
# ============================================================

def read_sources():
    with open(
        SOURCES_FILE,
        "r",
        encoding="utf-8",
    ) as f:
        return [
            line.strip()
            for line in f
            if line.strip()
            and not line.lstrip().startswith("#")
        ]


def download(url):
    print(f"[+] Downloading: {url}")

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 "
                "KafkaSubBuilder/4.0"
            )
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=30,
    ) as response:
        return response.read()


def extract_vless(text):
    return re.findall(
        r"vless://[^\s\"'<>]+",
        text,
        flags=re.IGNORECASE,
    )


def try_base64_decode(data):
    try:
        clean = re.sub(
            r"\s+",
            "",
            data,
        )

        if not clean:
            return None

        # Если это явно не похоже на base64,
        # не тратим время.
        if len(clean) < 20:
            return None

        clean += "=" * (
            (-len(clean)) % 4
        )

        decoded = base64.b64decode(
            clean,
            validate=False,
        )

        text = decoded.decode(
            "utf-8",
            errors="ignore",
        )

        if (
            "vless://" in text.lower()
            or "vmess://" in text.lower()
            or "trojan://" in text.lower()
        ):
            return text

    except Exception:
        pass

    return None


def extract_from_json(value):
    result = []

    if isinstance(value, str):
        result.extend(
            extract_vless(value)
        )

    elif isinstance(value, list):
        for item in value:
            result.extend(
                extract_from_json(item)
            )

    elif isinstance(value, dict):
        for key, item in value.items():
            result.extend(
                extract_from_json(item)
            )

    return result


def extract_links_from_source(data):
    """
    Универсальный поиск VLESS:

    - обычный текст;
    - JSON;
    - YAML;
    - Base64;
    - VLESS, находящийся внутри JSON/YAML.
    """

    text = data.decode(
        "utf-8",
        errors="replace",
    )

    links = []

    # Обычный текст.
    links.extend(
        extract_vless(text)
    )

    # JSON.
    try:
        parsed_json = json.loads(
            text
        )

        links.extend(
            extract_from_json(
                parsed_json
            )
        )

    except Exception:
        pass

    # YAML / Mihomo-конфиг.
    try:
        parsed_yaml = yaml.safe_load(
            text
        )

        links.extend(
            extract_from_json(
                parsed_yaml
            )
        )

    except Exception:
        pass

    # Base64.
    decoded = try_base64_decode(
        text
    )

    if decoded:
        links.extend(
            extract_vless(
                decoded
            )
        )

        try:
            decoded_json = json.loads(
                decoded
            )

            links.extend(
                extract_from_json(
                    decoded_json
                )
            )

        except Exception:
            pass

        try:
            decoded_yaml = yaml.safe_load(
                decoded
            )

            links.extend(
                extract_from_json(
                    decoded_yaml
                )
            )

        except Exception:
            pass

    # Убираем дубли самих ссылок.
    unique = []
    seen = set()

    for link in links:
        link = link.strip()

        if not link:
            continue

        if link in seen:
            continue

        seen.add(link)
        unique.append(link)

    return unique


# ============================================================
# VLESS PARSER
# ============================================================

def get_country(remark):
    for flag, country in COUNTRY_MAP.items():
        if flag in remark:
            return country

    countries = sorted(
        set(COUNTRY_MAP.values()),
        key=len,
        reverse=True,
    )

    lower = remark.lower()

    for country in countries:
        if country.lower() in lower:
            return country

    return "Европа"


def valid_uuid(uuid):
    return bool(
        re.fullmatch(
            r"[0-9a-fA-F]{8}-"
            r"[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F4]{4}-"
            r"[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{12}",
            uuid,
        )
    )


def valid_reality_key(key):
    return bool(
        re.fullmatch(
            r"[A-Za-z0-9_-]{43}",
            key,
        )
    )


def clean_query_value(value):
    if value is None:
        return None

    value = unquote(
        str(value)
    )

    if value == "":
        return None

    return value


def get_first(query, key):
    values = query.get(key)

    if not values:
        return None

    return clean_query_value(
        values[0]
    )


def is_dummy(parsed_url, query):
    hostname = (
        parsed_url.hostname
        or ""
    )

    try:
        port = parsed_url.port
    except ValueError:
        return True

    uuid = unquote(
        parsed_url.username
        or ""
    )

    if uuid.lower() in DUMMY_VALUES:
        return True

    if hostname.lower() in DUMMY_VALUES:
        return True

    if port in {
        1,
        11,
        123,
        1234,
        12344,
    }:
        return True

    pbk = get_first(
        query,
        "pbk",
    )

    if (
        pbk
        and pbk.lower()
        in DUMMY_VALUES
    ):
        return True

    sid = get_first(
        query,
        "sid",
    )

    if (
        sid
        and sid.lower()
        in DUMMY_VALUES
    ):
        return True

    remark = unquote(
        parsed_url.fragment
        or ""
    ).strip()

    lower = remark.lower()

    for word in [
        "test",
        "testing",
        "example",
        "пустышка",
    ]:
        if word in lower:
            return True

    return False


def parse_vless(link):
    parsed = urlparse(
        link
    )

    if parsed.scheme.lower() != "vless":
        return None, "invalid scheme"

    try:
        query = parse_qs(
            parsed.query,
            keep_blank_values=True,
        )

        uuid = unquote(
            parsed.username
            or ""
        )

        server = parsed.hostname

        port = parsed.port

        remark = unquote(
            parsed.fragment
            or ""
        ).strip()

    except Exception:
        return None, "malformed URL"

    if not server:
        return None, "missing server"

    if not port:
        return None, "missing port"

    if not valid_uuid(uuid):
        return None, "invalid UUID"

    if is_dummy(
        parsed,
        query,
    ):
        return None, "dummy/test"

    proxy = {
        "name": remark or "Европа",
        "type": "vless",
        "server": server,
        "port": port,
        "uuid": uuid,
    }

    flow = get_first(
        query,
        "flow",
    )

    if flow:
        proxy["flow"] = flow

    encryption = get_first(
        query,
        "encryption",
    )

    if encryption:
        proxy["encryption"] = encryption

    packet_encoding = get_first(
        query,
        "packetEncoding",
    )

    if packet_encoding:
        proxy["packet-encoding"] = (
            packet_encoding
        )

    security = get_first(
        query,
        "security",
    )

    if security in VALID_SECURITY:
        proxy["tls"] = True

    servername = get_first(
        query,
        "sni",
    )

    if servername:
        proxy["servername"] = (
            servername
        )

    fingerprint = get_first(
        query,
        "fp",
    )

    if fingerprint:
        proxy[
            "client-fingerprint"
        ] = fingerprint

    pbk = get_first(
        query,
        "pbk",
    )

    sid = get_first(
        query,
        "sid",
    )

    if (
        security == "reality"
        or pbk
    ):
        if not pbk:
            return (
                None,
                "Reality without public key",
            )

        if not valid_reality_key(
            pbk
        ):
            return (
                None,
                "invalid Reality key",
            )

        proxy[
            "reality-opts"
        ] = {
            "public-key": pbk
        }

        if sid:
            proxy[
                "reality-opts"
            ][
                "short-id"
            ] = sid

    alpn = get_first(
        query,
        "alpn",
    )

    if alpn:
        proxy["alpn"] = [
            item.strip()
            for item in alpn.split(",")
            if item.strip()
        ]

    allow_insecure = get_first(
        query,
        "allowInsecure",
    )

    if allow_insecure in {
        "1",
        "true",
        "True",
    }:
        proxy[
            "skip-cert-verify"
        ] = True

    network = get_first(
        query,
        "type",
    )

    if network in VALID_NETWORKS:
        proxy["network"] = (
            network
        )

    # -------------------------
    # WebSocket
    # -------------------------

    if network == "ws":
        ws_path = get_first(
            query,
            "path",
        )

        ws_host = get_first(
            query,
            "host",
        )

        ws_opts = {}

        if ws_path:
            ws_opts["path"] = (
                ws_path
            )

        if ws_host:
            ws_opts[
                "headers"
            ] = {
                "Host": ws_host
            }

        if ws_opts:
            proxy[
                "ws-opts"
            ] = ws_opts

    # -------------------------
    # gRPC
    # -------------------------

    if network == "grpc":
        service_name = get_first(
            query,
            "serviceName",
        )

        if service_name:
            proxy[
                "grpc-opts"
            ] = {
                "grpc-service-name":
                    service_name
            }

    # -------------------------
    # XHTTP
    # -------------------------

    if network == "xhttp":
        xhttp = {}

        path = get_first(
            query,
            "path",
        )

        host = get_first(
            query,
            "host",
        )

        mode = get_first(
            query,
            "mode",
        )

        if path:
            xhttp["path"] = path

        if host:
            xhttp["host"] = host

        if mode:
            xhttp["mode"] = mode

        xhttp_fields = [
            (
                "xPaddingBytes",
                "x-padding-bytes",
            ),
            (
                "xPaddingObfsMode",
                "x-padding-obfs-mode",
            ),
            (
                "xPaddingKey",
                "x-padding-key",
            ),
            (
                "xPaddingHeader",
                "x-padding-header",
            ),
            (
                "xPaddingPlacement",
                "x-padding-placement",
            ),
            (
                "xPaddingMethod",
                "x-padding-method",
            ),
            (
                "uplinkHTTPMethod",
                "uplink-http-method",
            ),
            (
                "sessionIDPlacement",
                "session-placement",
            ),
            (
                "sessionIDKey",
                "session-key",
            ),
            (
                "sessionIDTable",
                "session-table",
            ),
            (
                "sessionIDLength",
                "session-length",
            ),
            (
                "seqPlacement",
                "seq-placement",
            ),
            (
                "seqKey",
                "seq-key",
            ),
            (
                "uplinkDataPlacement",
                "uplink-data-placement",
            ),
            (
                "uplinkDataKey",
                "uplink-data-key",
            ),
            (
                "uplinkChunkSize",
                "uplink-chunk-size",
            ),
            (
                "scMaxEachPostBytes",
                "sc-max-each-post-bytes",
            ),
            (
                "scMinPostsIntervalMs",
                "sc-min-posts-interval-ms",
            ),
        ]

        for source, target in xhttp_fields:
            value = get_first(
                query,
                source,
            )

            if value is None:
                continue

            if source == "xPaddingObfsMode":
                value = (
                    value.lower()
                    == "true"
                )

            elif source in {
                "uplinkChunkSize",
                "scMaxEachPostBytes",
                "scMinPostsIntervalMs",
            }:
                try:
                    value = int(value)
                except ValueError:
                    continue

            xhttp[target] = value

        interval = xhttp.get(
            "sc-min-posts-interval-ms"
        )

        if (
            interval is None
            or interval <= 0
        ):
            xhttp[
                "sc-min-posts-interval-ms"
            ] = 30

        reuse = {}

        reuse_fields = [
            (
                "maxConcurrency",
                "max-concurrency",
            ),
            (
                "maxConnections",
                "max-connections",
            ),
            (
                "cMaxReuseTimes",
                "c-max-reuse-times",
            ),
            (
                "hMaxRequestTimes",
                "h-max-request-times",
            ),
            (
                "hMaxReusableSecs",
                "h-max-reusable-secs",
            ),
            (
                "hKeepAlivePeriod",
                "h-keep-alive-period",
            ),
        ]

        for source, target in reuse_fields:
            value = get_first(
                query,
                source,
            )

            if value is not None:
                reuse[target] = value

        if reuse:
            xhttp[
                "reuse-settings"
            ] = reuse

        if xhttp:
            proxy[
                "xhttp-opts"
            ] = xhttp

    return proxy, None


# ============================================================
# DEDUPLICATION / NAMING
# ============================================================

def connection_key(proxy):
    ignored = {
        "name",
    }

    def freeze(value):
        if isinstance(
            value,
            dict,
        ):
            return tuple(
                sorted(
                    (
                        key,
                        freeze(val),
                    )
                    for key, val
                    in value.items()
                    if key not in ignored
                )
            )

        if isinstance(
            value,
            list,
        ):
            return tuple(
                freeze(item)
                for item in value
            )

        return value

    return freeze(proxy)


def assign_names(proxies):
    counters = Counter()

    for proxy in proxies:
        country = get_country(
            proxy["name"]
        )

        counters[country] += 1

        proxy["name"] = (
            f"{country} "
            f"{counters[country]}"
        )

    return proxies


# ============================================================
# CONFIG GENERATION
# ============================================================

def build_config(proxies):
    names = [
        proxy["name"]
        for proxy in proxies
    ]

    return {
        "mixed-port": 7890,

        "mode": "rule",

        "proxies": proxies,

        "proxy-groups": [
            {
                "name": GROUP_NAME,
                "type": "select",
                "proxies": (
                    names + ["DIRECT"]
                ),
            }
        ],

        "rules": [
            f"MATCH,{GROUP_NAME}"
        ],
    }


# ============================================================
# VALIDATION
# ============================================================

def validate_proxy(
    proxy,
    index,
):
    errors = []

    required = {
        "name",
        "type",
        "server",
        "port",
        "uuid",
    }

    missing = (
        required
        - set(proxy)
    )

    if missing:
        errors.append(
            f"proxy #{index}: "
            f"missing "
            f"{sorted(missing)}"
        )

    if proxy.get("type") != "vless":
        errors.append(
            f"proxy #{index}: "
            f"unsupported type"
        )

    uuid = proxy.get(
        "uuid"
    )

    if (
        uuid
        and not valid_uuid(uuid)
    ):
        errors.append(
            f"proxy #{index}: "
            f"invalid UUID"
        )

    server = proxy.get(
        "server"
    )

    if (
        not isinstance(
            server,
            str,
        )
        or not server
    ):
        errors.append(
            f"proxy #{index}: "
            f"invalid server"
        )

    port = proxy.get(
        "port"
    )

    if not isinstance(
        port,
        int,
    ):
        errors.append(
            f"proxy #{index}: "
            f"invalid port"
        )

    elif not 1 <= port <= 65535:
        errors.append(
            f"proxy #{index}: "
            f"invalid port"
        )

    network = proxy.get(
        "network"
    )

    if (
        network
        and network
        not in VALID_NETWORKS
    ):
        errors.append(
            f"proxy #{index}: "
            f"invalid network"
        )

    reality = proxy.get(
        "reality-opts"
    )

    if reality:
        if not isinstance(
            reality,
            dict,
        ):
            errors.append(
                f"proxy #{index}: "
                f"invalid reality-opts"
            )

        else:
            public_key = (
                reality.get(
                    "public-key"
                )
            )

            if not public_key:
                errors.append(
                    f"proxy #{index}: "
                    f"missing Reality key"
                )

            elif not valid_reality_key(
                public_key
            ):
                errors.append(
                    f"proxy #{index}: "
                    f"invalid Reality key"
                )

    return errors


def validate_config(config):
    errors = []

    if not isinstance(
        config,
        dict,
    ):
        return [
            "config root is not mapping"
        ]

    proxies = config.get(
        "proxies"
    )

    if not isinstance(
        proxies,
        list,
    ):
        return [
            "proxies must be list"
        ]

    names = []

    for index, proxy in enumerate(
        proxies,
        start=1,
    ):
        if not isinstance(
            proxy,
            dict,
        ):
            errors.append(
                f"proxy #{index}: "
                f"not mapping"
            )
            continue

        errors.extend(
            validate_proxy(
                proxy,
                index,
            )
        )

        if proxy.get("name"):
            names.append(
                proxy["name"]
            )

    if len(names) != len(
        set(names)
    ):
        errors.append(
            "proxy names are not unique"
        )

    groups = config.get(
        "proxy-groups"
    )

    if not isinstance(
        groups,
        list,
    ):
        errors.append(
            "proxy-groups invalid"
        )

    else:
        group = None

        for item in groups:
            if (
                isinstance(
                    item,
                    dict,
                )
                and item.get("name")
                == GROUP_NAME
            ):
                group = item
                break

        if group is None:
            errors.append(
                f"group '{GROUP_NAME}' missing"
            )

        else:
            expected = (
                names + ["DIRECT"]
            )

            if group.get(
                "proxies"
            ) != expected:
                errors.append(
                    "proxy group does "
                    "not match proxies"
                )

    expected_rules = [
        f"MATCH,{GROUP_NAME}"
    ]

    if config.get(
        "rules"
    ) != expected_rules:
        errors.append(
            "rules are invalid"
        )

    return errors


# ============================================================
# MIHOMO API
# ============================================================

def api_url(
    path,
    params=None,
):
    url = (
        f"http://{API_HOST}:"
        f"{API_PORT}{path}"
    )

    if params:
        url += "?" + (
            urllib.parse.urlencode(
                params
            )
        )

    return url


def api_get(
    path,
    params=None,
):
    request = urllib.request.Request(
        api_url(
            path,
            params,
        ),
        headers={
            "User-Agent":
                "KafkaSubChecker/4.0"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=REQUEST_TIMEOUT,
    ) as response:
        return response.read().decode(
            "utf-8",
            errors="replace",
        )


def create_check_config(
    config
):
    check_config = dict(
        config
    )

    check_config[
        "external-controller"
    ] = (
        f"{API_HOST}:{API_PORT}"
    )

    check_config[
        "profile"
    ] = {
        "store-selected": False,
        "store-fake-ip": False,
    }

    with open(
        CHECK_CONFIG_FILE,
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:
        yaml.safe_dump(
            check_config,
            f,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )


def start_mihomo(config):
    print(
        "[+] Creating temporary "
        "Mihomo config..."
    )

    create_check_config(
        config
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
            CHECK_CONFIG_FILE,
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
            try:
                output = (
                    process.stdout.read()
                )
            except Exception:
                output = ""

            print()
            print(
                "[!] Mihomo exited "
                "during startup:"
            )
            print(output)

            return None

        try:
            api_get(
                "/proxies"
            )

            print(
                "[+] Mihomo API is ready."
            )

            return process

        except Exception:
            time.sleep(0.5)

    print()
    print(
        "[!] Mihomo API did not start."
    )

    try:
        process.kill()

        output = (
            process.stdout.read()
        )

        if output:
            print(output)

    except Exception:
        pass

    return None


# ============================================================
# NODE HEALTH CHECK
# ============================================================

def check_endpoint(
    name,
    url,
):
    encoded_name = (
        urllib.parse.quote(
            name,
            safe="",
        )
    )

    started = time.monotonic()

    try:
        data = api_get(
            f"/proxies/"
            f"{encoded_name}/delay",
            {
                "url": url,
                "timeout": TIMEOUT_MS,
                "expected": "204",
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
            return None

        elapsed = (
            time.monotonic()
            - started
        )

        # Не доверяем странным значениям
        # задержки.
        if delay <= 0:
            return None

        if delay > MAX_LATENCY_MS:
            return None

        return delay

    except Exception:
        return None


def check_one_round(
    name
):
    successful = []
    failed = []

    for url in TEST_URLS:
        delay = check_endpoint(
            name,
            url,
        )

        if delay is None:
            failed.append(url)

        else:
            successful.append(
                (
                    url,
                    delay,
                )
            )

    if (
        len(successful)
        < MIN_SUCCESSFUL_ENDPOINTS
    ):
        return {
            "ok": False,
            "delays": [],
            "successful": 0,
        }

    delays = [
        delay
        for _, delay
        in successful
    ]

    return {
        "ok": True,
        "delays": delays,
        "successful": len(
            successful
        ),
    }


def check_one_proxy(
    proxy
):
    name = proxy["name"]

    round_results = []

    for round_number in range(
        1,
        STABILITY_ROUNDS + 1,
    ):
        result = check_one_round(
            name
        )

        round_results.append(
            result
        )

        if not result["ok"]:
            return (
                proxy,
                False,
                None,
                round_results,
            )

        if (
            round_number
            < STABILITY_ROUNDS
        ):
            time.sleep(
                STABILITY_DELAY
            )

    all_delays = []

    for result in round_results:
        all_delays.extend(
            result["delays"]
        )

    if not all_delays:
        return (
            proxy,
            False,
            None,
            round_results,
        )

    # Берём среднее из успешных проверок.
    average_delay = (
        sum(all_delays)
        // len(all_delays)
    )

    return (
        proxy,
        True,
        average_delay,
        round_results,
    )


def check_proxies(
    proxies,
    config,
):
    mihomo = start_mihomo(
        config
    )

    if mihomo is None:
        return None, None

    results = {}

    print()
    print(
        "=== Health Check ==="
    )

    print(
        f"[+] Candidates: "
        f"{len(proxies)}"
    )

    print(
        f"[+] Endpoint tests: "
        f"{len(TEST_URLS)}"
    )

    print(
        f"[+] Required per round: "
        f"{MIN_SUCCESSFUL_ENDPOINTS}/"
        f"{len(TEST_URLS)}"
    )

    print(
        f"[+] Stability rounds: "
        f"{STABILITY_ROUNDS}"
    )

    print(
        f"[+] Max latency: "
        f"{MAX_LATENCY_MS} ms"
    )

    try:
        with ThreadPoolExecutor(
            max_workers=CHECK_WORKERS
        ) as executor:

            futures = {
                executor.submit(
                    check_one_proxy,
                    proxy,
                ): index
                for index, proxy
                in enumerate(
                    proxies,
                    start=1,
                )
            }

            completed = 0
            total = len(
                proxies
            )

            for future in as_completed(
                futures
            ):
                completed += 1

                index = futures[
                    future
                ]

                try:
                    (
                        proxy,
                        ok,
                        delay,
                        details,
                    ) = future.result()

                except Exception:
                    proxy = proxies[
                        index - 1
                    ]

                    ok = False
                    delay = None
                    details = []

                results[index] = (
                    proxy,
                    ok,
                    delay,
                    details,
                )

                if ok:
                    print(
                        f"[{completed}/{total}] "
                        f"OK   "
                        f"{proxy['name']} "
                        f"{delay} ms"
                    )

                else:
                    print(
                        f"[{completed}/{total}] "
                        f"FAIL "
                        f"{proxy['name']}"
                    )

    finally:
        print()
        print(
            "[+] Stopping Mihomo..."
        )

        try:
            mihomo.terminate()

            try:
                mihomo.wait(
                    timeout=5
                )

            except subprocess.TimeoutExpired:
                mihomo.kill()

        except Exception:
            pass

        if os.path.exists(
            CHECK_CONFIG_FILE
        ):
            try:
                os.remove(
                    CHECK_CONFIG_FILE
                )
            except Exception:
                pass

    alive = []
    dead = []

    for index in sorted(
        results
    ):
        (
            proxy,
            ok,
            delay,
            details,
        ) = results[index]

        if ok:
            alive.append(
                (
                    proxy,
                    delay,
                )
            )

        else:
            dead.append(
                proxy
            )

    return alive, dead


# ============================================================
# STATISTICS
# ============================================================

def print_statistics(
    proxies,
    rejected,
    duplicates,
    candidates,
    dead,
    delays,
):
    networks = Counter()
    security = Counter()
    countries = Counter()

    for proxy in proxies:
        network = proxy.get(
            "network",
            "tcp",
        )

        networks[network] += 1

        if proxy.get(
            "reality-opts"
        ):
            security[
                "Reality"
            ] += 1

        elif proxy.get(
            "tls"
        ):
            security[
                "TLS"
            ] += 1

        else:
            security[
                "No TLS"
            ] += 1

        countries[
            get_country(
                proxy["name"]
            )
        ] += 1

    print()
    print(
        "=== Final Statistics ==="
    )

    print(
        f"Candidates: "
        f"{candidates}"
    )

    print(
        f"Alive:      "
        f"{len(proxies)}"
    )

    print(
        f"Removed:    "
        f"{dead}"
    )

    print(
        f"Duplicates: "
        f"{duplicates}"
    )

    if delays:
        print()
        print(
            "Latency:"
        )

        print(
            f"  Minimum: "
            f"{min(delays)} ms"
        )

        print(
            f"  Maximum: "
            f"{max(delays)} ms"
        )

        print(
            f"  Average: "
            f"{sum(delays) // len(delays)} ms"
        )

    print()
    print(
        "Networks:"
    )

    network_names = {
        "tcp": "TCP",
        "ws": "WebSocket",
        "grpc": "gRPC",
        "xhttp": "XHTTP",
        "http": "HTTP",
        "h2": "H2",
    }

    for network, count in sorted(
        networks.items(),
        key=lambda item: (
            -item[1],
            item[0],
        ),
    ):
        print(
            f"  "
            f"{network_names.get(network, network)}: "
            f"{count}"
        )

    print()
    print(
        "Security:"
    )

    for key in [
        "Reality",
        "TLS",
        "No TLS",
    ]:
        if security[key]:
            print(
                f"  {key}: "
                f"{security[key]}"
            )

    print()
    print(
        "Countries:"
    )

    for country, count in sorted(
        countries.items(),
        key=lambda item: (
            -item[1],
            item[0],
        ),
    ):
        print(
            f"  {country}: "
            f"{count}"
        )

    print()
    print(
        "Rejected during parsing:"
    )

    if rejected:
        for reason, count in sorted(
            rejected.items(),
            key=lambda item: (
                -item[1],
                item[0],
            ),
        ):
            print(
                f"  {reason}: "
                f"{count}"
            )
    else:
        print(
            "  none"
        )


# ============================================================
# WRITE CONFIG
# ============================================================

def write_config(
    config
):
    temporary_file = (
        OUTPUT_FILE
        + ".tmp"
    )

    with open(
        temporary_file,
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:
        yaml.safe_dump(
            config,
            f,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )

    # Атомарная замена.
    os.replace(
        temporary_file,
        OUTPUT_FILE,
    )


# ============================================================
# MAIN
# ============================================================

def main():
    print(
        "=== Kafka Sub Builder ==="
    )
    print()

    if not os.path.exists(
        MIHOMO_BINARY
    ):
        print(
            f"[!] Mihomo not found: "
            f"{MIHOMO_BINARY}"
        )

        sys.exit(1)

    try:
        sources = read_sources()

    except Exception as e:
        print(
            f"[!] Failed to read "
            f"{SOURCES_FILE}: {e}"
        )

        sys.exit(1)

    if not sources:
        print(
            "[!] No sources."
        )

        sys.exit(1)

    print(
        f"[+] Sources: "
        f"{len(sources)}"
    )

    print()

    all_links = []

    for source in sources:
        try:
            data = download(
                source
            )

            links = (
                extract_links_from_source(
                    data
                )
            )

            print(
                f"[+] Found VLESS: "
                f"{len(links)}"
            )

            all_links.extend(
                links
            )

        except Exception as e:
            print(
                f"[!] Failed source:"
            )

            print(
                f"    {source}"
            )

            print(
                f"    {e}"
            )

    print()

    print(
        f"[+] Total VLESS: "
        f"{len(all_links)}"
    )

    # ========================================================
    # PARSE
    # ========================================================

    proxies = []
    rejected = Counter()

    for link in all_links:
        try:
            proxy, reason = (
                parse_vless(
                    link
                )
            )

            if proxy is None:
                rejected[
                    reason
                    or "unknown"
                ] += 1

                continue

            proxies.append(
                proxy
            )

        except Exception:
            rejected[
                "parse exception"
            ] += 1

    print(
        f"[+] Valid proxies: "
        f"{len(proxies)}"
    )

    print(
        f"[+] Rejected: "
        f"{sum(rejected.values())}"
    )

    # ========================================================
    # DEDUP
    # ========================================================

    unique = []
    seen = set()

    for proxy in proxies:
        key = connection_key(
            proxy
        )

        if key in seen:
            continue

        seen.add(key)

        unique.append(
            proxy
        )

    duplicates = (
        len(proxies)
        - len(unique)
    )

    print(
        f"[+] Duplicates removed: "
        f"{duplicates}"
    )

    if not unique:
        print()
        print(
            "[!] No valid proxies."
        )

        print(
            "[!] Existing "
            "config.yaml will "
            "NOT be modified."
        )

        sys.exit(1)

    # ========================================================
    # INITIAL NAMES
    # ========================================================

    unique = assign_names(
        unique
    )

    candidate_config = (
        build_config(
            unique
        )
    )

    validation_errors = (
        validate_config(
            candidate_config
        )
    )

    if validation_errors:
        print()
        print(
            "[!] Candidate config "
            "is invalid:"
        )

        for error in validation_errors:
            print(
                f"  - {error}"
            )

        sys.exit(1)

    # ========================================================
    # HEALTH CHECK
    # ========================================================

    (
        alive_result,
        dead_proxies,
    ) = check_proxies(
        unique,
        candidate_config,
    )

    if (
        alive_result is None
        and dead_proxies is None
    ):
        print()
        print(
            "[!] Health check "
            "failed completely."
        )

        print(
            "[!] Existing "
            "config.yaml will "
            "NOT be modified."
        )

        sys.exit(1)

    alive_proxies = [
        proxy
        for proxy, delay
        in alive_result
    ]

    delays = [
        delay
        for proxy, delay
        in alive_result
    ]

    checked_count = len(
        unique
    )

    alive_count = len(
        alive_proxies
    )

    dead_count = (
        checked_count
        - alive_count
    )

    print()
    print(
        "=== Health Result ==="
    )

    print(
        f"Checked: "
        f"{checked_count}"
    )

    print(
        f"Alive:   "
        f"{alive_count}"
    )

    print(
        f"Dead:    "
        f"{dead_count}"
    )

    # ========================================================
    # SAFETY CHECK
    # ========================================================

    minimum_by_percent = math.ceil(
        checked_count
        * MIN_ALIVE_PERCENT
    )

    minimum_alive = max(
        MIN_ALIVE_ABSOLUTE,
        minimum_by_percent,
    )

    print()
    print(
        f"Minimum required: "
        f"{minimum_alive}"
    )

    if (
        alive_count
        < minimum_alive
    ):
        print()
        print(
            "[!] Too few healthy "
            "proxies."
        )

        print(
            "[!] Possible source "
            "or network failure."
        )

        print(
            "[!] Existing "
            "config.yaml will "
            "NOT be modified."
        )

        sys.exit(1)

    # ========================================================
    # FINAL NAMES
    # ========================================================

    # После удаления мёртвых
    # перенумеровываем страны.
    alive_proxies = assign_names(
        alive_proxies
    )

    final_config = build_config(
        alive_proxies
    )

    final_errors = (
        validate_config(
            final_config
        )
    )

    if final_errors:
        print()
        print(
            "[!] Final config "
            "validation failed:"
        )

        for error in final_errors:
            print(
                f"  - {error}"
            )

        print()
        print(
            "[!] Existing "
            "config.yaml will "
            "NOT be modified."
        )

        sys.exit(1)

    # ========================================================
    # WRITE
    # ========================================================

    write_config(
        final_config
    )

    # Ещё раз читаем YAML
    # уже с диска.
    try:
        with open(
            OUTPUT_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            loaded = yaml.safe_load(
                f
            )

    except Exception as e:
        print(
            f"[!] Failed to reload "
            f"config.yaml: {e}"
        )

        sys.exit(1)

    reload_errors = validate_config(
        loaded
    )

    if reload_errors:
        print()
        print(
            "[!] Reloaded YAML "
            "validation failed:"
        )

        for error in reload_errors:
            print(
                f"  - {error}"
            )

        sys.exit(1)

    print()
    print(
        "[+] config.yaml written."
    )

    print(
        "[+] Final validation: OK"
    )

    print_statistics(
        alive_proxies,
        rejected,
        duplicates,
        checked_count,
        dead_count,
        delays,
    )

    print()
    print(
        "=== Build complete ==="
    )

    print(
        f"Working proxies: "
        f"{len(alive_proxies)}"
    )

    print(
        f"Output: "
        f"{OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
