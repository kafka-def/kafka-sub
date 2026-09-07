import re
import sys
import urllib.request
from urllib.parse import urlparse, parse_qs, unquote
from collections import Counter

import yaml


SOURCES_FILE = "sources.txt"
OUTPUT_FILE = "config.yaml"

GROUP_NAME = "🚀 Freedom Rudy"

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
    "🇻🇬": "Виргинские острова",
}


# Очевидные значения-пустышки.
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


def read_sources():
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        ]


def download(url):
    print(f"[+] Downloading: {url}")

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 KafkaSubBuilder/1.0"
        },
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        data = response.read()

    return data.decode("utf-8", errors="replace")


def extract_vless(text):
    """
    Извлекает VLESS-ссылки.
    Работает как с обычным списком строк,
    так и с текстом, где ссылки находятся вперемешку.
    """
    return re.findall(r"vless://[^\s\"'<>]+", text)


def get_country(remark):
    """
    Пытаемся получить страну из названия ноды.

    Приоритет:
    1. Флаг
    2. Название страны на русском
    3. Европа
    """

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


def is_dummy(parsed_url, query):
    """
    Отбрасывает очевидные тестовые/пустые конфиги.
    """

    hostname = parsed_url.hostname or ""
    port = parsed_url.port

    uuid = unquote(parsed_url.username or "")

    # UUID test / пустой UUID
    if uuid.lower() in DUMMY_VALUES:
        return True

    # Очевидные dummy-host
    if hostname.lower() in DUMMY_VALUES:
        return True

    # Очевидные тестовые порты
    if port in {1, 11, 123, 1234, 12344}:
        return True

    # Reality public key
    pbk = query.get("pbk", [""])[0]

    if pbk.lower() in DUMMY_VALUES:
        return True

    # Reality short id
    sid = query.get("sid", [""])[0]

    if sid.lower() in DUMMY_VALUES:
        return True

    # Очевидные рекламные/тестовые названия
    remark = unquote(parsed_url.fragment or "")

    dummy_words = [
        "test",
        "testing",
        "example",
        "пустышка",
        "наш форум",
    ]

    remark_lower = remark.lower()

    if any(word in remark_lower for word in dummy_words):
        return True

    return False


def valid_uuid(uuid):
    pattern = (
        r"^[0-9a-fA-F]{8}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{12}$"
    )

    return bool(re.match(pattern, uuid))


def valid_reality_key(key):
    """
    Reality public key в Mihomo должен быть
    URL-safe base64 строкой соответствующей длины.
    """

    return bool(re.fullmatch(r"[A-Za-z0-9_-]{43}", key))


def clean_query_value(value):
    if value is None:
        return None

    value = unquote(value)

    if value == "":
        return None

    return value


def parse_vless(link):
    """
    Преобразует vless:// URI в Mihomo proxy dictionary.
    """

    parsed = urlparse(link)

    if parsed.scheme.lower() != "vless":
        return None

    query = parse_qs(parsed.query, keep_blank_values=True)

    uuid = unquote(parsed.username or "")
    server = parsed.hostname
    port = parsed.port
    remark = unquote(parsed.fragment or "").strip()

    if not server or not port:
        return None

    if not valid_uuid(uuid):
        return None

    if is_dummy(parsed, query):
        return None

    proxy = {
        "name": remark or "Европа",
        "type": "vless",
        "server": server,
        "port": port,
        "uuid": uuid,
    }

    # -------------------------
    # Основные VLESS параметры
    # -------------------------

    flow = clean_query_value(query.get("flow", [None])[0])

    if flow:
        proxy["flow"] = flow

    encryption = clean_query_value(
        query.get("encryption", [None])[0]
    )

    if encryption:
        proxy["encryption"] = encryption

    packet_encoding = clean_query_value(
        query.get("packetEncoding", [None])[0]
    )

    if packet_encoding:
        proxy["packet-encoding"] = packet_encoding

    # -------------------------
    # TLS
    # -------------------------

    security = clean_query_value(
        query.get("security", [None])[0]
    )

    if security in {"tls", "reality"}:
        proxy["tls"] = True

    servername = clean_query_value(
        query.get("sni", [None])[0]
    )

    if servername:
        proxy["servername"] = servername

    fingerprint = clean_query_value(
        query.get("fp", [None])[0]
    )

    if fingerprint:
        proxy["client-fingerprint"] = fingerprint

    # -------------------------
    # Reality
    # -------------------------

    pbk = clean_query_value(
        query.get("pbk", [None])[0]
    )

    sid = clean_query_value(
        query.get("sid", [None])[0]
    )

    if security == "reality" or pbk:
        if not pbk:
            return None

        if not valid_reality_key(pbk):
            return None

        proxy["reality-opts"] = {
            "public-key": pbk
        }

        if sid:
            proxy["reality-opts"]["short-id"] = sid

    # -------------------------
    # ALPN
    # -------------------------

    alpn = clean_query_value(
        query.get("alpn", [None])[0]
    )

    if alpn:
        proxy["alpn"] = [
            item.strip()
            for item in alpn.split(",")
            if item.strip()
        ]

    # -------------------------
    # Skip cert verify
    # -------------------------

    allow_insecure = clean_query_value(
        query.get("allowInsecure", [None])[0]
    )

    if allow_insecure in {"1", "true", "True"}:
        proxy["skip-cert-verify"] = True

    # -------------------------
    # Network
    # -------------------------

    network = clean_query_value(
        query.get("type", [None])[0]
    )

    if network in {"ws", "grpc", "xhttp", "http", "h2"}:
        if network == "http":
            network = "http"

        proxy["network"] = network

    # -------------------------
    # WebSocket
    # -------------------------

    if network == "ws":
        ws_path = clean_query_value(
            query.get("path", [None])[0]
        )

        ws_host = clean_query_value(
            query.get("host", [None])[0]
        )

        ws_opts = {}

        if ws_path:
            ws_opts["path"] = ws_path

        if ws_host:
            ws_opts["headers"] = {
                "Host": ws_host
            }

        if ws_opts:
            proxy["ws-opts"] = ws_opts

    # -------------------------
    # gRPC
    # -------------------------

    if network == "grpc":
        service_name = clean_query_value(
            query.get("serviceName", [None])[0]
        )

        if service_name:
            proxy["grpc-opts"] = {
                "grpc-service-name": service_name
            }

    # -------------------------
    # XHTTP
    # -------------------------

    if network == "xhttp":
        xhttp = {}

        path = clean_query_value(
            query.get("path", [None])[0]
        )

        host = clean_query_value(
            query.get("host", [None])[0]
        )

        mode = clean_query_value(
            query.get("mode", [None])[0]
        )

        if path:
            xhttp["path"] = path

        if host:
            xhttp["host"] = host

        if mode:
            xhttp["mode"] = mode

        # Padding
        for source, target in [
            ("xPaddingBytes", "x-padding-bytes"),
            ("xPaddingObfsMode", "x-padding-obfs-mode"),
            ("xPaddingKey", "x-padding-key"),
            ("xPaddingHeader", "x-padding-header"),
            ("xPaddingPlacement", "x-padding-placement"),
            ("xPaddingMethod", "x-padding-method"),
            ("uplinkHTTPMethod", "uplink-http-method"),
            ("sessionIDPlacement", "session-placement"),
            ("sessionIDKey", "session-key"),
            ("sessionIDTable", "session-table"),
            ("sessionIDLength", "session-length"),
            ("seqPlacement", "seq-placement"),
            ("seqKey", "seq-key"),
            ("uplinkDataPlacement", "uplink-data-placement"),
            ("uplinkDataKey", "uplink-data-key"),
            ("uplinkChunkSize", "uplink-chunk-size"),
            ("scMaxEachPostBytes", "sc-max-each-post-bytes"),
            ("scMinPostsIntervalMs", "sc-min-posts-interval-ms"),
        ]:
            value = clean_query_value(
                query.get(source, [None])[0]
            )

            if value is not None:
                if source == "xPaddingObfsMode":
                    value = value.lower() == "true"

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

        # Mihomo требует положительное значение.
        # 30 — документированное значение по умолчанию.
        interval = xhttp.get("sc-min-posts-interval-ms")

        if interval is None or interval <= 0:
            xhttp["sc-min-posts-interval-ms"] = 30

        # XMUX / reuse settings
        reuse = {}

        for source, target in [
            ("maxConcurrency", "max-concurrency"),
            ("maxConnections", "max-connections"),
            ("cMaxReuseTimes", "c-max-reuse-times"),
            ("hMaxRequestTimes", "h-max-request-times"),
            ("hMaxReusableSecs", "h-max-reusable-secs"),
            ("hKeepAlivePeriod", "h-keep-alive-period"),
        ]:
            value = clean_query_value(
                query.get(source, [None])[0]
            )

            if value is not None:
                reuse[target] = value

        if reuse:
            xhttp["reuse-settings"] = reuse

        if xhttp:
            proxy["xhttp-opts"] = xhttp

    return proxy


def connection_key(proxy):
    """
    Ключ для удаления дубликатов.

    Имя ноды намеренно не учитывается:
    одна и та же нода с разными названиями считается дубликатом.
    """

    ignored = {"name"}

    def freeze(value):
        if isinstance(value, dict):
            return tuple(
                sorted(
                    (k, freeze(v))
                    for k, v in value.items()
                    if k not in ignored
                )
            )

        if isinstance(value, list):
            return tuple(freeze(x) for x in value)

        return value

    return freeze(proxy)


def assign_names(proxies):
    """
    Создаёт:

    Германия 1
    Германия 2
    Германия 3

    и т.д.
    """

    counters = Counter()

    for proxy in proxies:
        country = get_country(proxy["name"])

        counters[country] += 1

        proxy["name"] = f"{country} {counters[country]}"

    return proxies


def build_config(proxies):
    names = [proxy["name"] for proxy in proxies]

    config = {
        "mixed-port": 7890,
        "mode": "rule",
        "proxies": proxies,
        "proxy-groups": [
            {
                "name": GROUP_NAME,
                "type": "select",
                "proxies": names + ["DIRECT"],
            }
        ],
        "rules": [
            f"MATCH,{GROUP_NAME}"
        ],
    }

    return config


def main():
    print("=== Kafka Sub Builder ===")

    sources = read_sources()

    if not sources:
        print("[!] sources.txt is empty")
        sys.exit(1)

    all_links = []

    for source in sources:
        try:
            text = download(source)
            links = extract_vless(text)

            print(f"[+] Found VLESS: {len(links)}")

            all_links.extend(links)

        except Exception as e:
            print(f"[!] Failed to download {source}: {e}")

    print(f"[+] Total VLESS: {len(all_links)}")

    proxies = []

    rejected = 0

    for link in all_links:
        try:
            proxy = parse_vless(link)

            if proxy is None:
                rejected += 1
                continue

            proxies.append(proxy)

        except Exception as e:
            rejected += 1
            print(f"[!] Parse error: {e}")

    print(f"[+] Valid proxies: {len(proxies)}")
    print(f"[+] Rejected: {rejected}")

    # -------------------------
    # Deduplicate
    # -------------------------

    unique = []
    seen = set()

    for proxy in proxies:
        key = connection_key(proxy)

        if key in seen:
            continue

        seen.add(key)
        unique.append(proxy)

    duplicates = len(proxies) - len(unique)

    print(f"[+] Duplicates removed: {duplicates}")

    proxies = assign_names(unique)

    config = build_config(proxies)

    # -------------------------
    # Save YAML
    # -------------------------

    with open(
        OUTPUT_FILE,
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

    print()
    print("=== Build complete ===")
    print(f"Proxies: {len(proxies)}")
    print(f"Output:  {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
