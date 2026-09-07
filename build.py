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

# Только один endpoint
TEST_URL = "https://www.gstatic.com/generate_204"
EXPECTED_STATUS = "204"

# Максимальный допустимый пинг
MAX_LATENCY_MS = 5000

# Сколько раз проверять каждый узел
CHECK_ATTEMPTS = 3

# Сколько успешных проверок необходимо
REQUIRED_SUCCESSES = 2

# Таймаут одного запроса
REQUEST_TIMEOUT_MS = 5000

# Параллельные проверки
MAX_WORKERS = 10

# Время ожидания запуска Mihomo
STARTUP_TIMEOUT = 20

# Минимальная доля живых узлов.
# Если живых меньше этого количества — считаем проверку подозрительной
# и НЕ трогаем старый config.yaml.
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
            "User-Agent": "KafkaSubBuilder/2.0"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=30,
    ) as response:
        data = response.read()

    return data


def decode_possible_base64(text):
    text = text.strip()

    if not text:
        return text

    # Если это явно VLESS — не трогаем
    if "vless://" in text.lower():
        return text

    try:
        compact = re.sub(r"\s+", "", text)

        # Base64 должен иметь адекватную длину
        if len(compact) < 20:
            return text

        padding = "=" * (-len(compact) % 4)

        decoded = base64.b64decode(
            compact + padding,
            validate=False,
        )

        decoded_text = decoded.decode(
            "utf-8",
            errors="ignore",
        )

        if "vless://" in decoded_text.lower():
            return decoded_text

    except Exception:
        pass

    return text


def extract_vless(text):
    found = []

    text = decode_possible_base64(text)

    for match in VLESS_RE.findall(text):
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
            result.extend(extract_from_json(item))

    elif isinstance(value, list):
        for item in value:
            result.extend(extract_from_json(item))

    return result


def parse_source(data):
    text = data.decode(
        "utf-8",
        errors="ignore",
    )

    result = []

    # Обычный текст
    result.extend(extract_vless(text))

    # JSON
    try:
        parsed = json.loads(text)
        result.extend(extract_from_json(parsed))
    except Exception:
        pass

    # YAML
    try:
        parsed = yaml.safe_load(text)

        if parsed is not None:
            result.extend(extract_from_json(parsed))

    except Exception:
        pass

    return list(dict.fromkeys(result))


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

        if not parsed.port:
            return None, "missing port"

        uuid = urllib.parse.unquote(parsed.username or "")

        if not UUID_RE.match(uuid):
            return None, "invalid UUID"

        query = urllib.parse.parse_qs(
            parsed.query,
            keep_blank_values=True,
        )

        def get(name, default=None):
            values = query.get(name)

            if not values:
                return default

            return values[0]

        proxy = {
            "type": "vless",
            "server": parsed.hostname,
            "port": parsed.port,
            "uuid": uuid,
        }

        encryption = get("encryption")

        if encryption:
            proxy["encryption"] = encryption

        flow = get("flow")

        if flow:
            proxy["flow"] = flow

        security = get("security")

        if security == "tls":
            proxy["tls"] = True

            servername = get("sni") or get("servername")

            if servername:
                proxy["servername"] = servername

            fingerprint = get("fp")

            if fingerprint:
                proxy["client-fingerprint"] = fingerprint

            alpn = get("alpn")

            if alpn:
                proxy["alpn"] = [
                    x.strip()
                    for x in alpn.split(",")
                    if x.strip()
                ]

        elif security == "reality":
            proxy["tls"] = True

            servername = get("sni") or get("servername")

            if servername:
                proxy["servername"] = servername

            fingerprint = get("fp")

            if fingerprint:
                proxy["client-fingerprint"] = fingerprint

            public_key = get("pbk") or get("public-key")

            if not public_key:
                return None, "missing Reality public key"

            if len(public_key) != 43:
                return None, "invalid Reality public key"

            short_id = get("sid") or get("short-id")

            reality = {
                "public-key": public_key,
            }

            if short_id:
                reality["short-id"] = short_id

            proxy["reality-opts"] = reality

        network = get("type", "tcp")

        proxy["network"] = network

        # --------------------------------------------------------
        # WebSocket
        # --------------------------------------------------------

        if network == "ws":
            ws_opts = {}

            path = get("path")

            if path:
                ws_opts["path"] = path

            host = get("host")

            if host:
                ws_opts["headers"] = {
                    "Host": host
                }

            if ws_opts:
                proxy["ws-opts"] = ws_opts

        # --------------------------------------------------------
        # gRPC
        # --------------------------------------------------------

        elif network == "grpc":
            grpc_service = (
                get("serviceName")
                or get("service-name")
            )

            if grpc_service:
                proxy["grpc-opts"] = {
                    "grpc-service-name": grpc_service
                }

        # --------------------------------------------------------
        # XHTTP
        # --------------------------------------------------------

        elif network == "xhttp":
            xhttp_opts = {}

            mappings = {
                "path": "path",
                "host": "host",
                "mode": "mode",
                "extra": "extra",
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
            }

            for source_key, target_key in mappings.items():
                value = get(source_key)

                if value is not None:
                    xhttp_opts[target_key] = value

            if xhttp_opts:
                proxy["xhttp-opts"] = xhttp_opts

        # --------------------------------------------------------
        # Name from URL
        # --------------------------------------------------------

        name = urllib.parse.unquote(
            parsed.fragment or ""
        ).strip()

        if name:
            proxy["_source_name"] = name

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
    )


def deduplicate(proxies):
    result = []
    seen = set()
    duplicates = 0

    for proxy in proxies:
        identity = proxy_identity(proxy)

        if identity in seen:
            duplicates += 1
            continue

        seen.add(identity)
        result.append(proxy)

    return result, duplicates


# ============================================================
# COUNTRY NAMES
# ============================================================

def detect_country(proxy):
    haystack = " ".join(
        [
            str(proxy.get("server", "")),
            str(proxy.get("servername", "")),
            str(proxy.get("_source_name", "")),
        ]
    ).lower()

    for country, keywords in COUNTRIES:
        for keyword in keywords:
            if keyword in haystack:
                return country

    return "Европа"


def assign_names(proxies):
    counters = {}

    for proxy in proxies:
        country = detect_country(proxy)

        counters[country] = (
            counters.get(country, 0) + 1
        )

        proxy["name"] = (
            f"{country} {counters[country]}"
        )

    return proxies


# ============================================================
# MIHOMO
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
            "User-Agent": "KafkaSubBuilder/2.0"
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
        )

    return path


def start_mihomo(proxies):
    check_config = create_check_config(
        proxies
    )

    print("[+] Starting Mihomo...")

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

    while time.time() - start < STARTUP_TIMEOUT:
        if process.poll() is not None:
            output = ""

            try:
                output = process.stdout.read()
            except Exception:
                pass

            os.remove(check_config)

            print()
            print("[!] Mihomo exited during startup:")
            print(output)

            sys.exit(1)

        try:
            api_get("/proxies")

            print("[+] Mihomo API is ready.")

            return process, check_config

        except Exception:
            time.sleep(0.5)

    print("[!] Mihomo API did not start.")

    process.kill()

    if os.path.exists(check_config):
        os.remove(check_config)

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
                "timeout": REQUEST_TIMEOUT_MS,
                "expected": EXPECTED_STATUS,
            },
        )

        result = json.loads(data)

        delay = result.get("delay")

        if not isinstance(delay, int):
            return False, None

        if delay <= 0:
            return False, None

        if delay > MAX_LATENCY_MS:
            return False, delay

        return True, delay

    except Exception:
        return False, None


def check_proxy(proxy):
    name = proxy["name"]

    successes = 0
    delays = []

    for attempt in range(
        1,
        CHECK_ATTEMPTS + 1,
    ):
        ok, delay = check_once(name)

        if ok:
            successes += 1
            delays.append(delay)

        # Если уже невозможно набрать нужное
        # количество успешных проверок — выходим
        remaining = (
            CHECK_ATTEMPTS - attempt
        )

        if (
            successes + remaining
            < REQUIRED_SUCCESSES
        ):
            break

    alive = (
        successes >= REQUIRED_SUCCESSES
    )

    if alive:
        average_delay = (
            sum(delays) // len(delays)
            if delays
            else None
        )
    else:
        average_delay = None

    return {
        "proxy": proxy,
        "alive": alive,
        "successes": successes,
        "attempts": CHECK_ATTEMPTS,
        "delay": average_delay,
    }


def health_check(proxies):
    print()
    print("=== Health Check ===")
    print()
    print(
        f"[+] Endpoint: {TEST_URL}"
    )
    print(
        f"[+] Attempts: {CHECK_ATTEMPTS}"
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

    process, check_config = start_mihomo(
        proxies
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
                result = future.result()

                proxy = result["proxy"]
                name = proxy["name"]

                if result["alive"]:
                    alive.append(proxy)

                    print(
                        f"[{index}/{total}] "
                        f"OK   {name} "
                        f"({result['successes']}/"
                        f"{result['attempts']}) "
                        f"{result['delay']} ms"
                    )

                else:
                    dead.append(proxy)

                    print(
                        f"[{index}/{total}] "
                        f"FAIL {name} "
                        f"({result['successes']}/"
                        f"{result['attempts']})"
                    )

    finally:
        print()
        print("[+] Stopping Mihomo...")

        process.terminate()

        try:
            process.wait(
                timeout=5
            )
        except subprocess.TimeoutExpired:
            process.kill()

        if os.path.exists(check_config):
            os.remove(check_config)

    return alive, dead


# ============================================================
# CONFIG GENERATION
# ============================================================

def generate_config(proxies):
    config = {
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

    return config


def validate_config(config):
    if not isinstance(config, dict):
        return False, "config is not a mapping"

    proxies = config.get("proxies")

    if not isinstance(proxies, list):
        return False, "proxies is not a list"

    if not proxies:
        return False, "no proxies"

    names = set()

    for proxy in proxies:
        if not isinstance(proxy, dict):
            return False, "invalid proxy object"

        name = proxy.get("name")

        if not name:
            return False, "proxy without name"

        if name in names:
            return False, (
                f"duplicate proxy name: {name}"
            )

        names.add(name)

        if proxy.get("type") != "vless":
            return False, (
                f"unsupported proxy type: "
                f"{proxy.get('type')}"
            )

    groups = config.get(
        "proxy-groups"
    )

    if not isinstance(groups, list):
        return False, "proxy-groups missing"

    return True, None


def atomic_write_config(config):
    directory = (
        os.path.dirname(
            os.path.abspath(CONFIG_FILE)
        )
        or "."
    )

    fd, temp_path = tempfile.mkstemp(
        prefix="config-",
        suffix=".yaml",
        dir=directory,
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

        # Проверяем то, что реально записали
        with open(
            temp_path,
            "r",
            encoding="utf-8",
        ) as f:
            reloaded = yaml.safe_load(f)

        valid, error = validate_config(
            reloaded
        )

        if not valid:
            raise RuntimeError(
                f"Final config validation failed: "
                f"{error}"
            )

        os.replace(
            temp_path,
            CONFIG_FILE,
        )

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# ============================================================
# STATISTICS
# ============================================================

def print_statistics(
    total_found,
    valid_count,
    rejected,
    duplicates,
    alive,
    dead,
):
    print()
    print("=== Statistics ===")
    print()

    print(
        f"Found VLESS:      {total_found}"
    )

    print(
        f"Valid before check: {valid_count}"
    )

    print(
        f"Alive:             {len(alive)}"
    )

    print(
        f"Dead:              {len(dead)}"
    )

    print(
        f"Duplicates removed: {duplicates}"
    )

    print()

    if rejected:
        print("Rejected:")

        for reason, count in sorted(
            rejected.items()
        ):
            print(
                f"  {reason}: {count}"
            )

        print()

    countries = {}

    for proxy in alive:
        country = detect_country(proxy)

        countries[country] = (
            countries.get(country, 0) + 1
        )

    if countries:
        print("Countries:")

        for country, count in sorted(
            countries.items(),
            key=lambda x: (-x[1], x[0])
        ):
            print(
                f"  {country}: {count}"
            )


# ============================================================
# MAIN
# ============================================================

def main():
    print("=== Kafka Sub Builder ===")
    print()

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
            data = download_source(source)

            found = parse_source(data)

            print(
                f"[+] Found VLESS: {len(found)}"
            )

            all_vless.extend(found)

        except Exception as e:
            print(
                f"[!] Failed: {e}"
            )

    all_vless = list(
        dict.fromkeys(all_vless)
    )

    print()
    print(
        f"[+] Total VLESS: "
        f"{len(all_vless)}"
    )
    print()

    # --------------------------------------------------------
    # Parse / validate
    # --------------------------------------------------------

    valid = []
    rejected = {}

    for url in all_vless:
        proxy, error = parse_vless(url)

        if proxy is None:
            rejected[error] = (
                rejected.get(error, 0) + 1
            )
            continue

        valid.append(proxy)

    print(
        f"[+] Valid proxies: "
        f"{len(valid)}"
    )

    # --------------------------------------------------------
    # Deduplicate
    # --------------------------------------------------------

    valid, duplicates = deduplicate(
        valid
    )

    print(
        f"[+] Duplicates removed: "
        f"{duplicates}"
    )

    # --------------------------------------------------------
    # Names
    # --------------------------------------------------------

    valid = assign_names(valid)

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
    # Health check
    # --------------------------------------------------------

    alive, dead = health_check(
        valid
    )

    # --------------------------------------------------------
    # Safety threshold
    # --------------------------------------------------------

    minimum_alive = max(
        MIN_ALIVE_ABSOLUTE,
        math.ceil(
            len(valid)
            * MIN_ALIVE_PERCENT
        ),
    )

    print()
    print(
        f"[+] Alive: "
        f"{len(alive)}/{len(valid)}"
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
            "[!] This update looks suspicious."
        )
        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )
        sys.exit(1)

    # --------------------------------------------------------
    # Reassign names after filtering
    # --------------------------------------------------------

    alive = assign_names(alive)

    # --------------------------------------------------------
    # Generate
    # --------------------------------------------------------

    final_config = generate_config(
        alive
    )

    print()
    print(
        "[+] Validating generated config..."
    )

    valid_config, error = validate_config(
        final_config
    )

    if not valid_config:
        print(
            f"[!] Validation failed: "
            f"{error}"
        )
        print(
            "[!] Existing config.yaml "
            "was NOT modified."
        )
        sys.exit(1)

    # --------------------------------------------------------
    # Write atomically
    # --------------------------------------------------------

    try:
        atomic_write_config(
            final_config
        )

    except Exception as e:
        print()
        print(
            f"[!] Failed to write config: "
            f"{e}"
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
