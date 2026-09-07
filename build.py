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
            "User-Agent": "Mozilla/5.0 KafkaSubBuilder/2.0"
        },
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        data = response.read()

    return data.decode("utf-8", errors="replace")


def extract_vless(text):
    return re.findall(
        r"vless://[^\s\"'<>]+",
        text,
        flags=re.IGNORECASE,
    )


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
    pattern = (
        r"^[0-9a-fA-F]{8}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{12}$"
    )

    return bool(re.fullmatch(pattern, uuid))


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

    value = unquote(str(value))

    if value == "":
        return None

    return value


def get_first(query, key):
    values = query.get(key)

    if not values:
        return None

    return clean_query_value(values[0])


def is_dummy(parsed_url, query):
    hostname = parsed_url.hostname or ""

    try:
        port = parsed_url.port
    except ValueError:
        return True

    uuid = unquote(parsed_url.username or "")

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

    pbk = get_first(query, "pbk")

    if pbk and pbk.lower() in DUMMY_VALUES:
        return True

    sid = get_first(query, "sid")

    if sid and sid.lower() in DUMMY_VALUES:
        return True

    remark = unquote(parsed_url.fragment or "").strip()
    remark_lower = remark.lower()

    dummy_words = [
        "test",
        "testing",
        "example",
        "пустышка",
        "наш форум",
    ]

    if any(word in remark_lower for word in dummy_words):
        return True

    return False


def parse_vless(link):
    parsed = urlparse(link)

    if parsed.scheme.lower() != "vless":
        return None, "invalid scheme"

    try:
        query = parse_qs(
            parsed.query,
            keep_blank_values=True,
        )

        uuid = unquote(parsed.username or "")
        server = parsed.hostname
        port = parsed.port
        remark = unquote(parsed.fragment or "").strip()

    except Exception:
        return None, "malformed URL"

    if not server:
        return None, "missing server"

    if not port:
        return None, "missing port"

    if not valid_uuid(uuid):
        return None, "invalid UUID"

    if is_dummy(parsed, query):
        return None, "dummy/test"

    proxy = {
        "name": remark or "Европа",
        "type": "vless",
        "server": server,
        "port": port,
        "uuid": uuid,
    }

    flow = get_first(query, "flow")

    if flow:
        proxy["flow"] = flow

    encryption = get_first(query, "encryption")

    if encryption:
        proxy["encryption"] = encryption

    packet_encoding = get_first(
        query,
        "packetEncoding",
    )

    if packet_encoding:
        proxy["packet-encoding"] = packet_encoding

    security = get_first(query, "security")

    if security in VALID_SECURITY:
        proxy["tls"] = True

    servername = get_first(query, "sni")

    if servername:
        proxy["servername"] = servername

    fingerprint = get_first(query, "fp")

    if fingerprint:
        proxy["client-fingerprint"] = fingerprint

    pbk = get_first(query, "pbk")
    sid = get_first(query, "sid")

    if security == "reality" or pbk:
        if not pbk:
            return None, "Reality without public key"

        if not valid_reality_key(pbk):
            return None, "invalid Reality key"

        proxy["reality-opts"] = {
            "public-key": pbk
        }

        if sid:
            proxy["reality-opts"]["short-id"] = sid

    alpn = get_first(query, "alpn")

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
        proxy["skip-cert-verify"] = True

    network = get_first(query, "type")

    if network in VALID_NETWORKS:
        proxy["network"] = network

    if network == "ws":
        ws_path = get_first(query, "path")
        ws_host = get_first(query, "host")

        ws_opts = {}

        if ws_path:
            ws_opts["path"] = ws_path

        if ws_host:
            ws_opts["headers"] = {
                "Host": ws_host
            }

        if ws_opts:
            proxy["ws-opts"] = ws_opts

    if network == "grpc":
        service_name = get_first(
            query,
            "serviceName",
        )

        if service_name:
            proxy["grpc-opts"] = {
                "grpc-service-name": service_name
            }

    if network == "xhttp":
        xhttp = {}

        path = get_first(query, "path")
        host = get_first(query, "host")
        mode = get_first(query, "mode")

        if path:
            xhttp["path"] = path

        if host:
            xhttp["host"] = host

        if mode:
            xhttp["mode"] = mode

        xhttp_fields = [
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
        ]

        for source, target in xhttp_fields:
            value = get_first(query, source)

            if value is None:
                continue

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

        interval = xhttp.get(
            "sc-min-posts-interval-ms"
        )

        if interval is None or interval <= 0:
            xhttp["sc-min-posts-interval-ms"] = 30

        reuse = {}

        reuse_fields = [
            ("maxConcurrency", "max-concurrency"),
            ("maxConnections", "max-connections"),
            ("cMaxReuseTimes", "c-max-reuse-times"),
            ("hMaxRequestTimes", "h-max-request-times"),
            ("hMaxReusableSecs", "h-max-reusable-secs"),
            ("hKeepAlivePeriod", "h-keep-alive-period"),
        ]

        for source, target in reuse_fields:
            value = get_first(query, source)

            if value is not None:
                reuse[target] = value

        if reuse:
            xhttp["reuse-settings"] = reuse

        if xhttp:
            proxy["xhttp-opts"] = xhttp

    return proxy, None


def connection_key(proxy):
    ignored = {
        "name",
    }

    def freeze(value):
        if isinstance(value, dict):
            return tuple(
                sorted(
                    (
                        key,
                        freeze(val),
                    )
                    for key, val in value.items()
                    if key not in ignored
                )
            )

        if isinstance(value, list):
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


def validate_proxy(proxy, index):
    errors = []

    required = {
        "name",
        "type",
        "server",
        "port",
        "uuid",
    }

    missing = required - set(proxy)

    if missing:
        errors.append(
            f"proxy #{index}: "
            f"missing {sorted(missing)}"
        )

    if proxy.get("type") != "vless":
        errors.append(
            f"proxy #{index}: "
            f"unsupported type"
        )

    uuid = proxy.get("uuid")

    if uuid and not valid_uuid(uuid):
        errors.append(
            f"proxy #{index}: "
            f"invalid UUID"
        )

    server = proxy.get("server")

    if not isinstance(server, str) or not server:
        errors.append(
            f"proxy #{index}: "
            f"invalid server"
        )

    port = proxy.get("port")

    if not isinstance(port, int):
        errors.append(
            f"proxy #{index}: "
            f"invalid port"
        )
    elif not 1 <= port <= 65535:
        errors.append(
            f"proxy #{index}: "
            f"port outside 1-65535"
        )

    network = proxy.get("network")

    if network and network not in VALID_NETWORKS:
        errors.append(
            f"proxy #{index}: "
            f"invalid network {network}"
        )

    if proxy.get("tls") is True:
        security_fields = {
            "servername",
            "reality-opts",
        }

        if not (
            "servername" in proxy
            or "reality-opts" in proxy
        ):
            errors.append(
                f"proxy #{index}: "
                f"TLS enabled without "
                f"servername/reality-opts"
            )

    reality = proxy.get("reality-opts")

    if reality:
        if not isinstance(reality, dict):
            errors.append(
                f"proxy #{index}: "
                f"reality-opts is not a mapping"
            )
        else:
            public_key = reality.get(
                "public-key"
            )

            if not public_key:
                errors.append(
                    f"proxy #{index}: "
                    f"Reality public-key missing"
                )
            elif not valid_reality_key(
                public_key
            ):
                errors.append(
                    f"proxy #{index}: "
                    f"invalid Reality public-key"
                )

    alpn = proxy.get("alpn")

    if alpn is not None:
        if not isinstance(alpn, list):
            errors.append(
                f"proxy #{index}: "
                f"alpn must be a list"
            )

        elif any(
            not isinstance(item, str)
            for item in alpn
        ):
            errors.append(
                f"proxy #{index}: "
                f"alpn contains non-string"
            )

    ws_opts = proxy.get("ws-opts")

    if ws_opts is not None:
        if not isinstance(ws_opts, dict):
            errors.append(
                f"proxy #{index}: "
                f"ws-opts must be a mapping"
            )

    grpc_opts = proxy.get("grpc-opts")

    if grpc_opts is not None:
        if not isinstance(
            grpc_opts,
            dict,
        ):
            errors.append(
                f"proxy #{index}: "
                f"grpc-opts must be a mapping"
            )

    xhttp_opts = proxy.get(
        "xhttp-opts"
    )

    if xhttp_opts is not None:
        if not isinstance(
            xhttp_opts,
            dict,
        ):
            errors.append(
                f"proxy #{index}: "
                f"xhttp-opts must be a mapping"
            )

        else:
            interval = xhttp_opts.get(
                "sc-min-posts-interval-ms"
            )

            if interval is not None:
                if not isinstance(
                    interval,
                    int,
                ):
                    errors.append(
                        f"proxy #{index}: "
                        f"XHTTP interval "
                        f"must be integer"
                    )

                elif interval <= 0:
                    errors.append(
                        f"proxy #{index}: "
                        f"XHTTP interval "
                        f"must be > 0"
                    )

    return errors


def validate_config(config):
    errors = []

    if not isinstance(config, dict):
        return [
            "config root is not a mapping"
        ]

    if config.get("mixed-port") != 7890:
        errors.append(
            "mixed-port must be 7890"
        )

    if config.get("mode") != "rule":
        errors.append(
            "mode must be rule"
        )

    proxies = config.get("proxies")

    if not isinstance(proxies, list):
        errors.append(
            "proxies must be a list"
        )
        return errors

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
                f"not a mapping"
            )
            continue

        errors.extend(
            validate_proxy(
                proxy,
                index,
            )
        )

        name = proxy.get("name")

        if name:
            names.append(name)

    if len(names) != len(set(names)):
        errors.append(
            "proxy names are not unique"
        )

    groups = config.get(
        "proxy-groups"
    )

    if not isinstance(groups, list):
        errors.append(
            "proxy-groups must be a list"
        )

    else:
        target_group = None

        for group in groups:
            if (
                isinstance(group, dict)
                and group.get("name")
                == GROUP_NAME
            ):
                target_group = group
                break

        if target_group is None:
            errors.append(
                f"proxy group "
                f"'{GROUP_NAME}' missing"
            )

        else:
            group_proxies = (
                target_group.get("proxies")
            )

            if not isinstance(
                group_proxies,
                list,
            ):
                errors.append(
                    "proxy group proxies "
                    "must be a list"
                )

            else:
                expected = (
                    names + ["DIRECT"]
                )

                if group_proxies != expected:
                    errors.append(
                        "proxy group does not "
                        "match proxy list"
                    )

    rules = config.get("rules")

    if rules != [
        f"MATCH,{GROUP_NAME}"
    ]:
        errors.append(
            "rules are invalid"
        )

    return errors


def get_statistics(proxies):
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
            security["Reality"] += 1

        elif proxy.get("tls"):
            security["TLS"] += 1

        else:
            security["No TLS"] += 1

        countries[
            get_country(
                proxy["name"]
            )
        ] += 1

    return (
        networks,
        security,
        countries,
    )


def print_statistics(
    proxies,
    rejected_reasons,
    duplicates,
):
    networks, security, countries = (
        get_statistics(proxies)
    )

    print()
    print("=== Statistics ===")

    print(
        f"Final proxies: {len(proxies)}"
    )

    print(
        f"Duplicates removed: "
        f"{duplicates}"
    )

    print()

    print("Networks:")

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
            f"  {network_names.get(network, network)}: "
            f"{count}"
        )

    print()

    print("Security:")

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

    print("Rejected:")

    if rejected_reasons:
        for reason, count in sorted(
            rejected_reasons.items(),
            key=lambda item: (
                -item[1],
                item[0],
            ),
        ):
            print(
                f"  {reason}: {count}"
            )
    else:
        print("  none")

    print()

    print("Countries:")

    for country, count in sorted(
        countries.items(),
        key=lambda item: (
            -item[1],
            item[0],
        ),
    ):
        print(
            f"  {country}: {count}"
        )


def write_config(config):
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


def reload_and_validate_yaml():
    try:
        with open(
            OUTPUT_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            loaded = yaml.safe_load(f)

    except Exception as e:
        return [
            f"YAML parse error: {e}"
        ]

    return validate_config(loaded)


def main():
    print("=== Kafka Sub Builder ===")
    print()

    try:
        sources = read_sources()

    except FileNotFoundError:
        print(
            f"[!] {SOURCES_FILE} not found"
        )
        sys.exit(1)

    if not sources:
        print(
            f"[!] {SOURCES_FILE} is empty"
        )
        sys.exit(1)

    print(
        f"[+] Sources: {len(sources)}"
    )
    print()

    all_links = []
    source_stats = []

    for source in sources:
        try:
            text = download(source)
            links = extract_vless(text)

            source_stats.append(
                (
                    source,
                    len(links),
                )
            )

            print(
                f"[+] Found VLESS: "
                f"{len(links)}"
            )

            all_links.extend(links)

        except Exception as e:
            print(
                f"[!] Failed to download "
                f"{source}: {e}"
            )

            source_stats.append(
                (
                    source,
                    0,
                )
            )

    print()

    print(
        f"[+] Total VLESS: "
        f"{len(all_links)}"
    )

    proxies = []
    rejected_reasons = Counter()

    for link in all_links:
        try:
            proxy, reason = parse_vless(
                link
            )

            if proxy is None:
                rejected_reasons[
                    reason or "unknown"
                ] += 1
                continue

            proxies.append(proxy)

        except Exception as e:
            rejected_reasons[
                "parse exception"
            ] += 1

            print(
                f"[!] Parse error: {e}"
            )

    print(
        f"[+] Valid proxies: "
        f"{len(proxies)}"
    )

    rejected = sum(
        rejected_reasons.values()
    )

    print(
        f"[+] Rejected: {rejected}"
    )

    unique = []
    seen = set()

    for proxy in proxies:
        key = connection_key(proxy)

        if key in seen:
            continue

        seen.add(key)
        unique.append(proxy)

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
            "[!] No valid proxies found."
        )
        print(
            "[!] Refusing to overwrite "
            f"{OUTPUT_FILE}."
        )
        sys.exit(1)

    proxies = assign_names(
        unique
    )

    config = build_config(
        proxies
    )

    print()
    print(
        "[+] Validating generated config..."
    )

    validation_errors = (
        validate_config(config)
    )

    if validation_errors:
        print()
        print(
            "[!] Validation failed:"
        )

        for error in validation_errors:
            print(
                f"  - {error}"
            )

        print()
        print(
            "[!] Refusing to write "
            f"{OUTPUT_FILE}."
        )

        sys.exit(1)

    write_config(config)

    print(
        "[+] config.yaml written."
    )

    print(
        "[+] Reloading YAML "
        "for final validation..."
    )

    yaml_errors = (
        reload_and_validate_yaml()
    )

    if yaml_errors:
        print()
        print(
            "[!] Final YAML validation "
            "failed:"
        )

        for error in yaml_errors:
            print(
                f"  - {error}"
            )

        print()
        print(
            "[!] Generated config is "
            "invalid."
        )

        sys.exit(1)

    print(
        "[+] Final validation: OK"
    )

    print_statistics(
        proxies,
        rejected_reasons,
        duplicates,
    )

    print()
    print("=== Build complete ===")
    print(
        f"Proxies: {len(proxies)}"
    )
    print(
        f"Output:  {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
