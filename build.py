#!/usr/bin/env python3
import base64
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request

import yaml


# ----------------------------- Settings -----------------------------

MIHOMO_PATH = "./mihomo"
OUTPUT_CONFIG = "config.yaml"
SOURCES_FILE = "sources.txt"

VLESS_RE = re.compile(r"vless://[^\s\"'<>]+")


# ----------------------------- Helpers ------------------------------

def log(message):
    print(message, flush=True)


def bval(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value

    value = str(value).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


def first(query, *keys):
    for key in keys:
        values = query.get(key)
        if values:
            return values[0]
    return None


def dec(value):
    if value is None:
        return None

    value = str(value)
    for _ in range(4):
        decoded = urllib.parse.unquote(value)
        if decoded == value:
            break
        value = decoded
    return value


def split_list(value):
    return [item.strip() for item in str(value).split(",") if item.strip()]


def json_decode(value):
    if not value:
        return None

    value = str(value)
    for _ in range(4):
        try:
            return json.loads(value)
        except Exception:
            decoded = urllib.parse.unquote(value)
            if decoded == value:
                break
            value = decoded

    return None


def download(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "kafka-sub-builder/3.0",
            "Accept": "*/*",
        },
    )

    with urllib.request.urlopen(request, timeout=SOURCE_TIMEOUT_SECONDS) as response:
        data = response.read(20 * 1024 * 1024 + 1)

    if len(data) > 20 * 1024 * 1024:
        raise ValueError("source is larger than 20 MiB")

    return data.decode("utf-8", errors="ignore")


# ----------------------------- Parsing -------------------------------

def extract(text):
    found = list(VLESS_RE.findall(text))

    compact = re.sub(r"\s+", "", text)

    # Many subscription endpoints return plain/base64 VLESS lists.
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            padded = compact + "=" * (-len(compact) % 4)
            raw = decoder(padded, validate=False)
            decoded = raw.decode("utf-8", errors="ignore")
            if "vless://" in decoded:
                found.extend(VLESS_RE.findall(decoded))
        except Exception:
            pass

    # Also accept JSON/YAML containers containing VLESS strings or
    # native Xray VLESS outbounds.
    for loader in (json.loads, yaml.safe_load):
        try:
            obj = loader(text)
        except Exception:
            continue

        def walk(value):
            if isinstance(value, str):
                found.extend(VLESS_RE.findall(value))
                return

            if isinstance(value, list):
                for item in value:
                    walk(item)
                return

            if not isinstance(value, dict):
                return

            if str(value.get("protocol", "")).lower() == "vless":
                try:
                    vnext = value["settings"]["vnext"][0]
                    user = vnext["users"][0]
                    stream = value.get("streamSettings", {}) or {}

                    network = stream.get("network", "tcp")
                    security = stream.get("security", "")

                    query = {
                        "type": network,
                        "security": security,
                    }

                    tls = stream.get("tlsSettings", {}) or {}
                    reality = stream.get("realitySettings", {}) or {}

                    for source_key, query_key in (
                        ("serverName", "sni"),
                        ("fingerprint", "fp"),
                        ("publicKey", "pbk"),
                        ("shortId", "sid"),
                        ("spiderX", "spx"),
                    ):
                        value2 = tls.get(source_key) or reality.get(source_key)
                        if value2:
                            query[query_key] = value2

                    ws = stream.get("wsSettings", {}) or {}
                    grpc = stream.get("grpcSettings", {}) or {}
                    h2 = stream.get("httpSettings", {}) or {}

                    if ws.get("path"):
                        query["path"] = ws["path"]
                    if (ws.get("headers") or {}).get("Host"):
                        query["host"] = ws["headers"]["Host"]
                    if grpc.get("serviceName"):
                        query["serviceName"] = grpc["serviceName"]
                    if grpc.get("authority"):
                        query["authority"] = grpc["authority"]
                    if h2.get("path"):
                        query["path"] = h2["path"]
                    if h2.get("host"):
                        query["host"] = ",".join(h2["host"]) if isinstance(h2["host"], list) else h2["host"]
                    if h2.get("method"):
                        query["method"] = h2["method"]

                    query_string = urllib.parse.urlencode(
                        {k: v for k, v in query.items() if v not in (None, "")}
                    )

                    uri = "vless://{}@{}:{}?{}#{}".format(
                        urllib.parse.quote(str(user["id"]), safe=""),
                        vnext["address"],
                        vnext["port"],
                        query_string,
                        urllib.parse.quote(
                            str(value.get("tag", vnext["address"])),
                            safe="",
                        ),
                    )
                    found.append(uri)
                except Exception:
                    pass

            for child in value.values():
                walk(child)

        walk(obj)

    result = []
    seen = set()

    for uri in found:
        uri = uri.strip().rstrip(".,;)")
        if uri and uri not in seen:
            seen.add(uri)
            result.append(uri)

    return result


def parse_vless(uri):
    try:
        parsed = urllib.parse.urlparse(uri)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

        if (
            parsed.scheme.lower() != "vless"
            or not parsed.hostname
            or not parsed.username
            or not parsed.port
        ):
            return None

        node = {
            "name": dec(parsed.fragment) or parsed.hostname,
            "type": "vless",
            "server": parsed.hostname,
            "port": parsed.port,
            "uuid": dec(parsed.username),
            "udp": True,
        }

        for source_key, target_key in (
            ("encryption", "encryption"),
            ("flow", "flow"),
            ("packet-encoding", "packet-encoding"),
        ):
            value = first(query, source_key)
            if value:
                node[target_key] = dec(value)

        value = first(query, "udp")
        if value is not None:
            boolean = bval(value)
            if boolean is not None:
                node["udp"] = boolean

        security = (first(query, "security") or "").lower()
        network = (first(query, "type", "network") or "tcp").lower()

        # Xray calls this transport "splithttp"; Mihomo calls it xhttp.
        if network == "splithttp":
            network = "xhttp"

        if network not in {"tcp", "ws", "grpc", "xhttp", "h2", "http"}:
            return None

        node["network"] = network

        if security in {"tls", "reality"}:
            node["tls"] = True

        for source_key, target_key in (
            ("sni", "servername"),
            ("servername", "servername"),
            ("fp", "client-fingerprint"),
            ("fingerprint", "client-fingerprint"),
        ):
            value = first(query, source_key)
            if value:
                node[target_key] = dec(value)

        value = first(query, "alpn")
        if value:
            node["alpn"] = split_list(dec(value))

        value = first(query, "allowInsecure", "skip-cert-verify", "skipCertVerify")
        if value is not None:
            boolean = bval(value)
            if boolean is not None:
                node["skip-cert-verify"] = boolean

        if security == "reality":
            reality_opts = {}

            # Reality short-id is a hexadecimal byte string. Mihomo/Xray
            # reject malformed values (non-hex, odd length, or > 16 chars).
            # Empty short-id is allowed, so validate it only when present.
            short_id_raw = first(query, "sid", "shortId")
            if short_id_raw is not None:
                short_id = dec(short_id_raw).strip()
                if short_id and (
                    len(short_id) > 16
                    or len(short_id) % 2 != 0
                    or not re.fullmatch(r"[0-9a-fA-F]+", short_id)
                ):
                    return None
                if short_id:
                    reality_opts["short-id"] = short_id

            for source_key, target_key in (
                ("pbk", "public-key"),
                ("publicKey", "public-key"),
                ("spx", "spider-x"),
                ("spiderX", "spider-x"),
            ):
                value = first(query, source_key)
                if value:
                    reality_opts[target_key] = dec(value)

            if reality_opts:
                node["reality-opts"] = reality_opts

        if network == "ws":
            opts = {}

            value = first(query, "path")
            if value:
                opts["path"] = dec(value)

            value = first(query, "host")
            if value:
                opts["headers"] = {"Host": dec(value)}

            if opts:
                node["ws-opts"] = opts

        elif network == "grpc":
            opts = {}

            value = first(query, "serviceName", "service-name")
            if value:
                opts["grpc-service-name"] = dec(value)

            value = first(query, "authority")
            if value:
                opts["grpc-authority"] = dec(value)

            value = first(query, "grpc-user-agent")
            if value:
                opts["grpc-user-agent"] = dec(value)

            if opts:
                node["grpc-opts"] = opts

        elif network == "http":
            opts = {}

            value = first(query, "method")
            if value:
                opts["method"] = dec(value)

            value = first(query, "path")
            if value:
                opts["path"] = split_list(dec(value))

            value = first(query, "host")
            if value:
                opts["headers"] = {"Host": split_list(dec(value))}

            if opts:
                node["http-opts"] = opts

        elif network == "h2":
            opts = {}

            value = first(query, "path")
            if value:
                opts["path"] = dec(value)

            value = first(query, "host")
            if value:
                opts["host"] = split_list(dec(value))

            if opts:
                node["h2-opts"] = opts

        elif network == "xhttp":
            opts = {}

            # Direct URI parameters.
            for key in (
                "path",
                "host",
                "mode",
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
                "session-key",
                "session-table",
                "session-length",
                "seq-key",
                "uplink-data-placement",
                "uplink-data-key",
                "uplink-chunk-size",
                "sc-max-each-post-bytes",
                "sc-min-posts-interval-ms",
            ):
                value = first(query, key)
                if value is not None:
                    opts[key] = dec(value)

            # Common aliases used in Xray's "extra" JSON.
            extra = json_decode(first(query, "extra"))
            if isinstance(extra, dict):
                aliases = {
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
                    "sessionKey": "session-key",
                    "sessionTable": "session-table",
                    "sessionLength": "session-length",
                    "seqKey": "seq-key",
                    "uplinkDataPlacement": "uplink-data-placement",
                    "uplinkDataKey": "uplink-data-key",
                    "uplinkChunkSize": "uplink-chunk-size",
                    "scMaxEachPostBytes": "sc-max-each-post-bytes",
                    "scMinPostsIntervalMs": "sc-min-posts-interval-ms",
                    "reuseSettings": "no-op-reuse-settings",
                    "downloadSettings": "no-op-download-settings",
                }

                for source_key, target_key in aliases.items():
                    if source_key in extra:
                        # reuse/download settings are nested XHTTP objects and
                        # are handled below instead of being copied blindly.
                        if target_key.startswith("no-op-"):
                            continue
                        opts[target_key] = extra[source_key]

                if "headers" in extra and isinstance(extra["headers"], dict):
                    opts["headers"] = extra["headers"]

                # Preserve nested XHTTP settings when present.
                if isinstance(extra.get("xmux"), dict):
                    opts["xmux"] = extra["xmux"]
                if isinstance(extra.get("downloadSettings"), dict):
                    opts["download-settings"] = extra["downloadSettings"]
                if isinstance(extra.get("reuseSettings"), dict):
                    opts["reuse-settings"] = extra["reuseSettings"]

            if opts:
                node["xhttp-opts"] = opts

        return node

    except Exception:
        return None


# ----------------------------- Output --------------------------------

def identity(node):
    value = dict(node)
    value.pop("name", None)
    value.pop("_delay", None)
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def name_nodes(nodes):
    used = set()
    nodes.sort(key=lambda item: (item["server"], item["port"], item["uuid"]))

    for node in nodes:
        base = re.sub(
            r"\s+",
            " ",
            str(node.get("name") or node["server"]).strip(),
        )[:150] or "VLESS"

        name = base
        index = 2

        while name in used:
            name = f"{base} {index}"
            index += 1

        used.add(name)
        node["name"] = name


def make_config(nodes, controller=None):
    config = {
        "mixed-port": 7890,
        "mode": "rule",
        "proxies": nodes,
        "proxy-groups": [
            {
                "name": "🚀 Freedom Rudy",
                "type": "select",
                "proxies": [node["name"] for node in nodes] + ["DIRECT"],
            }
        ],
        "rules": ["MATCH,🚀 Freedom Rudy"],
    }

    if controller:
        config["external-controller"] = controller

    return config


def validate_config(path):
    result = subprocess.run(
        [MIHOMO_PATH, "-t", "-f", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
    )

    print(result.stdout)

    if result.returncode != 0:
        raise RuntimeError("Mihomo rejected config")


_PROXY_ERROR_RE = re.compile(r"\bproxy\s+(\d+)\s*:", re.IGNORECASE)


def filter_mihomo_invalid_nodes(nodes):
    """Remove Mihomo-invalid proxies without recursively validating batches.

    Mihomo reports the failing proxy index as ``proxy N: ...``.  We use that
    index to remove the exact bad node, then validate the complete config
    again.  This is intentionally linear in the number of bad nodes instead
    of recursively re-validating hundreds of sub-configs.
    """
    nodes = list(nodes)
    if not nodes:
        return []

    removed = []
    max_invalid = max(50, math.ceil(len(nodes) * 0.10))
    validation_runs = 0

    while nodes:
        fd, path = tempfile.mkstemp(suffix=".yaml")
        os.close(fd)
        try:
            with open(path, "w", encoding="utf-8") as file:
                yaml.safe_dump(
                    make_config(nodes),
                    file,
                    allow_unicode=True,
                    sort_keys=False,
                )

            validation_runs += 1
            try:
                result = subprocess.run(
                    [MIHOMO_PATH, "-t", "-f", path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=20,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(
                    "Mihomo config validation timed out while filtering invalid nodes"
                )

            output = result.stdout or ""
            if result.returncode == 0:
                log(
                    f"[FILTER] Mihomo validation passed: {len(nodes)} nodes; "
                    f"removed={len(removed)}; validation runs={validation_runs}"
                )
                return nodes

            match = _PROXY_ERROR_RE.search(output)
            if not match:
                print(output)
                raise RuntimeError(
                    "Mihomo rejected config, but did not report a proxy index"
                )

            # Mihomo reports proxy numbers as 1-based (proxy 1 is the
            # first proxy). Convert that number to the Python 0-based index.
            reported_index = int(match.group(1))
            index = reported_index - 1
            if index < 0 or index >= len(nodes):
                print(output)
                raise RuntimeError(
                    f"Mihomo reported invalid proxy index {reported_index}, "
                    f"but current list contains {len(nodes)} nodes"
                )

            bad = nodes.pop(index)
            last_line = output.strip().splitlines()[-1] if output.strip() else "unknown Mihomo error"
            removed.append(bad)
            log(
                f"[FILTER] removed proxy {reported_index}: {bad['name']} | {last_line} | "
                f"remaining={len(nodes)}"
            )

            if len(removed) > max_invalid:
                raise RuntimeError(
                    f"Too many Mihomo-invalid nodes ({len(removed)}); "
                    "aborting to avoid publishing a suspicious subscription"
                )
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    raise RuntimeError("All VLESS nodes were rejected by Mihomo")


# ----------------------------- Main ----------------------------------

def main():
    if not os.path.exists(SOURCES_FILE):
        raise FileNotFoundError(SOURCES_FILE)

    with open(SOURCES_FILE, encoding="utf-8") as file:
        sources = [
            line.strip()
            for line in file
            if line.strip() and not line.lstrip().startswith("#")
        ]

    if not sources:
        raise RuntimeError("sources.txt is empty")

    uris = []

    for source in sources:
        try:
            extracted = extract(download(source))
            log(f"[SOURCE] {source} -> {len(extracted)} VLESS")
            uris.extend(extracted)
        except Exception as error:
            log(f"[SOURCE ERROR] {source}: {error}")

    nodes = []
    seen = set()

    for uri in uris:
        node = parse_vless(uri)

        if node is None:
            continue

        key = identity(node)

        if key in seen:
            continue

        seen.add(key)
        nodes.append(node)

    if not nodes:
        raise RuntimeError("No valid VLESS nodes")

    name_nodes(nodes)

    log(f"[TOTAL] {len(nodes)} unique nodes")

    # No connectivity/health checks.
    # Keep only local parsing plus Mihomo config validation so malformed
    # proxies cannot break the whole generated subscription.
    nodes = filter_mihomo_invalid_nodes(nodes)
    alive = nodes

    log(f"[RESULT] published without connectivity check: {len(alive)} nodes")

    fd, temporary_config = tempfile.mkstemp(
        suffix=".yaml",
        dir=".",
    )
    os.close(fd)

    try:
        with open(temporary_config, "w", encoding="utf-8") as file:
            yaml.safe_dump(
                make_config(alive),
                file,
                allow_unicode=True,
                sort_keys=False,
            )

        validate_config(temporary_config)
        os.replace(temporary_config, OUTPUT_CONFIG)

    finally:
        if os.path.exists(temporary_config):
            os.unlink(temporary_config)

    log(f"[DONE] published {len(alive)} nodes")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"[FATAL] {error}", file=sys.stderr)
        sys.exit(1)
