import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import yaml


CONFIG_FILE = "config.yaml"
CHECK_CONFIG_FILE = "check-config.yaml"

MIHOMO_BINARY = "./mihomo"

API_HOST = "127.0.0.1"
API_PORT = 9090

TEST_URL = "https://www.gstatic.com/generate_204"
TIMEOUT_MS = 5000

STARTUP_TIMEOUT = 20
REQUEST_TIMEOUT = 10


def api_url(path, params=None):
    url = f"http://{API_HOST}:{API_PORT}{path}"

    if params:
        url += "?" + urllib.parse.urlencode(params)

    return url


def api_get(path, params=None):
    url = api_url(path, params)

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "KafkaSubChecker/1.0"
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


def load_config():
    with open(
        CONFIG_FILE,
        "r",
        encoding="utf-8",
    ) as f:
        return yaml.safe_load(f)


def create_check_config():
    config = load_config()

    # API нужен только для проверки.
    # В основной config.yaml он НЕ добавляется.
    config["external-controller"] = (
        f"{API_HOST}:{API_PORT}"
    )

    # Чтобы Mihomo не пытался сохранять
    # какие-либо изменения в основной конфиг.
    config["profile"] = {
        "store-selected": False,
        "store-fake-ip": False,
    }

    with open(
        CHECK_CONFIG_FILE,
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


def start_mihomo():
    print("[+] Creating temporary check config...")

    create_check_config()

    print("[+] Starting Mihomo...")

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

    while time.time() - start < STARTUP_TIMEOUT:
        if process.poll() is not None:
            output = process.stdout.read()

            print()
            print(
                "[!] Mihomo exited during startup:"
            )
            print(output)

            sys.exit(1)

        try:
            api_get("/proxies")

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

    print()
    print(
        "[!] Mihomo output:"
    )

    try:
        output = process.stdout.read(
            10000
        )
        print(output)
    except Exception:
        pass

    process.kill()

    sys.exit(1)


def check_proxy(name):
    encoded_name = urllib.parse.quote(
        name,
        safe="",
    )

    try:
        data = api_get(
            f"/proxies/{encoded_name}/delay",
            {
                "url": TEST_URL,
                "timeout": TIMEOUT_MS,
                "expected": "204",
            },
        )

        result = json.loads(data)

        delay = result.get("delay")

        if isinstance(delay, int):
            return True, delay

        return False, None

    except Exception:
        return False, None


def main():
    print("=== Kafka Sub Checker ===")
    print()

    try:
        config = load_config()

    except Exception as e:
        print(
            f"[!] Failed to load config.yaml: {e}"
        )
        sys.exit(1)

    proxies = config.get(
        "proxies",
        [],
    )

    if not proxies:
        print(
            "[!] No proxies found."
        )
        sys.exit(1)

    print(
        f"[+] Proxies to check: "
        f"{len(proxies)}"
    )

    print(
        f"[+] Test URL: {TEST_URL}"
    )

    print(
        f"[+] Timeout: {TIMEOUT_MS} ms"
    )

    print()

    mihomo = start_mihomo()

    alive = []
    dead = []

    try:
        total = len(proxies)

        for index, proxy in enumerate(
            proxies,
            start=1,
        ):
            name = proxy.get("name")

            if not name:
                continue

            ok, delay = check_proxy(name)

            if ok:
                alive.append(
                    {
                        "name": name,
                        "delay": delay,
                    }
                )

                print(
                    f"[{index}/{total}] "
                    f"OK   {name} "
                    f"{delay} ms"
                )

            else:
                dead.append(name)

                print(
                    f"[{index}/{total}] "
                    f"FAIL {name}"
                )

    finally:
        print()
        print(
            "[+] Stopping Mihomo..."
        )

        mihomo.terminate()

        try:
            mihomo.wait(
                timeout=5
            )

        except subprocess.TimeoutExpired:
            mihomo.kill()

        if os.path.exists(
            CHECK_CONFIG_FILE
        ):
            os.remove(
                CHECK_CONFIG_FILE
            )

    print()
    print(
        "=== Check complete ==="
    )

    print()
    print(
        f"Checked: {len(proxies)}"
    )

    print(
        f"Alive:   {len(alive)}"
    )

    print(
        f"Dead:    {len(dead)}"
    )

    if alive:
        delays = [
            item["delay"]
            for item in alive
        ]

        print()
        print("Latency:")

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

    if dead:
        print("Dead nodes:")

        for name in dead:
            print(
                f"  - {name}"
            )

    print()
    print(
        "NOTE: Diagnostic mode."
    )

    print(
        "config.yaml was NOT modified."
    )


if __name__ == "__main__":
    main()
