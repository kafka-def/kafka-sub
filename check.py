import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import yaml


CONFIG_FILE = "config.yaml"

MIHOMO_BINARY = "./mihomo"

API_HOST = "127.0.0.1"
API_PORT = 9090

TEST_URL = "https://www.gstatic.com/generate_204"
TIMEOUT_MS = 5000

STARTUP_TIMEOUT = 15
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


def start_mihomo():
    print("[+] Starting Mihomo...")

    process = subprocess.Popen(
        [
            MIHOMO_BINARY,
            "-d",
            ".",
            "-f",
            CONFIG_FILE,
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
            print("[!] Mihomo exited during startup:")
            print(output)

            sys.exit(1)

        try:
            api_get("/proxies")
            print("[+] Mihomo API is ready.")
            return process

        except Exception:
            time.sleep(0.5)

    process.kill()

    print("[!] Mihomo API did not start.")
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
        print(f"[!] Failed to load config.yaml: {e}")
        sys.exit(1)

    proxies = config.get("proxies", [])

    if not proxies:
        print("[!] No proxies found.")
        sys.exit(1)

    print(f"[+] Proxies to check: {len(proxies)}")
    print(f"[+] Test URL: {TEST_URL}")
    print(f"[+] Timeout: {TIMEOUT_MS} ms")
    print()

    mihomo = start_mihomo()

    alive = []
    dead = []

    try:
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
                    f"[{index}/{len(proxies)}] "
                    f"OK   {name} "
                    f"{delay} ms"
                )

            else:
                dead.append(name)

                print(
                    f"[{index}/{len(proxies)}] "
                    f"FAIL {name}"
                )

    finally:
        print()
        print("[+] Stopping Mihomo...")

        mihomo.terminate()

        try:
            mihomo.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mihomo.kill()

    print()
    print("=== Check complete ===")
    print()
    print(f"Checked: {len(proxies)}")
    print(f"Alive:   {len(alive)}")
    print(f"Dead:    {len(dead)}")

    if alive:
        delays = [
            item["delay"]
            for item in alive
        ]

        print()
        print("Latency:")

        print(
            f"  Minimum: {min(delays)} ms"
        )

        print(
            f"  Maximum: {max(delays)} ms"
        )

        print(
            f"  Average: "
            f"{sum(delays) // len(delays)} ms"
        )

    print()

    if dead:
        print("Dead nodes:")

        for name in dead:
            print(f"  - {name}")

    print()
    print("NOTE:")
    print(
        "This is diagnostic mode. "
        "config.yaml was NOT modified."
    )


if __name__ == "__main__":
    main()
