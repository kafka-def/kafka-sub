#!/usr/bin/env python3
import base64, concurrent.futures, json, math, os, re, subprocess, sys, tempfile, time, urllib.parse, urllib.request
import yaml

TEST_URLS = [
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/",
    "https://www.google.com/generate_204",
]
MAX_LATENCY_MS = 5000
CHECK_ATTEMPTS = 3
REQUIRED_SUCCESSES_PER_URL = 2
REQUIRED_GOOD_URLS = 2
MAX_WORKERS = 10
REQUEST_TIMEOUT_MS = 5000
STARTUP_TIMEOUT = 20
MIN_ALIVE_PERCENT = 0.05
MIN_ALIVE_ABSOLUTE = 10
MIHOMO_PATH = "./mihomo"
OUTPUT_CONFIG = "config.yaml"
SOURCES_FILE = "sources.txt"
VLESS_RE = re.compile(r"vless://[^\s\"'<>]+")


def log(s): print(s, flush=True)
def bval(v):
    if v is None: return None
    if isinstance(v, bool): return v
    return str(v).lower() in {"1","true","yes","on"} if str(v).lower() in {"1","true","yes","on","0","false","no","off"} else None

def first(q,*keys):
    for k in keys:
        if q.get(k): return q[k][0]
    return None

def dec(v):
    if v is None: return None
    for _ in range(4):
        n=urllib.parse.unquote(str(v))
        if n==v: break
        v=n
    return v

def split(v): return [x.strip() for x in str(v).split(',') if x.strip()]

def json_decode(v):
    if not v: return None
    s=str(v)
    for _ in range(4):
        try: return json.loads(s)
        except Exception:
            n=urllib.parse.unquote(s)
            if n==s: break
            s=n
    return None

def download(url):
    req=urllib.request.Request(url, headers={"User-Agent":"kafka-sub-builder/2.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data=r.read(20*1024*1024+1)
    if len(data)>20*1024*1024: raise ValueError("source too large")
    return data.decode("utf-8", errors="ignore")

def extract(text):
    out=list(VLESS_RE.findall(text))
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            raw=decoder(re.sub(r"\s+","",text)+"="*(-len(re.sub(r"\s+","",text))%4), validate=False)
            s=raw.decode("utf-8",errors="ignore")
            if "vless://" in s: out += VLESS_RE.findall(s)
        except Exception: pass
    for loader in (json.loads, yaml.safe_load):
        try:
            obj=loader(text)
            def walk(x):
                if isinstance(x,str): out.extend(VLESS_RE.findall(x))
                elif isinstance(x,list):
                    for y in x: walk(y)
                elif isinstance(x,dict):
                    # common native Xray VLESS outbound
                    if str(x.get("protocol","")).lower()=="vless":
                        try:
                            v=x["settings"]["vnext"][0]; u=v["users"][0]
                            q={"type":x.get("streamSettings",{}).get("network","tcp"),"security":x.get("streamSettings",{}).get("security","")}
                            ss=x.get("streamSettings",{})
                            for k,a in (("serverName","sni"),("fingerprint","fp"),("publicKey","pbk"),("shortId","sid"),("spiderX","spx")):
                                z=ss.get("tlsSettings",{}).get(k) or ss.get("realitySettings",{}).get(k)
                                if z: q[a]=z
                            ws=ss.get("wsSettings",{}); grpc=ss.get("grpcSettings",{})
                            if ws.get("path"): q["path"]=ws["path"]
                            if ws.get("headers",{}).get("Host"): q["host"]=ws["headers"]["Host"]
                            if grpc.get("serviceName"): q["serviceName"]=grpc["serviceName"]
                            uri="vless://%s@%s:%s?%s#%s"%(urllib.parse.quote(str(u["id"]),safe=''),v["address"],v["port"],urllib.parse.urlencode({k:v for k,v in q.items() if v}),urllib.parse.quote(str(x.get("tag",v["address"])),safe=''))
                            out.append(uri)
                        except Exception: pass
                    for y in x.values(): walk(y)
            walk(obj)
        except Exception: pass
    seen=set(); result=[]
    for x in out:
        x=x.strip().rstrip('.,;)')
        if x not in seen: seen.add(x); result.append(x)
    return result

def parse_vless(uri):
    try:
        p=urllib.parse.urlparse(uri); q=urllib.parse.parse_qs(p.query,keep_blank_values=True)
        if p.scheme.lower()!="vless" or not p.hostname or not p.username or not p.port: return None
        x={"name":dec(p.fragment) or p.hostname,"type":"vless","server":p.hostname,"port":p.port,"uuid":dec(p.username),"udp":True}
        for src,dst in (("encryption","encryption"),("flow","flow"),("packet-encoding","packet-encoding")):
            v=first(q,src)
            if v: x[dst]=dec(v)
        v=first(q,"udp")
        if v is not None:
            bv=bval(v)
            if bv is not None:x["udp"]=bv
        sec=(first(q,"security") or "").lower(); net=(first(q,"type","network") or "tcp").lower()
        if net=="splithttp": net="xhttp"
        if net not in {"tcp","ws","grpc","xhttp","httpupgrade","h2"}: return None
        x["network"]=net
        if sec in {"tls","reality"}: x["tls"]=True
        for src,dst in (("sni","servername"),("servername","servername"),("fp","client-fingerprint"),("fingerprint","client-fingerprint")):
            v=first(q,src)
            if v: x[dst]=dec(v)
        v=first(q,"alpn")
        if v:x["alpn"]=split(dec(v))
        v=first(q,"allowInsecure","skip-cert-verify","skipCertVerify")
        if v is not None:
            bv=bval(v)
            if bv is not None:x["skip-cert-verify"]=bv
        if sec=="reality":
            ro={}
            for src,dst in (("pbk","public-key"),("publicKey","public-key"),("sid","short-id"),("shortId","short-id"),("spx","spider-x"),("spiderX","spider-x")):
                v=first(q,src)
                if v:ro[dst]=dec(v)
            if ro:x["reality-opts"]=ro
        if net=="ws":
            o={}; v=first(q,"path")
            if v:o["path"]=dec(v)
            v=first(q,"host")
            if v:o["headers"]={"Host":dec(v)}
            if o:x["ws-opts"]=o
        elif net=="grpc":
            o={}; v=first(q,"serviceName","service-name")
            if v:o["grpc-service-name"]=dec(v)
            v=first(q,"authority")
            if v:o["grpc-authority"]=dec(v)
            if o:x["grpc-opts"]=o
        elif net=="h2":
            o={}; v=first(q,"path")
            if v:o["path"]=dec(v)
            v=first(q,"host")
            if v:o["host"]=split(dec(v))
            if o:x["h2-opts"]=o
        elif net=="httpupgrade":
            o={}; v=first(q,"path")
            if v:o["path"]=dec(v)
            v=first(q,"host")
            if v:o["headers"]={"Host":dec(v)}
            if o:x["httpupgrade-opts"]=o
        elif net=="xhttp":
            o={}
            for k in ("path","host","mode"):
                v=first(q,k)
                if v:o[k]=dec(v)
            e=json_decode(first(q,"extra"))
            if isinstance(e,dict):
                mp={"noGRPCHeader":"no-grpc-header","xPaddingBytes":"x-padding-bytes","xPaddingObfsMode":"x-padding-obfs-mode","xPaddingKey":"x-padding-key","xPaddingHeader":"x-padding-header","xPaddingPlacement":"x-padding-placement","xPaddingMethod":"x-padding-method","uplinkHttpMethod":"uplink-http-method","sessionPlacement":"session-placement","seqPlacement":"seq-placement","scMaxEachPostBytes":"sc-max-each-post-bytes","scMinPostsIntervalMs":"sc-min-posts-interval-ms","reuseSettings":"reuse-settings","downloadSettings":"download-settings"}
                for a,b in mp.items():
                    if a in e:o[b]=e[a]
                if "headers" in e:o["headers"]=e["headers"]
            if o:x["xhttp-opts"]=o
        return x
    except Exception:return None

def identity(x):
    y=dict(x); y.pop("name",None); y.pop("_delay",None)
    return json.dumps(y,sort_keys=True,ensure_ascii=False,separators=(',',':'))

def name_nodes(nodes):
    used=set(); nodes.sort(key=lambda x:(x["server"],x["port"],x["uuid"]))
    for x in nodes:
        base=re.sub(r"\s+"," ",str(x.get("name") or x["server"]).strip())[:150] or "VLESS"
        n=base; i=2
        while n in used:n=f"{base} {i}";i+=1
        used.add(n);x["name"]=n

def config(nodes,controller=None):
    c={"mixed-port":7890,"mode":"rule","proxies":nodes,"proxy-groups":[{"name":"🚀 Freedom Rudy","type":"select","proxies":[x["name"] for x in nodes]+["DIRECT"]}],"rules":["MATCH,🚀 Freedom Rudy"]}
    if controller:c["external-controller"]=controller
    return c

def validate(path):
    p=subprocess.run([MIHOMO_PATH,"-t","-f",path],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=60)
    print(p.stdout)
    if p.returncode:raise RuntimeError("Mihomo rejected config")

def api(path):
    with urllib.request.urlopen("http://127.0.0.1:9090"+path,timeout=10) as r:return json.loads(r.read())

def probe(name,url):
    q=urllib.parse.urlencode({"url":url,"timeout":REQUEST_TIMEOUT_MS,"expected":"200/204"})
    try:
        d=api("/proxies/"+urllib.parse.quote(name,safe="")+"/delay?"+q);v=int(d.get("delay",0))
        return v if 0<v<=MAX_LATENCY_MS else None
    except Exception:return None

def check(name):
    good=0; delays=[]
    for url in TEST_URLS:
        ok=[]
        for _ in range(CHECK_ATTEMPTS):
            v=probe(name,url)
            if v is not None:ok.append(v)
        if len(ok)>=REQUIRED_SUCCESSES_PER_URL:
            good+=1;delays+=ok
        if good>=REQUIRED_GOOD_URLS:break
    return round(sum(delays)/len(delays)) if good>=REQUIRED_GOOD_URLS and delays else None

def health(nodes):
    fd,path=tempfile.mkstemp(suffix=".yaml");os.close(fd);proc=None
    try:
        with open(path,"w",encoding="utf-8") as f:yaml.safe_dump(config(nodes,"127.0.0.1:9090"),f,allow_unicode=True,sort_keys=False)
        validate(path)
        proc=subprocess.Popen([MIHOMO_PATH,"-f",path],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        end=time.time()+STARTUP_TIMEOUT
        while time.time()<end:
            if proc.poll() is not None:raise RuntimeError("Mihomo stopped during health check")
            try:
                with urllib.request.urlopen("http://127.0.0.1:9090/version",timeout=1):break
            except Exception:time.sleep(.25)
        else:raise TimeoutError("Mihomo controller timeout")
        alive=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fs={ex.submit(check,n["name"]):n for n in nodes}
            for f in concurrent.futures.as_completed(fs):
                n=fs[f];d=f.result()
                if d is not None:n["_delay"]=d;alive.append(n);log(f"[OK] {d}ms {n['name']}")
                else:log(f"[DEAD] {n['name']}")
        return alive
    finally:
        if proc:
            try:proc.terminate();proc.wait(5)
            except Exception:
                try:proc.kill()
                except Exception:pass
        try:os.unlink(path)
        except OSError:pass

def main():
    if not os.path.exists(MIHOMO_PATH):raise FileNotFoundError(MIHOMO_PATH)
    with open(SOURCES_FILE,encoding="utf-8") as f:sources=[x.strip() for x in f if x.strip() and not x.lstrip().startswith('#')]
    uris=[]
    for s in sources:
        try:
            u=extract(download(s));log(f"[SOURCE] {s} -> {len(u)} VLESS");uris+=u
        except Exception as e:log(f"[SOURCE ERROR] {s}: {e}")
    nodes=[];seen=set()
    for u in uris:
        n=parse_vless(u)
        if n and identity(n) not in seen:seen.add(identity(n));nodes.append(n)
    if not nodes:raise RuntimeError("No valid VLESS nodes")
    name_nodes(nodes)
    log(f"[TOTAL] {len(nodes)} unique nodes")
    alive=health(nodes)
    alive.sort(key=lambda x:x.get("_delay",999999))
    minimum=max(MIN_ALIVE_ABSOLUTE,math.ceil(len(nodes)*MIN_ALIVE_PERCENT))
    log(f"[RESULT] {len(alive)}/{len(nodes)} passed strict check; minimum={minimum}")
    if len(alive)<minimum:raise RuntimeError("Suspicious result; config.yaml preserved")
    for n in alive:n.pop("_delay",None)
    fd,tmp=tempfile.mkstemp(suffix=".yaml",dir=".");os.close(fd)
    try:
        with open(tmp,"w",encoding="utf-8") as f:yaml.safe_dump(config(alive),f,allow_unicode=True,sort_keys=False)
        validate(tmp);os.replace(tmp,OUTPUT_CONFIG)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
    log(f"[DONE] published {len(alive)} nodes")

if __name__=="__main__":
    try:main()
    except Exception as e:print(f"[FATAL] {e}",file=sys.stderr);sys.exit(1)
