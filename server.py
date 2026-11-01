#!/usr/bin/env python3
"""
Flappy Birds server  -  player UIDs + gifts.
Pure Python standard library: no pip install needed (works in Termux).

Run:   python server.py
Env:   PORT (default 8080), HOST (default 0.0.0.0), DATA_FILE (default ./data.json)
"""
import hashlib
import hmac
import json
import os
import secrets
import shutil
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))
try:
    import fsstore            # Firestore storage (optional)
except Exception:
    fsstore = None
DATA_FILE = os.environ.get("DATA_FILE", os.path.join(BASE, "data.json"))
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))

# ---- gift rules (change freely) ----
MAX_COINS_PER_GIFT = 100      # one gift
DAILY_COINS = 300             # total coins one player may send per day (UTC)
DAILY_GIFTS = 10              # number of gifts one player may send per day
INBOX_MAX = 50                # unclaimed gifts a player can hold
MAX_MSG = 40                  # optional message length
ITEM_KINDS = ("shield", "magnet", "double")
DUEL_GAMES = ("flappy", "shooter")   # 1v1 modes the player can pick
MAX_PLAYERS = 20000
ADMIN_MAX_COINS = None        # per admin gift; None = no upper limit (the game accepts any amount)
ADMIN_KEY_FILE = os.path.join(BASE, "admin_key.txt")
admin_key = ""
LOG_MAX = 3000                # transfer history kept in data.json (oldest dropped)
# 1 = the admin web page + admin API also work through the public tunnel link (admin key still required)
# 0 = admin only works from the server phone itself (127.0.0.1)
ALLOW_REMOTE_ADMIN = os.environ.get("ALLOW_REMOTE_ADMIN", "1") == "1"
ADMIN_FAILS = 5               # wrong admin keys allowed per IP per 15 min
ADMIN_FAILS_ALL = 20          # wrong admin keys allowed from everyone per 15 min
RESERVED_NAMES = ("admin",)   # players can't use these inside their name
# ---- abuse protection ----
MAX_BODY = 4096
RATE_PER_MIN = 600            # requests / minute / IP (a whole school can share one Wi-Fi IP)
REGISTER_PER_HOUR = 60        # new accounts / hour / IP (many players on one school Wi-Fi)
# ---- 1v1 duels ----
RATE_DUEL_PER_MIN = 1800      # duel polling is fast, so it has its own (bigger) limit per IP
DUEL_COUNTDOWN = 5.0          # seconds from 'match found' to 'GO'
DUEL_GONE = 10.0              # no update for this long = player left
DUEL_QUEUE_STALE = 6.0        # a searching player must ask again within this time
DUEL_PIPE_RATE = 0.85         # max pipes/second a player could possibly pass (anti-cheat cap)
DUEL_LEAD = 1.5               # seconds before the first pipe can be passed
SHOOTER_SCORE_RATE = 8.0      # max points/second a shooter player could possibly earn (anti-cheat cap)
SHOOTER_LEAD = 1.0            # seconds before the first alien can die
SHOOTER_BURST = 6             # extra points allowed (boss kills etc.)
MAX_DUELS = 2000

lock = threading.Lock()
db = {"players": {}, "inbox": {}, "next_id": 1, "log": []}
_rate = {}
_admin_fail = {}
srv = None
_restart = False
_reg = {}
_last_backup = 0.0
_rate_duel = {}
duels = {}          # match id -> match            (memory only, never saved)
duel_of = {}        # uid -> match id
duel_queue = {}     # room key ('*' = quick match, or a 4-digit code) -> waiting player


class ApiError(Exception):
    def __init__(self, code, msg):
        Exception.__init__(self, msg)
        self.code = code
        self.msg = msg


# ---------------------------------------------------------------- storage
def load():
    global db
    if fsstore and fsstore.init():
        try:
            d = fsstore.load_all()
            if not d["players"]:                       # first run: bring old data.json along
                try:
                    with open(DATA_FILE) as f:
                        old = json.load(f)
                    for k, v in (("players", {}), ("inbox", {}), ("next_id", 1), ("log", [])):
                        old.setdefault(k, v)
                    d = old
                    print("Firestore is empty - importing %d players from data.json" % len(d["players"]))
                    global _dirty
                    _dirty = True
                except Exception:
                    pass
            db = d
            print("Firestore: loaded %d players" % len(db["players"]))
            return
        except Exception as e:
            sys.exit("Firestore connected but could not read data (is the database created in the Firebase console?): %s" % e)
    for path in (DATA_FILE, DATA_FILE + ".bak"):
        try:
            with open(path) as f:
                d = json.load(f)
            d.setdefault("players", {})
            d.setdefault("inbox", {})
            d.setdefault("next_id", 1)
            d.setdefault("log", [])
            db = d
            print("loaded %d players from %s" % (len(db["players"]), os.path.basename(path)))
            return
        except FileNotFoundError:
            continue
        except Exception as e:
            print("could not read %s: %s" % (path, e))
    print("starting with an empty database")


_dirty = False
_save_lock = threading.Lock()


def save():
    """Just mark the database as changed. A background thread writes it to disk,
    so replies are no longer held up by slow phone storage."""
    global _dirty
    _dirty = True


def write_db():
    """Write the database to disk now (called by the background saver and on shutdown)."""
    global _last_backup, _dirty
    with _save_lock:
        if fsstore and fsstore.enabled():
            with lock:
                if not _dirty:
                    return
                _dirty = False
                changes = fsstore.diff(db)
            if changes and fsstore.apply(changes):
                _dirty = True
            return
        with lock:
            if not _dirty:
                return
            _dirty = False
            data = json.dumps(db)
        tmp = DATA_FILE + ".tmp"
        try:
            with open(tmp, "w") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            if os.path.exists(DATA_FILE) and time.time() - _last_backup > 60:
                try:
                    shutil.copyfile(DATA_FILE, DATA_FILE + ".bak")
                    _last_backup = time.time()
                except Exception:
                    pass
            os.replace(tmp, DATA_FILE)
        except Exception as e:
            _dirty = True
            print("save failed:", e)


def saver():
    while True:
        time.sleep(3.0 if (fsstore and fsstore.enabled()) else 1.0)
        write_db()


# ---------------------------------------------------------------- helpers
def today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def clean_name(name):
    if not isinstance(name, str):
        return ""
    keep = "".join(c for c in name if c.isalnum() or c in " _-")
    return " ".join(keep.split())[:12]


def name_ok(name):
    low = name.lower()
    return not any(r in low for r in RESERVED_NAMES)


def add_log(by, frm, frm_name, to, to_name, kind, amount, msg, n=1):
    """One line in the transfer history. by = 'player' or 'admin'."""
    db["log"].append(dict(ts=int(time.time()), by=by, frm=frm, frm_name=frm_name, to=to,
                          to_name=to_name, kind=kind, amount=amount, msg=msg, n=n))
    del db["log"][:-LOG_MAX]


def clean_msg(m):
    if not isinstance(m, str):
        return ""
    m = "".join(c for c in m if c.isprintable())
    return " ".join(m.split())[:MAX_MSG]


def load_admin_key():
    """Secret key for admin.py. Created once, kept only on this phone."""
    global admin_key
    admin_key = os.environ.get("ADMIN_KEY", "").strip()      # set ADMIN_KEY on hosts with no permanent disk
    if len(admin_key) >= 16:
        return
    admin_key = ""
    try:
        with open(ADMIN_KEY_FILE) as f:
            admin_key = f.read().strip()
    except FileNotFoundError:
        pass
    if len(admin_key) < 16:
        admin_key = secrets.token_hex(24)
        with open(ADMIN_KEY_FILE, "w") as f:
            f.write(admin_key + "\n")
        print("created admin key file: admin_key.txt")


def _fail_window(k, now):
    w = _admin_fail.get(k)
    if not w or now - w[0] > 900:
        w = _admin_fail[k] = [now, 0]
    return w


def check_admin(b, ip="local"):
    now = time.time()
    mine, everyone = _fail_window(ip, now), _fail_window("*", now)
    local = ip in ("127.0.0.1", "::1", "local")      # admin.py on the phone is never locked out by strangers
    if mine[1] >= ADMIN_FAILS or (everyone[1] >= ADMIN_FAILS_ALL and not local):
        raise ApiError(429, "Too many wrong keys - try again in 15 minutes")
    if not admin_key or not hmac.compare_digest(str(b.get("key", "")), admin_key):
        mine[1] += 1
        everyone[1] += 1
        print("WRONG admin key from %s" % ip)
        raise ApiError(401, "Bad admin key")
    mine[1] = 0


def new_uid():
    while True:
        uid = str(secrets.randbelow(90000000) + 10000000)   # 8 digits
        if uid not in db["players"]:
            return uid


def auth(b):
    uid = str(b.get("uid", ""))
    tok = str(b.get("token", ""))
    p = db["players"].get(uid)
    if not p or not hmac.compare_digest(p["th"], sha(tok)):
        raise ApiError(401, "Login failed")
    p["seen"] = int(time.time())
    return uid, p


def rate_ok(ip, path=""):
    now = time.time()
    store, limit = (_rate_duel, RATE_DUEL_PER_MIN) if path.startswith("/duel/") else (_rate, RATE_PER_MIN)
    w = store.get(ip)
    if not w or now - w[0] > 60:
        store[ip] = [now, 1]
        if len(store) > 5000:
            for k in [k for k, v in store.items() if now - v[0] > 60]:
                store.pop(k, None)
        return True
    w[1] += 1
    return w[1] <= limit


def reg_ok(ip):
    now = time.time()
    w = _reg.get(ip)
    if not w or now - w[0] > 3600:
        _reg[ip] = [now, 1]
        return True
    w[1] += 1
    return w[1] <= REGISTER_PER_HOUR


# ---------------------------------------------------------------- routes
def r_register(b, ip):
    if len(db["players"]) >= MAX_PLAYERS:
        raise ApiError(503, "Server is full")
    if not reg_ok(ip):
        raise ApiError(429, "Too many new accounts from this network, try later")
    uid = new_uid()
    token = secrets.token_hex(16)
    name = clean_name(b.get("name"))
    if not name or not name_ok(name):
        name = "Player" + uid[-4:]
    now = int(time.time())
    db["players"][uid] = dict(name=name, th=sha(token), created=now, seen=now,
                              gday="", gcoins=0, gcount=0)
    save()
    print("new player %s (%s)" % (uid, name))
    return {"uid": uid, "token": token, "name": name}


def r_lookup(b, ip):
    auth(b)
    to = str(b.get("to", ""))
    t = db["players"].get(to)
    if not t:
        raise ApiError(404, "No player with that UID")
    return {"name": t["name"]}


def r_name(b, ip):
    uid, p = auth(b)
    name = clean_name(b.get("name"))
    if len(name) < 2:
        raise ApiError(400, "Name is too short")
    if not name_ok(name):
        raise ApiError(400, "That name is not allowed")
    p["name"] = name
    save()
    return {"name": name}


def r_gift(b, ip):
    uid, p = auth(b)
    to = str(b.get("to", ""))
    kind = str(b.get("kind", ""))
    try:
        amount = int(b.get("amount", 0))
    except (TypeError, ValueError):
        raise ApiError(400, "Bad amount")
    if to == uid:
        raise ApiError(400, "You can't gift yourself")
    target = db["players"].get(to)
    if not target:
        raise ApiError(404, "No player with that UID")
    if kind == "coins":
        if not 1 <= amount <= MAX_COINS_PER_GIFT:
            raise ApiError(400, "Max %d coins per gift" % MAX_COINS_PER_GIFT)
    elif kind in ITEM_KINDS:
        amount = 1
    else:
        raise ApiError(400, "Unknown gift")
    if p.get("gday") != today():
        p["gday"], p["gcoins"], p["gcount"] = today(), 0, 0
    if p["gcount"] >= DAILY_GIFTS:
        raise ApiError(429, "Daily gift limit reached (%d)" % DAILY_GIFTS)
    if kind == "coins" and p["gcoins"] + amount > DAILY_COINS:
        raise ApiError(429, "Only %d coins left to send today" % (DAILY_COINS - p["gcoins"]))
    msg = clean_msg(b.get("msg"))
    box = db["inbox"].setdefault(to, [])
    if len(box) >= INBOX_MAX:
        raise ApiError(409, "Their inbox is full")
    box.append(dict(id=db["next_id"], frm=uid, name=p["name"], kind=kind,
                    amount=amount, msg=msg, ts=int(time.time())))
    db["next_id"] += 1
    p["gcount"] += 1
    if kind == "coins":
        p["gcoins"] += amount
    add_log("player", uid, p["name"], to, target["name"], kind, amount, msg)
    save()
    print("gift %s -> %s : %d %s" % (uid, to, amount, kind))
    return {"ok": True, "to_name": target["name"], "coins_left_today": DAILY_COINS - p["gcoins"]}


def r_inbox(b, ip):
    uid, p = auth(b)
    return {"gifts": db["inbox"].get(uid, [])}


def r_ack(b, ip):
    uid, p = auth(b)
    ids = b.get("ids", [])
    if not isinstance(ids, list):
        raise ApiError(400, "Bad ids")
    ids = set(i for i in ids if isinstance(i, int))
    box = db["inbox"].get(uid, [])
    left = [g for g in box if g["id"] not in ids]
    if len(left) != len(box):
        db["inbox"][uid] = left
        save()
    return {"ok": True}


def r_admin_players(b, ip):
    check_admin(b, ip)
    rows = [dict(uid=u, name=p["name"], seen=p.get("seen", 0), created=p.get("created", 0),
                 waiting=len(db["inbox"].get(u, [])))
            for u, p in db["players"].items()]
    rows.sort(key=lambda r: -r["seen"])
    return {"players": rows}


def r_admin_gift(b, ip):
    check_admin(b, ip)
    to = str(b.get("to", ""))
    kind = str(b.get("kind", "coins"))
    try:
        amount = int(b.get("amount", 1))
    except (TypeError, ValueError):
        raise ApiError(400, "Bad amount")
    if kind == "coins":
        if amount < 1 or (ADMIN_MAX_COINS is not None and amount > ADMIN_MAX_COINS):
            raise ApiError(400, "Admin gift must be at least 1 coin")
    elif kind in ITEM_KINDS:
        amount = 1
    elif kind == "msg":                                  # message only, nothing to claim (shown as NEW MESSAGE)
        amount = 0
        if not clean_msg(b.get("msg")):
            raise ApiError(400, "A message gift needs a message")
    else:
        raise ApiError(400, "Unknown gift")
    if to == "all":
        targets = list(db["players"])
    elif to in db["players"]:
        targets = [to]
    else:
        raise ApiError(404, "No player with that UID")
    msg = clean_msg(b.get("msg"))
    sent = full = 0
    for uid in targets:
        box = db["inbox"].setdefault(uid, [])
        if len(box) >= INBOX_MAX:
            full += 1
            continue
        box.append(dict(id=db["next_id"], frm="admin", name="Admin", kind=kind,
                        amount=amount, msg=msg, ts=int(time.time())))
        db["next_id"] += 1
        sent += 1
    if sent:
        to_name = "Everyone" if to == "all" else db["players"][to]["name"]
        add_log("admin", "admin", "Admin", to, to_name, kind, amount, msg, n=sent)
    save()
    print("ADMIN gift -> %s : %d %s (sent %d, full %d)" % (to, amount, kind, sent, full))
    return {"ok": True, "sent": sent, "inbox_full": full}


def r_admin_log(b, ip):
    check_admin(b, ip)
    uid = str(b.get("uid", "")).strip()
    try:
        limit = max(1, min(500, int(b.get("limit", 100))))
    except (TypeError, ValueError):
        limit = 100
    rows = [e for e in db["log"] if not uid or uid in (e["frm"], e["to"])]
    return {"log": rows[-limit:][::-1], "total": len(db["log"])}


def schedule_exit(restart):
    """Answer the request first, then stop the server (and start it again if restart)."""
    global _restart
    _restart = restart

    def run():
        time.sleep(0.7)
        srv.shutdown()
    threading.Thread(target=run, daemon=True).start()


def r_admin_restart(b, ip):
    check_admin(b, ip)
    print("ADMIN: restart requested")
    schedule_exit(True)
    return {"ok": True, "msg": "Restarting - back in a few seconds"}


def r_admin_stop(b, ip):
    check_admin(b, ip)
    print("ADMIN: stop requested")
    schedule_exit(False)
    return {"ok": True, "msg": "Server stopped. Start it again from Termux: python server.py"}


# ---------------------------------------------------------------- 1v1 duels
# Both players fly the SAME pipes (same seed) at the same moment.
# The server only relays scores and decides the result:
#   - both crashed            -> higher score wins (same score = draw)
#   - one crashed, other alive-> the survivor wins as soon as their score passes the crashed one
#   - a player disappears     -> the other player wins
def duel_clean(now):
    for k in [k for k, q in duel_queue.items() if now - q["ts"] > DUEL_QUEUE_STALE]:
        duel_queue.pop(k, None)
    old = [i for i, m in duels.items()
           if (m["done"] and now - m["ended"] > 120) or now - m["created"] > 1800]
    for i in old:
        m = duels.pop(i)
        for u in m["players"]:
            if duel_of.get(u) == i:
                duel_of.pop(u, None)


def duel_game(v):
    v = str(v or "flappy").lower()
    return v if v in DUEL_GAMES else "flappy"


def duel_bird(v):
    return "".join(c for c in str(v or "") if c.isalnum())[:12] or "classic"


def duel_end(m, result, reason, now):
    m["done"], m["result"], m["reason"], m["ended"] = True, result, reason, now
    print("duel %s over: %s (%s)" % (m["id"], result, reason))


def duel_judge(m, now):
    if m["done"]:
        return
    a, b = m["players"]
    sa, sb = m["st"][a], m["st"][b]
    if now < m["start"]:                                  # still counting down
        if now - sa["seen"] > DUEL_GONE or now - sb["seen"] > DUEL_GONE:
            duel_end(m, "void", "left", now)
        return
    gone_a = sa["alive"] and now - sa["seen"] > DUEL_GONE
    gone_b = sb["alive"] and now - sb["seen"] > DUEL_GONE
    if gone_a and gone_b:
        duel_end(m, "void", "left", now)
    elif gone_a:
        duel_end(m, b, "left", now)
    elif gone_b:
        duel_end(m, a, "left", now)
    elif not sa["alive"] and not sb["alive"]:
        duel_end(m, a if sa["score"] > sb["score"] else b if sb["score"] > sa["score"] else "draw", "score", now)
    elif not sa["alive"] and sb["score"] > sa["score"]:
        duel_end(m, b, "score", now)
    elif not sb["alive"] and sa["score"] > sb["score"]:
        duel_end(m, a, "score", now)


def duel_matched(m, uid, now):
    a, b = m["players"]
    opp = b if uid == a else a
    return {"status": "matched", "mid": m["id"], "seed": m["seed"],
            "start_in": max(0.0, round(m["start"] - now, 2)),
            "opp_name": m["names"][opp], "opp_bird": m["birds"][opp], "game": m["game"]}


def duel_view(m, uid, now):
    a, b = m["players"]
    so = m["st"][b if uid == a else a]
    res = None
    if m["done"]:
        r = m["result"]
        res = r if r in ("void", "draw") else ("win" if r == uid else "lose")
    return {"start_in": max(0.0, round(m["start"] - now, 2)), "result": res, "reason": m["reason"],
            "opp": {"score": so["score"], "alive": so["alive"], "y": so["y"]}}


def r_duel_find(b, ip):
    """Quick match (no code) or private room (4-digit code). The client calls this again
    every second or so until it gets status 'matched'."""
    uid, p = auth(b)
    code = str(b.get("code", "")).strip()
    if code and not (code.isdigit() and len(code) == 4):
        raise ApiError(400, "Room code must be 4 digits")
    now = time.time()
    duel_clean(now)
    bird = duel_bird(b.get("bird"))
    game = duel_game(b.get("game"))
    mid = duel_of.get(uid)
    m = duels.get(mid) if mid else None
    if m and not m["done"]:
        return duel_matched(m, uid, now)                  # somebody already joined me
    duel_of.pop(uid, None)
    key = "%s:%s" % (game, code or "*")          # each mode has its own queue / room namespace
    for k in [k for k, q in duel_queue.items() if q["uid"] == uid and k != key]:
        duel_queue.pop(k, None)
    q = duel_queue.get(key)
    if q and q["uid"] != uid and now - q["ts"] <= DUEL_QUEUE_STALE:
        duel_queue.pop(key, None)
        if len(duels) >= MAX_DUELS:
            raise ApiError(503, "Server is busy, try again soon")
        first = q["uid"]
        mid = secrets.token_hex(6)
        names = {u: (db["players"].get(u) or {}).get("name", "Player") for u in (first, uid)}
        m = dict(id=mid, game=game, seed=secrets.randbelow(2 ** 31), players=[first, uid], created=now,
                 start=now + DUEL_COUNTDOWN, done=False, result=None, reason="", ended=0.0,
                 names=names, birds={first: q["bird"], uid: bird},
                 st={u: dict(score=0, alive=True, y=400.0, seen=now) for u in (first, uid)})
        duels[mid] = m
        duel_of[first] = duel_of[uid] = mid
        print("duel %s [%s]: %s vs %s" % (mid, game, first, uid))
        return duel_matched(m, uid, now)
    duel_queue[key] = dict(uid=uid, ts=now, bird=bird)
    return {"status": "waiting"}


def r_duel_state(b, ip):
    """Called ~5-8 times a second during a match: send my score, get the opponent's."""
    uid, p = auth(b)
    m = duels.get(str(b.get("mid", "")))
    if not m or uid not in m["players"]:
        raise ApiError(404, "Match not found")
    now = time.time()
    me = m["st"][uid]
    me["seen"] = now
    if not m["done"] and now >= m["start"] and me["alive"]:
        try:
            score = max(0, int(b.get("score", 0)))
        except (TypeError, ValueError):
            score = 0
        if m["game"] == "shooter":
            cap = int(max(0.0, now - m["start"] - SHOOTER_LEAD) * SHOOTER_SCORE_RATE) + SHOOTER_BURST
        else:
            cap = int(max(0.0, now - m["start"] - DUEL_LEAD) * DUEL_PIPE_RATE) + 1
        me["score"] = max(me["score"], min(score, cap))
        try:
            me["y"] = max(0.0, min(800.0, float(b.get("y", me["y"]))))
        except (TypeError, ValueError):
            pass
        if not b.get("alive", True):
            me["alive"] = False
    duel_judge(m, now)
    return duel_view(m, uid, now)


def r_duel_leave(b, ip):
    """Cancel a search, or quit a match (counts as crashing with the current score)."""
    uid, p = auth(b)
    now = time.time()
    for k in [k for k, q in duel_queue.items() if q["uid"] == uid]:
        duel_queue.pop(k, None)
    m = duels.get(duel_of.pop(uid, None) or str(b.get("mid", "")))
    if m and uid in m["players"] and not m["done"]:
        if now < m["start"]:
            duel_end(m, "void", "left", now)
        else:
            m["st"][uid]["alive"] = False
            m["st"][uid]["seen"] = now
            duel_judge(m, now)
    return {"ok": True}


def r_admin_duels(b, ip):
    check_admin(b, ip)
    now = time.time()
    rows = []
    for m in duels.values():
        pl = [dict(uid=u, name=m["names"][u], score=m["st"][u]["score"], alive=m["st"][u]["alive"])
              for u in m["players"]]
        rows.append(dict(id=m["id"], game=m["game"], age=int(now - m["created"]), done=m["done"], players=pl))
    return {"duels": rows, "waiting": len(duel_queue),
            "waiting_by_game": {g: sum(1 for k in duel_queue if k.startswith(g + ":")) for g in DUEL_GAMES}}


ROUTES = {"/duel/find": r_duel_find, "/duel/state": r_duel_state,
          "/duel/leave": r_duel_leave, "/admin/duels": r_admin_duels, "/register": r_register, "/lookup": r_lookup, "/name": r_name,
          "/gift": r_gift, "/inbox": r_inbox, "/ack": r_ack,
          "/admin/players": r_admin_players, "/admin/gift": r_admin_gift,
          "/admin/log": r_admin_log, "/admin/restart": r_admin_restart, "/admin/stop": r_admin_stop}


ADMIN_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flappy Admin</title>
<style>
*{box-sizing:border-box}
body{margin:0;font:15px system-ui,sans-serif;background:#10131c;color:#e8ecf5;padding:14px;max-width:760px;margin:auto}
h1{font-size:20px;margin:6px 0 12px}h2{font-size:15px;margin:0 0 8px;color:#ffd75e}
.card{background:#1b2030;border-radius:12px;padding:12px;margin-bottom:12px}
input,select,button{font:inherit;padding:9px 10px;border-radius:8px;border:1px solid #39425c;background:#0f1320;color:#fff}
input,select{width:100%;margin-bottom:8px}
button{background:#2f7df6;border:0;cursor:pointer;font-weight:600}
button.red{background:#d9434f}button.gray{background:#39425c}
.row{display:flex;gap:8px}.row>*{flex:1}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:5px 4px;border-bottom:1px solid #2a3148;text-align:left;vertical-align:top}
tr.pl{cursor:pointer}tr.pl:hover{background:#252c44}
.adm{color:#ffd75e;font-weight:700}.ok{color:#6be585}.bad{color:#ff7b7b}
.scroll{max-height:300px;overflow:auto}
#app{display:none}
</style></head><body>
<h1>Flappy Admin</h1>

<div class="card" id="login">
  <h2>Admin key</h2>
  <input id="key" type="password" placeholder="paste the key from admin_key.txt" autocomplete="off">
  <button onclick="login()">Log in</button>
  <div id="loginMsg" class="bad"></div>
</div>

<div id="app">
  <div class="card">
    <h2>Send a gift as Admin</h2>
    <div class="row"><input id="to" placeholder="Player UID (or all)"><select id="kind">
      <option value="coins">Coins</option><option value="shield">Shield</option>
      <option value="magnet">Magnet</option><option value="double">Double</option>
      <option value="msg">Message only</option></select></div>
    <div class="row"><input id="amount" type="number" value="100" min="1" max="1000" placeholder="Coins">
    <input id="msg" maxlength="40" placeholder="Message (optional)"></div>
    <button onclick="sendGift()">Send</button>
    <div id="giftMsg"></div>
  </div>

  <div class="card">
    <h2>Transfer log <span style="font-weight:400;color:#8b94ad">(who sent what to whom)</span></h2>
    <div class="row"><input id="logUid" placeholder="Filter by UID (empty = all)">
    <button class="gray" onclick="loadLog()">Refresh</button></div>
    <div class="scroll"><table id="logTbl"></table></div>
  </div>

  <div class="card">
    <h2>Players <span id="pcount" style="font-weight:400;color:#8b94ad"></span></h2>
    <div class="scroll"><table id="pTbl"></table></div>
  </div>

  <div class="card">
    <h2>Server</h2>
    <div class="row"><button class="gray" onclick="srv('restart')">Restart server</button>
    <button class="red" onclick="srv('stop')">Stop server</button></div>
    <div style="color:#8b94ad;margin-top:8px;font-size:13px">Stop cannot be undone from here - you must start it again on the phone.</div>
    <div id="srvMsg"></div>
  </div>
</div>

<script>
var KEY = "";
function esc(s){return String(s).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]})}
function $(id){return document.getElementById(id)}
function api(path, body){
  body = body || {}; body.key = KEY;
  return fetch(path, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)})
    .then(function(r){return r.json().then(function(j){ if(!r.ok) throw new Error(j.error||"Error"); return j; });});
}
function ago(ts){var s=Math.max(0,Math.floor(Date.now()/1000-ts));
  if(s<90)return s+"s";if(s<5400)return Math.floor(s/60)+"m";if(s<172800)return Math.floor(s/3600)+"h";return Math.floor(s/86400)+"d";}
function login(){
  KEY = $("key").value.trim();
  api("/admin/players").then(function(){
    $("login").style.display="none"; $("app").style.display="block"; refresh();
  }).catch(function(e){ $("loginMsg").textContent = e.message; KEY=""; });
}
$("key").addEventListener("keydown", function(e){ if(e.key==="Enter") login(); });
function refresh(){ loadPlayers(); loadLog(); }
function loadPlayers(){
  api("/admin/players").then(function(r){
    $("pcount").textContent = "(" + r.players.length + ")";
    var h = "<tr><th>UID</th><th>Name</th><th>Seen</th><th>Waiting</th></tr>";
    r.players.forEach(function(p){
      h += "<tr class='pl' onclick=\"$('to').value='"+esc(p.uid)+"'\"><td>"+esc(p.uid)+"</td><td>"+esc(p.name)+"</td><td>"+ago(p.seen)+"</td><td>"+p.waiting+"</td></tr>";
    });
    $("pTbl").innerHTML = h;
  }).catch(function(e){ $("srvMsg").innerHTML = "<span class='bad'>"+esc(e.message)+"</span>"; });
}
function loadLog(){
  api("/admin/log", {uid:$("logUid").value.trim(), limit:150}).then(function(r){
    var h = "<tr><th>When</th><th>From</th><th></th><th>To</th><th>Gift</th></tr>";
    r.log.forEach(function(e){
      var who = e.by==="admin" ? "<span class='adm'>ADMIN</span>" : esc(e.frm_name)+"<br><small>"+esc(e.frm)+"</small>";
      var to = e.to==="all" ? "Everyone ("+e.n+")" : esc(e.to_name)+"<br><small>"+esc(e.to)+"</small>";
      var what = e.kind==="coins" ? e.amount+" coins" : e.kind==="msg" ? "message" : esc(e.kind);
      h += "<tr><td>"+ago(e.ts)+" ago</td><td>"+who+"</td><td>&rarr;</td><td>"+to+"</td><td>"+what+(e.msg?"<br><small>"+esc(e.msg)+"</small>":"")+"</td></tr>";
    });
    if(!r.log.length) h += "<tr><td colspan='5'>No transfers yet</td></tr>";
    $("logTbl").innerHTML = h;
  }).catch(function(e){ $("logTbl").innerHTML = "<tr><td class='bad'>"+esc(e.message)+"</td></tr>"; });
}
function sendGift(){
  var to = $("to").value.trim(), kind = $("kind").value;
  if(!to){ $("giftMsg").innerHTML="<span class='bad'>Enter a UID or all</span>"; return; }
  var label = kind==="coins" ? $("amount").value+" coins" : kind;
  if(!confirm("Send "+label+" to "+to+" as Admin?")) return;
  api("/admin/gift", {to:to, kind:kind, amount:parseInt($("amount").value||"1",10), msg:$("msg").value}).then(function(r){
    $("giftMsg").innerHTML = "<span class='ok'>Sent to "+r.sent+" player(s)"+(r.inbox_full?", "+r.inbox_full+" inbox full":"")+"</span>";
    refresh();
  }).catch(function(e){ $("giftMsg").innerHTML = "<span class='bad'>"+esc(e.message)+"</span>"; });
}
function srv(what){
  if(!confirm((what==="stop"?"STOP":"RESTART")+" the server?")) return;
  api("/admin/"+what).then(function(r){ $("srvMsg").innerHTML = "<span class='ok'>"+esc(r.msg)+"</span>";
    if(what==="restart") setTimeout(refresh, 4000); })
  .catch(function(e){ $("srvMsg").innerHTML = "<span class='bad'>"+esc(e.message)+"</span>"; });
}
</script></body></html>
"""


# ---------------------------------------------------------------- http
class Handler(BaseHTTPRequestHandler):
    server_version = "FlappyServer/1"
    protocol_version = "HTTP/1.1"
    wbufsize = 65536                  # send headers + body in ONE packet (much faster through a tunnel)
    disable_nagle_algorithm = True    # no 40 ms delays
    timeout = 60                      # drop idle connections

    def log_message(self, fmt, *args):
        pass

    def reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def client_ip(self):
        return (self.headers.get("CF-Connecting-IP")
                or (self.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
                or self.client_address[0])

    def do_HEAD(self):                 # uptime monitors often use HEAD
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path in ("/", "/health"):
            with lock:
                n = len(db["players"])
            self.reply(200, {"ok": True, "players": n, "time": int(time.time())})
        elif self.path == "/admin" and ALLOW_REMOTE_ADMIN:
            body = ADMIN_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.reply(404, {"error": "Not found"})

    def do_POST(self):
        try:
            ip = self.client_ip()
            length = int(self.headers.get("Content-Length") or 0)
            if length < 0 or length > MAX_BODY:
                return self.reply(413, {"error": "Too big"})
            raw = self.rfile.read(length) if length else b"{}"
            with lock:
                if not rate_ok(ip, self.path):
                    return self.reply(429, {"error": "Slow down"})
                route = ROUTES.get(self.path)
                if not route:
                    return self.reply(404, {"error": "Not found"})
                if self.path.startswith("/admin/"):
                    via_tunnel = (self.headers.get("CF-Connecting-IP")
                                  or self.headers.get("X-Forwarded-For"))
                    if not ALLOW_REMOTE_ADMIN and (via_tunnel or self.client_address[0] not in ("127.0.0.1", "::1")):
                        return self.reply(403, {"error": "Admin works only on the server phone"})
                try:
                    body = json.loads(raw.decode("utf-8"))
                    if not isinstance(body, dict):
                        raise ValueError
                except Exception:
                    return self.reply(400, {"error": "Bad request"})
                try:
                    result = route(body, ip)
                except ApiError as e:
                    return self.reply(e.code, {"error": e.msg})
            self.reply(200, result)
        except Exception as e:
            print("error:", repr(e))
            try:
                self.reply(500, {"error": "Server error"})
            except Exception:
                pass


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128


def main():
    global srv
    load()
    load_admin_key()
    threading.Thread(target=saver, daemon=True).start()
    srv = Server((HOST, PORT), Handler)
    try:                               # hosts stop servers with SIGTERM: save first
        signal.signal(signal.SIGTERM, lambda *a: threading.Thread(target=srv.shutdown, daemon=True).start())
    except Exception:
        pass
    print("Flappy server listening on %s:%d  (storage: %s)" % (
        HOST, PORT, "Firestore" if (fsstore and fsstore.enabled()) else "data.json"))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        with lock:
            save()
        write_db()
        srv.server_close()
    if _restart:
        print("restarting...")
        os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == "__main__":
    main()
