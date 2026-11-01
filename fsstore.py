"""Firestore storage for server.py (optional).

If a Firebase service-account key is found, players / gifts / mail are saved in
Firestore instead of data.json, so data survives restarts and free hosts that
wipe their disk. Without a key, server.py keeps using data.json (Termux mode).

Key sources (first found wins):
  env FIREBASE_KEY_JSON  = the whole contents of the key file
  env FIREBASE_KEY_FILE  = path to the key file
  ./serviceAccount.json
"""
import json
import os

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:           # not installed -> plain data.json mode
    firebase_admin = None

BASE = os.path.dirname(os.path.abspath(__file__))
PFX = os.environ.get("FS_PREFIX", "flappy_")
SEEN_STEP = 3600            # a player's 'last seen' is saved at most once an hour (saves free write quota)
LOG_DOC_MAX = 900000        # Firestore documents must stay under 1 MB

_client = None
_snap = {}                  # (collection, doc id) -> what Firestore already has (as text)


def enabled():
    return _client is not None


def init():
    """Connect to Firestore. Returns True if a key was found and it worked."""
    global _client
    if firebase_admin is None:
        return False
    raw = os.environ.get("FIREBASE_KEY_JSON", "").strip()
    path = os.environ.get("FIREBASE_KEY_FILE") or os.path.join(BASE, "serviceAccount.json")
    try:
        if raw:
            cred = credentials.Certificate(json.loads(raw))
        elif os.path.exists(path):
            cred = credentials.Certificate(path)
        else:
            return False
        firebase_admin.initialize_app(cred)
        _client = firestore.client()
        return True
    except Exception as e:
        print("Firebase key problem:", e)
        _client = None
        return False


def _col(name):
    return _client.collection(PFX + name)


def _pkey(p):
    q = dict(p)
    q["seen"] = int(p.get("seen", 0)) // SEEN_STEP
    return json.dumps(q, sort_keys=True)


def load_all():
    d = {"players": {}, "inbox": {}, "next_id": 1, "log": []}
    _snap.clear()
    for doc in _col("players").stream():
        p = doc.to_dict() or {}
        d["players"][doc.id] = p
        _snap[("players", doc.id)] = _pkey(p)
    for doc in _col("inbox").stream():
        items = (doc.to_dict() or {}).get("items", [])
        d["inbox"][doc.id] = items
        _snap[("inbox", doc.id)] = json.dumps(items)
    m = _col("meta").document("main").get()
    if m.exists:
        d["next_id"] = int((m.to_dict() or {}).get("next_id", 1))
        _snap[("meta", "main")] = json.dumps({"next_id": d["next_id"]})
    lg = _col("meta").document("log").get()
    if lg.exists:
        d["log"] = (lg.to_dict() or {}).get("items", [])
        _snap[("meta", "log")] = json.dumps(d["log"])
    return d


def diff(db):
    """Compare memory with what Firestore has. Call while holding server.lock.
    Returns a list of (op, collection, doc id, data)."""
    ch = []
    live = set()
    for uid, p in db["players"].items():
        k = ("players", uid)
        live.add(k)
        s = _pkey(p)
        if _snap.get(k) != s:
            _snap[k] = s
            ch.append(("set", "players", uid, dict(p)))
    for uid, items in db["inbox"].items():
        k = ("inbox", uid)
        if items:
            live.add(k)
            s = json.dumps(items)
            if _snap.get(k) != s:
                _snap[k] = s
                ch.append(("set", "inbox", uid, {"items": json.loads(s)}))
    for k in [k for k in _snap if k[0] in ("players", "inbox") and k not in live]:
        _snap.pop(k, None)
        ch.append(("delete", k[0], k[1], None))
    s = json.dumps({"next_id": db["next_id"]})
    if _snap.get(("meta", "main")) != s:
        _snap[("meta", "main")] = s
        ch.append(("set", "meta", "main", {"next_id": db["next_id"]}))
    s = json.dumps(db["log"])
    while len(s) > LOG_DOC_MAX and db["log"]:
        del db["log"][:max(1, len(db["log"]) // 5)]
        s = json.dumps(db["log"])
    if _snap.get(("meta", "log")) != s:
        _snap[("meta", "log")] = s
        ch.append(("set", "meta", "log", {"items": json.loads(s)}))
    return ch


def apply(changes):
    """Write the changes. Returns True if anything failed (they are retried later)."""
    failed = False
    for i in range(0, len(changes), 400):
        part = changes[i:i + 400]
        try:
            b = _client.batch()
            for op, col, doc, data in part:
                ref = _col(col).document(doc)
                if op == "set":
                    b.set(ref, data)
                else:
                    b.delete(ref)
            b.commit()
        except Exception as e:
            print("Firestore write failed:", e)
            failed = True
            for _op, col, doc, _data in part:
                _snap[(col, doc)] = "?"      # forces a retry on the next save
    return failed
