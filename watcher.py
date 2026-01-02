import os
import time
import json
from datetime import datetime
import platform
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import re

def _select_config_file():
    env_file = os.getenv("CONFIG_FILE")
    if env_file and os.path.isfile(env_file):
        return env_file
    sysname = platform.system().lower()
    if sysname == "windows":
        for n in ("config.windows.json", "config.json"):
            if os.path.isfile(n):
                return n
    elif sysname == "darwin":
        for n in ("config.macos.json", "config.json"):
            if os.path.isfile(n):
                return n
    else:
        for n in ("config.json",):
            if os.path.isfile(n):
                return n
    return "config.json"

_cfg_file = _select_config_file()
with open(_cfg_file) as f:
    config = json.load(f)

WATCH_PATH = os.getenv("WATCH_PATH", config["watch_path"]) if "watch_path" in config else None
MONGO_URI = os.getenv("MONGO_URI", config["mongo_uri"])
DB_NAME = os.getenv("DB_NAME", config["database"])
COLLECTION_NAME = os.getenv("COLLECTION", config.get("collection", "")) if "collection" in config else ""
RECURSIVE = os.getenv("RECURSIVE", "true").lower() == "true"
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "30"))
ENV_KEY_NAME = os.getenv("ENV_KEY")
REFRESH_TEST_RUNS = os.getenv("REFRESH_TEST_RUNS", "false").lower() == "true"
EXIT_AFTER_REFRESH = os.getenv("EXIT_AFTER_REFRESH", "false").lower() == "true"

# MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def log_watcher(level, message):
    print(f"[{level}] {message}")
    try:
        db["log-watcher"].insert_one({
            "level": level,
            "message": message,
            "timestamp": datetime.now()
        })
    except Exception as e:
        print(f"[ERROR] Log to DB failed: {e}")

log_watcher("CONFIG", f"Using file: {_cfg_file}")

def _utc_now():
    return datetime.utcnow()

def _read_json(fp):
    try:
        with open(fp) as fh:
            return json.load(fh)
    except Exception:
        return None

def _read_properties(fp):
    result = {}
    try:
        with open(fp) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    result[k.strip()] = v.strip()
    except Exception:
        pass
    return result

def _folder_start_time(folder_path):
    try:
        st = os.stat(folder_path)
        ts = None
        if hasattr(st, "st_birthtime") and st.st_birthtime:
            ts = st.st_birthtime
        else:
            ts = st.st_mtime
        return datetime.utcfromtimestamp(ts)
    except Exception:
        return _utc_now()

def _parse_summary_txt(folder_path):
    fp = os.path.join(folder_path, "summary.txt")
    if not os.path.isfile(fp):
        return None
    result = {
        "start_time_str": None,
        "total": None,
        "passed": None,
        "failed": None,
        "broken": None,
        "pending": None,
        "ignored": None,
        "skipped": None,
        "compromised": None
    }
    try:
        with open(fp, encoding="utf-8") as fh:
            text = fh.read()
        m = re.search(r"Serenity report generated\s+(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})", text)
        if m:
            result["start_time_str"] = m.group(1)
        def grab(label, key):
            mm = re.search(label + r"\s*:\s*(\d+)", text, re.IGNORECASE)
            if mm:
                result[key] = int(mm.group(1))
        grab(r"Number of test cases", "total")
        grab(r"Passed", "passed")
        grab(r"Failed", "failed")
        grab(r"Failed with errors", "broken")
        grab(r"Pending", "pending")
        grab(r"Ignored", "ignored")
        grab(r"Skipped", "skipped")
        grab(r"Compromised", "compromised")
    except Exception:
        return None
    return result

def _mask_headers(h):
    if not isinstance(h, dict):
        return None
    out = {}
    for k, v in h.items():
        if isinstance(k, str) and k.lower() == "authorization":
            out[k] = "***"
        else:
            out[k] = v
    return out

def _to_snake(s):
    if not isinstance(s, str) or not s:
        return None
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_{2,}", "_", s).strip("_")
    return s.lower()

def _collect_steps(obj):
    steps = []
    if isinstance(obj, dict):
        if isinstance(obj.get("steps"), list):
            steps.extend(obj.get("steps"))
        if isinstance(obj.get("testSteps"), list):
            steps.extend(obj.get("testSteps"))
    return steps

def _extract_req_res(step):
    req = None
    res = None
    if isinstance(step, dict):
        r = step.get("restQuery") or step.get("request")
        if isinstance(r, dict):
            url = r.get("path") or r.get("url")
            try:
                if isinstance(url, str):
                    url = url.replace("`", "").strip()
            except Exception:
                pass
            ctype = r.get("contentType")
            content = r.get("content")
            rh = r.get("requestHeaders")
            cbody = r.get("responseBody")
            sc = r.get("statusCode")
            parts = []
            try:
                s = rh if isinstance(rh, str) else ""
                s = s.replace("`", "").replace("\r", "")
                lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
                for ln in lines:
                    m = re.match(r"^\s*([A-Za-z][A-Za-z\-]*)\s*[:=]\s*(.+)$", ln)
                    if m:
                        k = m.group(1).strip()
                        v = m.group(2).strip()
                        parts.append(f"--header '{k}: {v}'")
            except Exception:
                pass
            if isinstance(ctype, str) and ctype:
                parts.insert(0, f"--header 'Content-Type: {ctype}'")
            curl = None
            try:
                base = f"curl --location --globoff '{url}'"
                if parts:
                    base += " \\\n" + " \\\n".join(parts)
                if isinstance(content, str) and content:
                    base += " \\\n" + f"--data '{content}'"
                curl = base
            except Exception:
                curl = None
            req = {
                "method": r.get("method"),
                "url": url,
                "content": content,
                "contentType": ctype,
                "requestHeaders": rh,
                "responseBody": cbody,
                "statusCode": sc,
                "cUrl": curl
            }
        rr = step.get("restResponse") or step.get("response")
        if isinstance(rr, dict):
            res = {
                "status": rr.get("status") or rr.get("statusCode"),
                "body": rr.get("body")
            }
    return req, res

def _flatten_steps(steps):
    flat = []
    def walk(arr):
        for s in arr or []:
            flat.append(s)
            children = s.get("children") or s.get("steps") or s.get("testSteps")
            if isinstance(children, list) and children:
                walk(children)
    walk(steps or [])
    return flat

def _compute_case_duration(d):
    dur = d.get("duration")
    if isinstance(dur, (int, float)):
        return int(dur)
    steps = _collect_steps(d)
    total = 0
    for s in steps:
        sd = s.get("duration")
        if isinstance(sd, (int, float)):
            total += int(sd)
    return total

def ensure_run_indexes(db):
    try:
        db["test-runs"].create_index([("runId", ASCENDING)], unique=True)
        db["test-cases"].create_index([("runId", ASCENDING), ("testCaseId", ASCENDING)], unique=True)
        db["test-steps"].create_index([("runId", ASCENDING), ("testCaseId", ASCENDING), ("stepOrder", ASCENDING)], unique=True)
        db["attachments"].create_index([("runId", ASCENDING), ("testCaseId", ASCENDING), ("name", ASCENDING), ("path", ASCENDING)], unique=False)
    except Exception as e:
        log_watcher("WARN", f"Create run indexes failed: {e}")


def folder_size(path):
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total += os.path.getsize(fp)
    return total


class FolderHandler(FileSystemEventHandler):
    def __init__(self, coll, base_path, summary_coll, error_coll, fail_coll, key=None):
        self.collection = coll
        self.base_path = base_path
        self.summary_coll = summary_coll
        self.error_coll = error_coll
        self.fail_coll = fail_coll
        self.key = key

    def process_folder(self, folder_path):
        name = os.path.basename(folder_path)
        parent = os.path.dirname(folder_path)
        if parent != self.base_path:
            return

        data = {
            "name": name,
            "path": folder_path,
            "time_insert": datetime.now()
        }

        try:
            res = self.collection.update_one(
                {"name": name, "path": folder_path},
                {"$setOnInsert": data},
                upsert=True
            )
            if getattr(res, "upserted_id", None):
                log_watcher("INSERT", f"Added folder: {name}")
            else:
                log_watcher("SKIP", f"Folder already exists: {name}")
        except DuplicateKeyError:
            log_watcher("SKIP", f"Folder already exists (dupe key): {name}")
        try:
            process_run_folder(folder_path, self.key)
        except Exception as e:
            log_watcher("ERROR", f"process_run_folder: {e}")
        update_summary(self.base_path, self.collection, self.summary_coll, self.key)
        update_error_summary(self.base_path, self.error_coll, self.key)
        update_fail_summary(self.base_path, self.fail_coll, self.key)

    def on_created(self, event):
        try:
            is_dir = event.is_directory or os.path.isdir(event.src_path)
            if is_dir and os.path.dirname(event.src_path) == self.base_path:
                log_watcher("EVENT", f"New folder detected: {event.src_path}")
                self.process_folder(event.src_path)
        except Exception as e:
            log_watcher("ERROR", f"on_created: {e}")

    def on_deleted(self, event):
        try:
            is_dir = event.is_directory or os.path.isdir(event.src_path)
            if is_dir and os.path.dirname(event.src_path) == self.base_path:
                name = os.path.basename(event.src_path)
                log_watcher("EVENT", f"Folder deleted: {event.src_path}")
                self.collection.delete_one({"name": name, "path": event.src_path})
                log_watcher("DELETE", f"Removed from DB: {name}")
                update_summary(self.base_path, self.collection, self.summary_coll, self.key)
                update_error_summary(self.base_path, self.error_coll, self.key)
                update_fail_summary(self.base_path, self.fail_coll, self.key)
        except Exception as e:
            log_watcher("ERROR", f"on_deleted: {e}")

    def on_moved(self, event):
        try:
            is_dir = event.is_directory or os.path.isdir(event.dest_path)
            if is_dir:
                if os.path.dirname(event.src_path) == self.base_path:
                    old_name = os.path.basename(event.src_path)
                    try:
                        self.collection.delete_one({"name": old_name, "path": event.src_path})
                        log_watcher("DELETE", f"Removed from DB: {old_name}")
                    except Exception as e:
                        log_watcher("ERROR", f"Delete old failed: {old_name} - {e}")
                if os.path.dirname(event.dest_path) == self.base_path:
                    self.process_folder(event.dest_path)
                    log_watcher("EVENT", f"Folder moved into base: {event.dest_path}")
                    update_summary(self.base_path, self.collection, self.summary_coll, self.key)
                    update_error_summary(self.base_path, self.error_coll, self.key)
                    update_fail_summary(self.base_path, self.fail_coll, self.key)
        except Exception as e:
            log_watcher("ERROR", f"on_moved: {e}")


def sync_target(base_path, coll):
    try:
        for entry in os.scandir(base_path):
            if entry.is_dir():
                name = os.path.basename(entry.path)
                try:
                    res = coll.update_one(
                        {"name": name, "path": entry.path},
                        {"$setOnInsert": {
                            "name": name,
                            "path": entry.path,
                            "time_insert": datetime.now()
                        }},
                        upsert=True
                    )
                    if getattr(res, "upserted_id", None):
                        log_watcher("SYNC", f"Added folder: {name}")
                except DuplicateKeyError:
                    pass
        for doc in coll.find({}, {"name": 1, "path": 1}):
            p = doc.get("path")
            if isinstance(p, str) and p.startswith(base_path) and not os.path.isdir(p):
                coll.delete_one({"_id": doc["_id"]})
                log_watcher("SYNC", f"Removed stale: {doc.get('name')}")
        for doc in coll.find({"path": {"$regex": f"^{base_path}"}}, {"name": 1, "path": 1}):
            p = doc.get("path")
            if isinstance(p, str) and os.path.dirname(p) != base_path:
                coll.delete_one({"_id": doc["_id"]})
                log_watcher("SYNC", f"Removed nested: {doc.get('name')}")
    except Exception as e:
        log_watcher("ERROR", f"Sync failed: {e}")

def deduplicate(coll, base_path):
    try:
        docs = list(coll.find({"path": {"$regex": f"^{base_path}"}}))
        groups = {}
        for d in docs:
            k = (d.get("name"), d.get("path"))
            arr = groups.get(k) or []
            arr.append(d)
            groups[k] = arr
        for k, arr in groups.items():
            if len(arr) > 1:
                arr_sorted = sorted(arr, key=lambda x: x.get("time_insert") or datetime.min, reverse=True)
                keep = arr_sorted[0]
                for d in arr_sorted[1:]:
                    coll.delete_one({"_id": d["_id"]})
                log_watcher("DEDUP", f"Removed {len(arr_sorted)-1} duplicates for {k[0]}")
    except Exception as e:
        log_watcher("ERROR", f"Dedup failed: {e}")

def ensure_indexes(coll):
    try:
        coll.create_index([("name", ASCENDING), ("path", ASCENDING)], unique=True)
    except Exception as e:
        log_watcher("WARN", f"Create unique index failed: {e}")

def count_results(base_path):
    passing = 0
    broken_flaky = 0
    failed = 0
    skipped = 0
    try:
        for root, dirs, files in os.walk(base_path):
            for f in files:
                if f.lower().endswith('.json'):
                    fp = os.path.join(root, f)
                    try:
                        with open(fp) as fh:
                            d = json.load(fh)
                            r = d.get('result')
                            if not r:
                                continue
                            v = str(r).upper()
                            if v == 'SUCCESS':
                                passing += 1
                            elif v == 'ERROR':
                                broken_flaky += 1
                            elif v == 'FAILURE':
                                failed += 1
                            elif v in ('PENDING', 'SKIPPED'):
                                skipped += 1
                    except Exception:
                        pass
    except Exception:
        pass
    return {
        'passing': passing,
        'broken_flaky': broken_flaky,
        'failed': failed,
        'skipped': skipped,
        'total': passing + broken_flaky + failed + skipped
    }

def update_summary(base_path, coll_folders, coll_summary, key=None):
    counts = count_results(base_path)
    earliest = None
    latest = None
    try:
        docs = list(coll_folders.find({"path": {"$regex": f"^{base_path}"}}, {"time_insert": 1}))
        times = [d.get('time_insert') for d in docs if d.get('time_insert')]
        if times:
            earliest = min(times)
            latest = max(times)
    except Exception:
        pass
    payload = {
        'path': base_path,
        'key': key,
        'passing': counts['passing'],
        'broken_flaky': counts['broken_flaky'],
        'failed': counts['failed'],
        'skipped': counts['skipped'],
        'total': counts['total'],
        'first_time': earliest,
        'latest_time': latest,
        'updated_at': datetime.now()
    }
    try:
        coll_summary.update_one({'path': base_path}, {'$set': payload}, upsert=True)
        log_watcher("SUMMARY", f"Upsert for {base_path}: total={counts['total']}")
    except Exception as e:
        log_watcher("ERROR", f"Summary upsert failed: {e}")

def _build_run_payload(folder_path, project_key=None):
    run_id = os.path.basename(folder_path)
    sum_txt = _parse_summary_txt(folder_path)
    fallback_counts = count_results(folder_path)
    start_time_str = sum_txt.get("start_time_str") if isinstance(sum_txt, dict) else None
    if not start_time_str:
        try:
            st = _folder_start_time(folder_path)
            start_time_str = st.strftime("%d-%m-%Y %H:%M:%S")
        except Exception:
            start_time_str = None
    payload = {
        "runId": run_id,
        "project": project_key,
        "startTime": start_time_str,
        "summary": {
            "total": (sum_txt.get("total") if sum_txt and sum_txt.get("total") is not None else fallback_counts["total"]),
            "passed": (sum_txt.get("passed") if sum_txt and sum_txt.get("passed") is not None else fallback_counts["passing"]),
            "failed": (sum_txt.get("failed") if sum_txt and sum_txt.get("failed") is not None else fallback_counts["failed"]),
            "broken": (sum_txt.get("broken") if sum_txt and sum_txt.get("broken") is not None else fallback_counts["broken_flaky"]),
            "skipped": (sum_txt.get("skipped") if sum_txt and sum_txt.get("skipped") is not None else fallback_counts.get("skipped")),
            "pending": (sum_txt.get("pending") if sum_txt else None),
            "ignored": (sum_txt.get("ignored") if sum_txt else None),
            "compromised": (sum_txt.get("compromised") if sum_txt else None)
        },
        "source": {
            "tool": "serenity",
            "reportPath": folder_path
        },
        "createdAt": _utc_now()
    }
    return payload

def refresh_runs_for_path(base_path, project_key=None):
    coll_runs = db["test-runs"]
    ensure_run_indexes(db)
    try:
        for entry in os.scandir(base_path):
            if entry.is_dir():
                p = entry.path
                payload = _build_run_payload(p, project_key)
                coll_runs.update_one({"runId": payload["runId"]}, {"$set": payload}, upsert=True)
                log_watcher("REFRESH", f"test-runs: {payload['runId']} updated")
    except Exception as e:
        log_watcher("ERROR", f"refresh_runs_for_path failed: {e}")

def process_run_folder(folder_path, project_key=None):
    run_id = os.path.basename(folder_path)
    coll_runs = db["test-runs"]
    coll_cases = db["test-cases"]
    coll_steps = db["test-steps"]
    coll_atts = db["attachments"]
    try:
        payload = _build_run_payload(folder_path, project_key)
        coll_runs.update_one({"runId": run_id}, {"$set": payload}, upsert=True)
    except Exception as e:
        log_watcher("ERROR", f"Insert test-runs failed: {e}")
    try:
        for root, dirs, files in os.walk(folder_path):
            for f in files:
                if not f.lower().endswith(".json"):
                    continue
                if f in ("serenity.configuration.json", "bootstrap-icons.json", "serenity-summary.json"):
                    continue
                fp = os.path.join(root, f)
                data = _read_json(fp)
                if not isinstance(data, dict):
                    continue
                name = data.get("name") or data.get("title")
                tcid = _to_snake(os.path.splitext(f)[0]) or _to_snake(name) or os.path.splitext(f)[0]
                feature = data.get("feature")
                story = None
                tags_arr = []
                tags = data.get("tags")
                if isinstance(tags, list):
                    for t in tags:
                        if isinstance(t, dict):
                            tn = t.get("name") or t.get("tag")
                            tt = t.get("type") or t.get("tagType")
                            if isinstance(tt, str) and tt.lower() in ("feature", "story") and not feature:
                                feature = tn
                            if isinstance(tt, str) and tt.lower() == "story" and not story:
                                story = tn
                            if tn:
                                tags_arr.append(str(tn))
                us = data.get("userStory")
                if isinstance(us, dict):
                    story = story or us.get("storyName") or us.get("name")
                    feature = feature or us.get("path")
                status = data.get("result")
                duration_case = _compute_case_duration(data)
                err = None
                tfc = data.get("testFailureCause")
                if isinstance(tfc, dict):
                    err = tfc.get("message") or tfc.get("errorType")
                elif isinstance(tfc, str):
                    err = tfc
                has_steps = bool(_collect_steps(data))
                has_att = bool(data.get("attachments") or data.get("screenshots"))
                case_doc = {
                    "runId": run_id,
                    "testCaseId": tcid,
                    "name": name,
                    "feature": feature,
                    "story": story,
                    "tags": tags_arr,
                    "status": str(status).upper() if status else None,
                    "duration": duration_case,
                    "errorMessage": err,
                    "hasSteps": has_steps,
                    "hasAttachment": has_att,
                    "createdAt": _utc_now()
                }
                try:
                    coll_cases.update_one({"runId": run_id, "testCaseId": tcid}, {"$set": case_doc}, upsert=True)
                except Exception:
                    pass
                steps = _flatten_steps(_collect_steps(data))
                order = 1
                for s in steps:
                    req, res = _extract_req_res(s)
                    sdoc = {
                        "runId": run_id,
                        "testCaseId": tcid,
                        "stepOrder": order,
                        "name": s.get("description") or s.get("name"),
                        "status": s.get("result"),
                        "duration": s.get("duration"),
                        "request": req,
                        "response": res,
                        "error": s.get("error"),
                        "createdAt": _utc_now()
                    }
                    try:
                        coll_steps.update_one({"runId": run_id, "testCaseId": tcid, "stepOrder": order}, {"$set": sdoc}, upsert=True)
                    except Exception:
                        pass
                    order += 1
                atts = []
                a = data.get("attachments")
                if isinstance(a, list):
                    for it in a:
                        if isinstance(it, dict):
                            atts.append(it)
                sc = data.get("screenshots")
                if isinstance(sc, list):
                    for it in sc:
                        if isinstance(it, dict):
                            atts.append(it)
                for it in atts:
                    nm = it.get("name") or it.get("title")
                    pth = it.get("path") or it.get("source")
                    typ = it.get("type") or it.get("format")
                    adoc = {
                        "runId": run_id,
                        "testCaseId": tcid,
                        "name": nm,
                        "type": typ,
                        "path": pth,
                        "createdAt": _utc_now()
                    }
                    try:
                        coll_atts.update_one(
                            {"runId": run_id, "testCaseId": tcid, "name": nm, "path": pth},
                            {"$set": adoc},
                            upsert=True
                        )
                    except Exception:
                        pass
    except Exception as e:
        log_watcher("ERROR", f"Parse run folder failed: {e}")

def update_error_summary(base_path, coll_error, key=None, top_n:int=10, examples_per:int=5):
    total_error = 0
    cause_counts = {}
    cause_examples = {}

    def scan(obj):
        results = []
        try:
            if isinstance(obj, dict):
                r = obj.get('result')
                if isinstance(r, str) and r.upper() == 'ERROR':
                    causes = obj.get('testFailureCause')
                    tc = obj.get('testCaseName') or obj.get('title') or obj.get('name')
                    use = []
                    if isinstance(causes, list):
                        use = [str(x) for x in causes if x]
                    elif isinstance(causes, str):
                        use = [causes]
                    elif isinstance(causes, dict):
                        et = causes.get('errorType')
                        msg = causes.get('message')
                        val = et or (msg[:200] if isinstance(msg, str) else None)
                        if val:
                            use = [str(val)]
                    if use:
                        results.append((use, tc))
                for v in obj.values():
                    results.extend(scan(v))
            elif isinstance(obj, list):
                for v in obj:
                    results.extend(scan(v))
        except Exception:
            pass
        return results

    try:
        for root, dirs, files in os.walk(base_path):
            for f in files:
                if f.lower().endswith('.json'):
                    fp = os.path.join(root, f)
                    try:
                        with open(fp) as fh:
                            data = json.load(fh)
                        extracted = scan(data)
                        for causes, tc in extracted:
                            total_error += 1
                            for c in causes:
                                cause_counts[c] = cause_counts.get(c, 0) + 1
                                if tc:
                                    arr = cause_examples.get(c) or []
                                    if len(arr) < examples_per and tc not in arr:
                                        arr.append(tc)
                                    cause_examples[c] = arr
                    except Exception:
                        pass

        sorted_causes = sorted(cause_counts.items(), key=lambda x: x[1], reverse=True)
        top_causes = [c for c,_ in sorted_causes[:top_n]]
        ex = {c: cause_examples.get(c, []) for c in top_causes}
        payload = {
            'path': base_path,
            'key': key,
            'totalError': total_error,
            'rootCause': top_causes,
            'ex': ex,
            'updated_at': datetime.now()
        }
        coll_error.update_one({'path': base_path}, {'$set': payload}, upsert=True)
        log_watcher("ERROR-SUMMARY", f"Upsert for {base_path}: totalError={total_error}, causes={len(top_causes)}")
    except Exception as e:
        log_watcher("ERROR", f"Error summary upsert failed: {e}")

def update_fail_summary(base_path, coll_fail, key=None, top_n:int=10, examples_per:int=5):
    total_fail = 0
    cause_counts = {}
    cause_examples = {}

    def scan(obj):
        results = []
        try:
            if isinstance(obj, dict):
                r = obj.get('result')
                if isinstance(r, str) and r.upper() == 'FAILURE':
                    causes = obj.get('testFailureCause')
                    tc = obj.get('testCaseName') or obj.get('title') or obj.get('name')
                    use = []
                    if isinstance(causes, list):
                        use = [str(x) for x in causes if x]
                    elif isinstance(causes, str):
                        use = [causes]
                    elif isinstance(causes, dict):
                        et = causes.get('errorType')
                        msg = causes.get('message')
                        val = et or (msg[:200] if isinstance(msg, str) else None)
                        if val:
                            use = [str(val)]
                    if use:
                        results.append((use, tc))
                for v in obj.values():
                    results.extend(scan(v))
            elif isinstance(obj, list):
                for v in obj:
                    results.extend(scan(v))
        except Exception:
            pass
        return results

    try:
        for root, dirs, files in os.walk(base_path):
            for f in files:
                if f.lower().endswith('.json'):
                    fp = os.path.join(root, f)
                    try:
                        with open(fp) as fh:
                            data = json.load(fh)
                        extracted = scan(data)
                        for causes, tc in extracted:
                            total_fail += 1
                            for c in causes:
                                cause_counts[c] = cause_counts.get(c, 0) + 1
                                if tc:
                                    arr = cause_examples.get(c) or []
                                    if len(arr) < examples_per and tc not in arr:
                                        arr.append(tc)
                                    cause_examples[c] = arr
                    except Exception:
                        pass

        sorted_causes = sorted(cause_counts.items(), key=lambda x: x[1], reverse=True)
        top_causes = [c for c,_ in sorted_causes[:top_n]]
        ex = {c: cause_examples.get(c, []) for c in top_causes}
        payload = {
            'path': base_path,
            'key': key,
            'totalFail': total_fail,
            'rootCause': top_causes,
            'ex': ex,
            'updated_at': datetime.now()
        }
        coll_fail.update_one({'path': base_path}, {'$set': payload}, upsert=True)
        log_watcher("FAIL-SUMMARY", f"Upsert for {base_path}: totalFail={total_fail}, causes={len(top_causes)}")
    except Exception as e:
        log_watcher("ERROR", f"Fail summary upsert failed: {e}")

if __name__ == "__main__":
    targets_cfg = config.get("targets") if isinstance(config, dict) else None
    targets = []
    if targets_cfg:
        for t in targets_cfg:
            p = t.get("watch_path")
            c = t.get("collection")
            s = t.get("summary_collection")
            e = t.get("error_collection")
            f = t.get("fail_collection")
            k = t.get("key") or c
            if p and c:
                targets.append((p, db[c], db[s] if s else db[c+"-summary"], db[e] if e else db[c+"-error"], db[f] if f else db[c+"-fail"], k))
    else:
        if WATCH_PATH and COLLECTION_NAME:
            targets.append((WATCH_PATH, db[COLLECTION_NAME], db[COLLECTION_NAME+"-summary"], db[COLLECTION_NAME+"-error"], db[COLLECTION_NAME+"-fail"], COLLECTION_NAME))
    valid_targets = []
    for item in targets:
        p = item[0]
        if os.path.isdir(p):
            log_watcher("INFO", f"===== Watching folder: {p} =====")
            valid_targets.append(item)
        else:
            log_watcher("WARN", f"Watch path not found: {p} (skipped)")
    targets = valid_targets
    if not targets:
        log_watcher("FATAL", "No valid watch paths found. Please update config.json or environment.")
        raise SystemExit(1)

    for item in targets:
        p, coll, s, e, f, k = item
        deduplicate(coll, p)
        ensure_indexes(coll)
        ensure_run_indexes(db)
        if REFRESH_TEST_RUNS:
            refresh_runs_for_path(p, k)
        sync_target(p, coll)
        try:
            for entry in os.scandir(p):
                if entry.is_dir():
                    process_run_folder(entry.path, k)
        except Exception as _e:
            log_watcher("WARN", f"Initial run parse failed for {p}: {_e}")
        update_summary(p, coll, s, k)
        update_error_summary(p, e, k)
        update_fail_summary(p, f, k)

    observer = Observer()
    handlers = []
    for item in targets:
        p, coll, s, e, f, k = item
        h = FolderHandler(coll, p, s, e, f, k)
        handlers.append(h)
        observer.schedule(h, p, recursive=RECURSIVE)
    observer.start()

    last_sync = time.time()
    try:
        while True:
            time.sleep(1)
            if REFRESH_TEST_RUNS and EXIT_AFTER_REFRESH:
                break
            if time.time() - last_sync >= SYNC_INTERVAL_SECONDS:
                for item in targets:
                    p, coll, s, e, f, k = item
                    if REFRESH_TEST_RUNS:
                        refresh_runs_for_path(p, k)
                    sync_target(p, coll)
                    try:
                        for entry in os.scandir(p):
                            if entry.is_dir():
                                process_run_folder(entry.path, k)
                    except Exception as _e:
                        log_watcher("WARN", f"Periodic run parse failed for {p}: {_e}")
                    update_summary(p, coll, s, k)
                    update_error_summary(p, e, k)
                    update_fail_summary(p, f, k)
                last_sync = time.time()
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
