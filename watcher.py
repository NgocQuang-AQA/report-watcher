import os
import time
import json
import hashlib
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import MongoClient

# Load config
with open("config.json") as f:
    config = json.load(f)

WATCH_PATH = os.getenv("WATCH_PATH", config["watch_path"]) if "watch_path" in config else None
MONGO_URI = os.getenv("MONGO_URI", config["mongo_uri"])
DB_NAME = os.getenv("DB_NAME", config["database"])
COLLECTION_NAME = os.getenv("COLLECTION", config.get("collection", "")) if "collection" in config else ""
RECURSIVE = os.getenv("RECURSIVE", "true").lower() == "true"
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "30"))

# MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def folder_size(path):
    """Tính dung lượng folder (bytes)."""
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total += os.path.getsize(fp)
    return total


class FolderHandler(FileSystemEventHandler):
    def __init__(self, coll, base_path, summary_coll, error_coll, fail_coll):
        self.collection = coll
        self.base_path = base_path
        self.summary_coll = summary_coll
        self.error_coll = error_coll
        self.fail_coll = fail_coll

    def process_folder(self, folder_path):
        """Ghi dữ liệu folder cấp 1 vào DB nếu chưa tồn tại."""
        name = os.path.basename(folder_path)
        parent = os.path.dirname(folder_path)
        if parent != self.base_path:
            return

        # check trùng bằng name + path
        existing = self.collection.find_one({"name": name, "path": folder_path})
        if existing:
            print(f"[SKIP] Folder already exists: {name}")
            return

        data = {
            "name": name,
            "path": folder_path,
            "time_insert": datetime.now()
        }

        self.collection.insert_one(data)
        print(f"[INSERT] Added folder: {name}")
        update_summary(self.base_path, self.collection, self.summary_coll)
        update_error_summary(self.base_path, self.error_coll)
        update_fail_summary(self.base_path, self.fail_coll)

    def on_created(self, event):
        try:
            is_dir = event.is_directory or os.path.isdir(event.src_path)
            if is_dir and os.path.dirname(event.src_path) == self.base_path:
                print(f"[EVENT] New folder detected: {event.src_path}")
                self.process_folder(event.src_path)
        except Exception as e:
            print(f"[ERROR] on_created: {e}")

    def on_deleted(self, event):
        try:
            is_dir = event.is_directory or os.path.isdir(event.src_path)
            if is_dir and os.path.dirname(event.src_path) == self.base_path:
                name = os.path.basename(event.src_path)
                print(f"[EVENT] Folder deleted: {event.src_path}")
                self.collection.delete_one({"name": name, "path": event.src_path})
                print(f"[DELETE] Removed from DB: {name}")
                update_summary(self.base_path, self.collection, self.summary_coll)
                update_error_summary(self.base_path, self.error_coll)
                update_fail_summary(self.base_path, self.fail_coll)
        except Exception as e:
            print(f"[ERROR] on_deleted: {e}")

    def on_moved(self, event):
        try:
            is_dir = event.is_directory or os.path.isdir(event.dest_path)
            if is_dir:
                if os.path.dirname(event.src_path) == self.base_path:
                    old_name = os.path.basename(event.src_path)
                    try:
                        self.collection.delete_one({"name": old_name, "path": event.src_path})
                        print(f"[DELETE] Removed from DB: {old_name}")
                    except Exception as e:
                        print(f"[ERROR] Delete old failed: {old_name} - {e}")
                if os.path.dirname(event.dest_path) == self.base_path:
                    self.process_folder(event.dest_path)
                    print(f"[EVENT] Folder moved into base: {event.dest_path}")
                    update_summary(self.base_path, self.collection, self.summary_coll)
                    update_error_summary(self.base_path, self.error_coll)
                    update_fail_summary(self.base_path, self.fail_coll)
        except Exception as e:
            print(f"[ERROR] on_moved: {e}")


def sync_target(base_path, coll):
    try:
        for entry in os.scandir(base_path):
            if entry.is_dir():
                name = os.path.basename(entry.path)
                if not coll.find_one({"name": name, "path": entry.path}):
                    coll.insert_one({
                        "name": name,
                        "path": entry.path,
                        "time_insert": datetime.now()
                    })
                    print(f"[SYNC] Added folder: {name}")
        for doc in coll.find({}, {"name": 1, "path": 1}):
            p = doc.get("path")
            if isinstance(p, str) and p.startswith(base_path) and not os.path.isdir(p):
                coll.delete_one({"_id": doc["_id"]})
                print(f"[SYNC] Removed stale: {doc.get('name')}")
        for doc in coll.find({"path": {"$regex": f"^{base_path}"}}, {"name": 1, "path": 1}):
            p = doc.get("path")
            if isinstance(p, str) and os.path.dirname(p) != base_path:
                coll.delete_one({"_id": doc["_id"]})
                print(f"[SYNC] Removed nested: {doc.get('name')}")
    except Exception as e:
        print(f"[ERROR] Sync failed: {e}")

def count_results(base_path):
    passing = 0
    broken_flaky = 0
    failed = 0
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
                    except Exception:
                        pass
    except Exception:
        pass
    return {
        'passing': passing,
        'broken_flaky': broken_flaky,
        'failed': failed,
        'total': passing + broken_flaky + failed
    }

def update_summary(base_path, coll_folders, coll_summary):
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
        'passing': counts['passing'],
        'broken_flaky': counts['broken_flaky'],
        'failed': counts['failed'],
        'total': counts['total'],
        'first_time': earliest,
        'latest_time': latest,
        'updated_at': datetime.now()
    }
    try:
        coll_summary.update_one({'path': base_path}, {'$set': payload}, upsert=True)
        print(f"[SUMMARY] Upsert for {base_path}: total={counts['total']}")
    except Exception as e:
        print(f"[ERROR] Summary upsert failed: {e}")

def update_error_summary(base_path, coll_error, top_n:int=10, examples_per:int=5):
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
            'totalError': total_error,
            'rootCause': top_causes,
            'ex': ex,
            'updated_at': datetime.now()
        }
        coll_error.update_one({'path': base_path}, {'$set': payload}, upsert=True)
        print(f"[ERROR-SUMMARY] Upsert for {base_path}: totalError={total_error}, causes={len(top_causes)}")
    except Exception as e:
        print(f"[ERROR] Error summary upsert failed: {e}")

def update_fail_summary(base_path, coll_fail, top_n:int=10, examples_per:int=5):
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
            'totalFail': total_fail,
            'rootCause': top_causes,
            'ex': ex,
            'updated_at': datetime.now()
        }
        coll_fail.update_one({'path': base_path}, {'$set': payload}, upsert=True)
        print(f"[FAIL-SUMMARY] Upsert for {base_path}: totalFail={total_fail}, causes={len(top_causes)}")
    except Exception as e:
        print(f"[ERROR] Fail summary upsert failed: {e}")

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
            if p and c:
                targets.append((p, db[c], db[s] if s else db[c+"-summary"], db[e] if e else db[c+"-error"], db[f] if f else db[c+"-fail"]))
    else:
        if WATCH_PATH and COLLECTION_NAME:
            targets.append((WATCH_PATH, db[COLLECTION_NAME], db[COLLECTION_NAME+"-summary"], db[COLLECTION_NAME+"-error"], db[COLLECTION_NAME+"-fail"]))
    valid_targets = []
    for item in targets:
        p = item[0]
        if os.path.isdir(p):
            print(f"===== Watching folder: {p} =====")
            valid_targets.append(item)
        else:
            print(f"[WARN] Watch path not found: {p} (skipped)")
    targets = valid_targets
    if not targets:
        print("[FATAL] No valid watch paths found. Please update config.json or environment.")
        raise SystemExit(1)

    for item in targets:
        p, coll, s, e, f = item
        sync_target(p, coll)
        update_summary(p, coll, s)
        update_error_summary(p, e)
        update_fail_summary(p, f)

    observer = Observer()
    handlers = []
    for item in targets:
        p, coll, s, e, f = item
        h = FolderHandler(coll, p, s, e, f)
        handlers.append(h)
        observer.schedule(h, p, recursive=RECURSIVE)
    observer.start()

    last_sync = time.time()
    try:
        while True:
            time.sleep(1)
            if time.time() - last_sync >= SYNC_INTERVAL_SECONDS:
                for item in targets:
                    p, coll, s, e, f = item
                    sync_target(p, coll)
                    update_summary(p, coll, s)
                    update_error_summary(p, e)
                    update_fail_summary(p, f)
                last_sync = time.time()
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
