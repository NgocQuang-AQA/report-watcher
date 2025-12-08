# Report Watcher

Công cụ theo dõi thư mục `report_history` và ghi nhận các thư mục mới/xóa vào MongoDB.

## Mục tiêu
- Lắng nghe sự kiện tạo/xóa thư mục trong `watch_path`.
- Lưu thông tin vào MongoDB gồm: `name`, `path`, `size`, `time_insert`.

## Yêu cầu hệ thống
- macOS hoặc Linux.
- Python 3.10+ (đã kiểm thử với Python 3.14).
- MongoDB chạy tại `mongodb://localhost:27017` hoặc địa chỉ bạn cấu hình.

## Chuẩn bị môi trường
Do Python trên macOS (Homebrew) là môi trường được quản lý (PEP 668), khuyến nghị dùng môi trường ảo.

```bash
cd /Users/gz-ngocquang/Build\ SDET/report-watcher
python3 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install watchdog pymongo
```

## Cấu hình
Sửa file `config.json` theo nhu cầu. Ví dụ cấu hình hiện tại:

```json
{
  "watch_path": "/Users/gz-ngocquang/gz-project/global-qa/report_history",
  "mongo_uri": "mongodb://localhost:27017",
  "database": "mydb",
  "collection": "global-qa"
}
```

- `watch_path`: đường dẫn thư mục cần theo dõi. Hãy đảm bảo thư mục tồn tại.
- `mongo_uri`: địa chỉ MongoDB.
- `database`: tên database.
- `collection`: tên collection lưu dữ liệu.

## Chạy tool

```bash
./.venv/bin/python watcher.py
```

Khi chạy thành công sẽ in:

```text
===== Watching folder: /Users/gz-ngocquang/gz-project/global-qa/report_history =====
```

- Dừng chương trình: nhấn `Ctrl + C`.

## Kiểm thử nhanh
1. Mở Finder tới `watch_path`.
2. Tạo một thư mục mới trong `report_history`.
3. Quan sát log:
   - `[EVENT] New folder detected: ...`
   - `[INSERT] Added folder: <tên-folder>`
4. Xóa thư mục vừa tạo:
   - `[EVENT] Folder deleted: ...`
   - `[DELETE] Removed from DB: <tên-folder>`

## Mô hình dữ liệu
Dữ liệu được ghi vào MongoDB có dạng:

```json
{
  "name": "folder_name",
  "path": "/full/path/to/folder",
  "size": 123456,
  "time_insert": "2025-12-02T12:34:56.789Z"
}
```

Ghi chú: hệ thống kiểm tra trùng theo `name + path`. Nếu đã tồn tại sẽ bỏ qua.

## Khắc phục sự cố
- Lỗi `externally-managed-environment` khi `pip install`:
  - Luôn dùng môi trường ảo: `python3 -m venv .venv` rồi cài bằng `./.venv/bin/python -m pip install ...`.
- Không kết nối được MongoDB:
  - Kiểm tra dịch vụ MongoDB đã chạy và `mongo_uri` đúng.
- Không thấy log sự kiện:
  - Đảm bảo bạn tạo/xóa thư mục trực tiếp trong `watch_path`.
  - Đã bật theo dõi đệ quy và đồng bộ định kỳ.

## Tuỳ chọn nâng cao
- Đổi `database`/`collection` để tách dữ liệu theo môi trường khác nhau (ví dụ `global-qa`, `global-cn`).
- Có thể override cấu hình bằng biến môi trường: `WATCH_PATH`, `MONGO_URI`, `DB_NAME`, `COLLECTION`, `RECURSIVE`, `SYNC_INTERVAL_SECONDS`.

## Chạy bằng Docker

### Dev local (macOS/Linux) – nhiều thư mục

Ứng dụng hỗ trợ theo dõi nhiều thư mục qua `targets` trong `config.json`. Trên máy dev local bạn có thể:

- Cách nhanh bằng Docker Compose (sửa đường dẫn theo máy bạn):

```yaml
services:
  report-watcher:
    build: .
    environment:
      MONGO_URI: mongodb://host.docker.internal:27017 # macOS
      DB_NAME: mydb
      RECURSIVE: "true"
      SYNC_INTERVAL_SECONDS: "30"
    volumes:
      - /Users/<user>/path/to/global-qa/report_history:/data/global-qa/report_history:ro
      - /Users/<user>/path/to/global-cn/report_history:/data/global-cn/report_history:ro
      - ./config.docker.json:/app/config.json:ro
    restart: unless-stopped
```

- Hoặc chạy native Python: dùng ngay `config.json` (đã có `targets` mẫu) và chạy `./.venv/bin/python watcher.py`.

Ghi chú:
- Trên Linux dùng `MONGO_URI=mongodb://localhost:27017` nếu MongoDB chạy cùng máy/container network.
- Nếu không dùng nhiều thư mục, có thể đặt `WATCH_PATH`/`COLLECTION` qua biến môi trường để theo dõi 1 thư mục.

### Server WBL (Windows chạy Ubuntu qua WSL) – hai thư mục

Sử dụng `docker-compose.yml` kèm `config.docker.json` (đã có sẵn trong repo) để mount hai thư mục WSL:

```yaml
services:
  report-watcher:
    build: .
    environment:
      MONGO_URI: mongodb://localhost:27017
      DB_NAME: mydb
      RECURSIVE: "true"
      SYNC_INTERVAL_SECONDS: "30"
    volumes:
      - /mnt/d/Project/global-qa/report_history:/data/global-qa/report_history:ro
      - /mnt/d/Project/global-cn/report_history:/data/global-cn/report_history:ro
      - ./config.docker.json:/app/config.json:ro
    restart: unless-stopped
```

Chạy:

```bash
cd /path/to/report-watcher
- docker compose down
- docker compose up -d --build
- docker compose logs -f report-watcher
```

### Dùng Docker thuần (tùy chọn)

```bash
docker build -t report-watcher:latest .
docker run --name report-watcher \
  -e MONGO_URI=mongodb://localhost:27017 \
  -e DB_NAME=mydb \
  -v /mnt/d/Project/global-qa/report_history:/data/global-qa/report_history:ro \
  -v /mnt/d/Project/global-cn/report_history:/data/global-cn/report_history:ro \
  -v "$(pwd)/config.docker.json:/app/config.json:ro" \
  --restart unless-stopped \
  report-watcher:latest
```

Ghi chú:
- Trên macOS dev có thể thay `MONGO_URI` thành `mongodb://host.docker.internal:27017` để kết nối MongoDB trên host.
- Nếu `host.docker.internal` không khả dụng (Linux), có thể chạy MongoDB trong cùng `docker-compose.yml` và dùng `MONGO_URI=mongodb://mongo:27017`.

## Triển khai native (không Docker)

### Dev local
- Dùng `config.json` có sẵn `targets` để theo dõi cả `global-qa` và `global-cn` (sửa đường dẫn nếu cần).
- Chạy: `./.venv/bin/python watcher.py`

### Server WBL
- Nếu chỉ muốn theo dõi một thư mục, có thể dùng biến môi trường:

```bash
export WATCH_PATH=/mnt/d/Project/global-qa/report_history
export MONGO_URI=mongodb://localhost:27017
export DB_NAME=mydb
export COLLECTION=global-qa
export RECURSIVE=true
export SYNC_INTERVAL_SECONDS=30
./.venv/bin/python watcher.py
```

### Lưu ý khi dùng thư mục mount mạng
- Một số mount (CIFS/NFS/NAS) có thể không phát sinh đầy đủ inotify sự kiện; đồng bộ định kỳ (`SYNC_INTERVAL_SECONDS`) sẽ bù.
- Muốn gần realtime hơn, giảm `SYNC_INTERVAL_SECONDS` (ví dụ 10–15 giây) qua biến môi trường hoặc trong `config`.

## Lệnh nhanh
```bash
# Kích hoạt môi trường ảo (tạm thời):
source .venv/bin/activate

# Chạy
python watcher.py

# Thoát môi trường ảo
deactivate
```
