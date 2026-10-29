# messenger-bridge

Sidecar Go giữ phiên Messenger cá nhân (tài khoản Facebook của người dùng) phía server 24/7 cho hộp thư Yona. Một tiến trình phục vụ mọi tài khoản đã nối: nhận tin trên cả kênh Messenger thường và kênh mã hoá đầu cuối, gửi tin thay Laravel, báo sự kiện về Laravel qua webhook có hàng đợi bền trên đĩa.

Hợp đồng HTTP hai chiều với Laravel: `plans/261029-personal-messenger-channel/contract.md` (trong repo Yona).

## Thành phần

| Tệp | Việc |
|---|---|
| `main.go` | khởi động, khôi phục mọi slot, tắt êm |
| `config.go` | đọc env, chặn khởi động production khi thiếu bí mật |
| `httpapi.go`, `apitypes.go` | các endpoint, kiểm secret hằng thời gian, mã lỗi |
| `login.go` | đăng nhập nhiều bước theo `login_token`, giữ trong RAM 10 phút |
| `slots.go` | mỗi `connection_id` một slot: nối 2 kênh, tự nối lại, phân biệt rớt tạm thời với mất phiên |
| `directory.go` | danh bạ hội thoại/liên hệ theo tài khoản, chỉ mục tin gần đây, chống echo |
| `inbound.go` | chuẩn hoá sự kiện 2 kênh thành webhook trung tính |
| `outbound.go` | gửi chữ/media/trả lời, cảm xúc, thu hồi, đang gõ, đã đọc, chấp nhận tin chờ |
| `media.go` | giải mã media mã hoá theo yêu cầu, phục vụ `GET /media/{token}`; tải media Laravel đưa để gửi |
| `outbox.go` | hàng đợi webhook FIFO bền trên đĩa |
| `sessionstore.go` | tệp phiên AES-256-GCM, kho khoá thiết bị SQLCipher |
| `logging.go` | log JSON mỗi dòng, lọc dữ liệu thô của thư viện |
| `importsession.go` | lệnh phụ chuyển phiên của bản chạy thử sang định dạng mã hoá (chỉ dùng khi phát triển) |

## Build

Cần Go 1.26.x và gcc (SQLCipher biên dịch bằng CGO).

```bash
export GOTOOLCHAIN=local
go build -ldflags "-X main.version=$(git rev-parse --short HEAD)" -o bin/messenger-bridge .
```

`GET /` trả `version` lấy từ `-X main.version`; không truyền thì lấy commit Go ghi trong binary, không có thì `dev`. Binary đọc `.env` ở thư mục đang chạy (cwd); biến đã có trong môi trường tiến trình được ưu tiên.

## Chạy local

```bash
cp .env.example .env
# điền BRIDGE_SESSION_KEY và BRIDGE_DEVICE_KEY: openssl rand -base64 32 (mỗi khoá một lần)
./bin/messenger-bridge
curl http://127.0.0.1:8614/
```

Cổng mặc định 8614, chỉ nghe 127.0.0.1. Ba secret phải trùng với `.env` của Laravel:

| Laravel | sidecar | header |
|---|---|---|
| `MESSENGER_BRIDGE_UI_SECRET` | `BRIDGE_UI_SECRET` | `X-Bridge-UI-Secret` (`/auth/*`, `/health`, `/media/*`) |
| `MESSENGER_BRIDGE_SEND_SECRET` | `BRIDGE_SEND_SECRET` | `X-Bridge-Send-Secret` (`/messages/*`, `/requests/*`) |
| `MESSENGER_BRIDGE_WEBHOOK_SECRET` | `LARAVEL_WEBHOOK_SECRET` | `X-Messenger-Bridge-Secret` (webhook) |

### Dùng lại phiên của bản chạy thử

Khi bridge đang **tắt**:

```bash
./bin/messenger-bridge import-session --connection-id 12 --from ~/www/yona-messenger-bridge/state
```

Lệnh đọc `session.json` + `device.db` (sqlite thường) rồi ghi tệp phiên mã hoá cho slot 12 và chép toàn bộ khoá mã hoá đầu cuối vào kho SQLCipher.

## Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `BRIDGE_ENV` | (rỗng) | `production` bật kiểm tra khởi động chặt |
| `BRIDGE_HTTP_HOST` / `BRIDGE_HTTP_PORT` | `127.0.0.1` / `8614` | chỉ nhận địa chỉ loopback |
| `BRIDGE_PUBLIC_URL` | `http://HOST:PORT` | gốc của link `/media/{token}` gửi cho Laravel |
| `LARAVEL_BASE_URL` | — | webhook tới `{LARAVEL_BASE_URL}/webhooks/messenger-personal`; production bắt buộc https |
| `LARAVEL_WEBHOOK_SECRET` | — | header `X-Messenger-Bridge-Secret` |
| `BRIDGE_UI_SECRET`, `BRIDGE_SEND_SECRET` | — | rỗng = từ chối mọi request nhóm đó |
| `BRIDGE_SESSION_KEY` | — | **bắt buộc**, 32 byte base64: niêm phong tệp phiên và outbox |
| `BRIDGE_DEVICE_KEY` | — | **bắt buộc**, 32 byte base64: khoá SQLCipher của kho khoá thiết bị |
| `BRIDGE_STATE_DIR` | `./state` | phiên, `devices.db`, outbox, media tạm |
| `BRIDGE_MAX_SESSIONS` | `50` | quá thì đăng nhập mới nhận `session_cap_reached` (429) |
| `BRIDGE_FETCH_HOST_ALLOWLIST` | (host:port của Laravel) | `host[:port]` được tải media khi gửi, khớp đúng như ghi; http chỉ cho localhost/127.0.0.1 |
| `BRIDGE_MAX_MEDIA_BYTES` | `26214400` | giới hạn media gửi đi |
| `BRIDGE_LOG_FILE` | (rỗng) | ghi thêm log ra tệp ngoài stdout |
| `BRIDGE_LOG_LEVEL` / `BRIDGE_LIBRARY_LOG_LEVEL` | `info` / `warn` | mức log của bridge / của thư viện |

## Dữ liệu và bảo mật

- Mật khẩu và mã 2FA chỉ đi thẳng từ request tới Meta, không ghi log, không ghi đĩa.
- Phiên (cookie, native session, id thiết bị): mỗi slot một tệp `state/sessions/<id>.session`, AES-256-GCM, quyền 0600.
- Khoá mã hoá đầu cuối: `state/devices.db`, SQLCipher mã hoá toàn bộ tệp (đầu tệp không còn là `SQLite format 3`).
- Outbox webhook cũng được niêm phong bằng `BRIDGE_SESSION_KEY` vì chứa nội dung tin.
- Media mã hoá chỉ được giải mã khi Laravel gọi `/media/{token}`, giữ 1 giờ rồi xoá.
- Log: JSON mỗi dòng, có `connection_id`, `event`, chat, `source_id`, thời gian, mã lỗi; không bao giờ có nội dung tin, tên, cookie, token, mật khẩu. Log thư viện đi qua bộ lọc bỏ `data`, `val`, `step_data` và mọi khối XML.
- Bảo vệ khi lộ đĩa, bản sao lưu, tệp bị chép; không chống được người đã có quyền root trên server (khoá nằm trong `.env` cùng máy).

## Vận hành

Một tiến trình duy nhất (outbox dựa trên tệp, không chạy nhiều bản cùng thư mục state). Production: supervisor chạy `bin/messenger-bridge` với user `www`, cwd = `services/messenger-bridge/`, stdout vào `storage/logs/messenger-bridge/bridge.log`; `BRIDGE_STATE_DIR` là đường dẫn tuyệt đối dưới `storage/` của app vì thư mục repo thuộc root. `SIGTERM` lưu phiên rồi thoát.

## Giấy phép

AGPL-3.0, xem `LICENSE`. Mã nguồn sidecar phải được công khai cho người dùng dịch vụ. Dùng thư viện giao thức Messenger của mautrix-meta (AGPL-3.0), whatsmeow (MPL-2.0) và go-sqlcipher (BSD).
