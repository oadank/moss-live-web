# moss-live-web

MOSS-Live 全双工语音互动网页。浏览器(麦克风) ↔ 本服务(WS 中转/播放回执/工具执行) ↔ api.mosi.cn。

## 本地跑

```bash
npm i
MOSS_API_KEY=sk-xxx PORT=3900 node server.mjs
# 可选扩展（不配也能跑，语音/游戏全部正常）：
#   MOSS_API_KEY_PARTY=sk-yyy   第二个账号 key，给 /party 用（官方每账号并发=1，两页同开必须配）
#   OPENMEM_URL=http://host:3466/mcp   接记忆中枢，开启 search_memory/save_memory
#   WB_BRIDGE_URL=http://host:18799       接后台 agent 桥，开启 dispatch_agent/wb_history")
```

打开 http://127.0.0.1:3900

## N5105 部署

1. 目录：`/opt/moss-live`（server.mjs / public/index.html / package.json）
2. 依赖：`cd /opt/moss-live && npm i --registry=https://registry.npmmirror.com`
3. 密钥：`/opt/moss-live/moss-live.env`（600 权限）内容：
   ```
   MOSS_API_KEY=sk-xxx
   PORT=3900
   ```
4. 服务：`cp deploy/moss-live.service /etc/systemd/system/ && systemctl daemon-reload && systemctl enable --now moss-live`
5. nginx：`live.example.com` server 块（8080）→ `proxy_pass http://127.0.0.1:3900`，带 WebSocket 头：
   ```
   proxy_http_version 1.1;
   proxy_set_header Upgrade $http_upgrade;
   proxy_set_header Connection "upgrade";
   proxy_set_header Host $http_host;
   ```
6. cloudflared config.yml 加 ingress：hostname `live.example.com` → `http://localhost:8080`（带 httpHostHeader）
7. Cloudflare DNS 加 CNAME `live` → `<tunnel-id>.cfargotunnel.com`
