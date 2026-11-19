// MOSS-Live 语音互动网页 · 服务端
// 浏览器(麦克风/页面) <-> 本服务(WS 中转 + 播放回执 + 工具执行) <-> wss://api.mosi.cn/v1/realtime
import { WebSocketServer, WebSocket } from 'ws';
import { attachDelegation } from '@mossai/mosslive';
import { spawn, exec } from 'node:child_process';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// 语音服务不许崩：任何未捕获异常都只记日志，进程活着才有人能说话
process.on('uncaughtException', e => console.log('[uncaught]', (e && e.stack || e.message || e).toString().slice(0, 500)));
process.on('unhandledRejection', e => console.log('[unrej]', ((e && e.message) || String(e)).slice(0, 500)));

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 3900);
const KEY = process.env.MOSS_API_KEY || '';
// 🔴 官方限制：每个用户在该模型下并发数 = 1（只能同时保 1 个会话）。
// 主页和年会页同时开就会互相挤掉=连上就秒断。所以年会走独立账号 key（env 变量名 MOSS_MAIN_API_KEY，
// 与 LiteLLM 的 main 组共用同一个变量，2026-11-18 老大定：一个 key 一个名，不许另造重名变量），没配就回退主 key。
const KEY_PARTY = process.env.MOSS_MAIN_API_KEY || KEY;
// 🔴 key 分工（2026-11-18 老大定）：/party=客户商用线，用最早账号 KEY(MOSS_API_KEY，老大专门充值)；/games+主页=自家测试线，共用第二账号 KEY_PARTY(MOSS_MAIN_API_KEY)。
function keyFor(convo) { return convo === 'party' ? KEY : KEY_PARTY; }
const UPSTREAM = 'wss://api.mosi.cn/v1/realtime?model=moss-live-1.0';
const WORKSPACE = '/opt/moss-live/workspace'; // 鱼的本机工作区（run_cmd 默认在这干活）
try { fs.mkdirSync(WORKSPACE, { recursive: true }); } catch {}
const PUBLIC_DIR = path.join(__dirname, 'public');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PERSONA_FILE = path.join(DATA_DIR, 'persona.txt');
const HISTORY_FILE = path.join(DATA_DIR, 'history.jsonl');
// 可选：接自己的记忆中枢（openmem MCP）。不配则 search_memory/save_memory 返回“未启用”，不影响语音。
const OPENMEM_URL = process.env.OPENMEM_URL || '';

// ---- 多会话聊天记录：每个会话一个 jsonl，全文存档 + 到阈值压缩成摘要 ----
const CONVOS_DIR = path.join(DATA_DIR, 'convos');
const ARCHIVE_DIR = path.join(CONVOS_DIR, '_archived'); // 归档区：可恢复或真删除
const TURN_CHAR_BUDGET = 7000;   // 每次连接逐字带多少字符的最近对话
const COMPACT_THRESHOLD = 140;   // 单个会话存档超过这么多行才触发压缩
const COMPACT_KEEP = 80;         // 压缩时最近多少行保持逐字
const safeConvoId = id => (String(id || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 40)) || 'default';
const convoFile = id => path.join(CONVOS_DIR, safeConvoId(id) + '.jsonl');
const summaryFile = id => path.join(CONVOS_DIR, safeConvoId(id) + '.summary.txt');

// 一次性迁移：老的单文件历史挪到 default 会话
try {
  if (fs.existsSync(HISTORY_FILE) && !fs.existsSync(convoFile('default'))) {
    fs.mkdirSync(CONVOS_DIR, { recursive: true });
    fs.copyFileSync(HISTORY_FILE, convoFile('default'));
  }
} catch {}

function readHistoryLines(convo) {
  try { return fs.readFileSync(convoFile(convo), 'utf8').trim().split('\n').filter(Boolean); } catch { return []; }
}
function parseTurnLine(line) {
  try {
    const j = JSON.parse(line);
    const who = j.role === 'user' ? '用户' : '小默';
    const text = String(j.text || '').replace(/\s+/g, ' ').trim();
    return text ? `${who}：${text.slice(0, 200)}` : '';
  } catch { return ''; }
}
function appendConvo(convo, role, text) {
  try {
    fs.mkdirSync(CONVOS_DIR, { recursive: true });
    fs.appendFileSync(convoFile(convo), JSON.stringify({ ts: new Date().toISOString(), role, text: String(text).slice(0, 500) }) + '\n', 'utf8');
    if (readHistoryLines(convo).length > COMPACT_THRESHOLD) compactHistory(convo).catch(() => {});
  } catch {}
}
function getSummary(convo) {
  try { return fs.readFileSync(summaryFile(convo), 'utf8').trim(); } catch { return ''; }
}
function listConvos(dir = CONVOS_DIR) {
  try {
    return fs.readdirSync(dir).filter(f => f.endsWith('.jsonl')).map(f => {
      const id = f.replace(/\.jsonl$/, '');
      const lines = fs.readFileSync(path.join(dir, f), 'utf8').trim().split('\n').filter(Boolean);
      let title = '新对话';
      for (const l of lines) { try { const j = JSON.parse(l); if (j.role === 'user' && j.text) { title = String(j.text).slice(0, 20); break; } } catch {} }
      return { id, title, count: lines.length, mtime: fs.statSync(path.join(dir, f)).mtimeMs };
    }).sort((a, b) => b.mtime - a.mtime);
  } catch { return []; }
}
let compacting = false;
async function compactHistory(convo) {
  if (compacting) return;
  const lines = readHistoryLines(convo);
  if (lines.length <= COMPACT_THRESHOLD) return;
  compacting = true;
  try {
    const oldLines = lines.slice(0, lines.length - COMPACT_KEEP);
    const oldText = oldLines.map(parseTurnLine).filter(Boolean).join('\n');
    if (oldText.length > 100) {
      const prompt = '把聊天记录压成一份连续的背景摘要（第三人称）。保留：用户是谁、偏好、聊过的事、约定、没聊完的话题；去掉寒暄和重复，1000 字以内，只输出摘要正文。'
        + `\n【已有摘要】\n${getSummary(convo) || '（无）'}\n【新增长的记录】\n${oldText.slice(-18000)}`;
      const res = await withTimeout(fetch('https://api.mosi.cn/v1/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` },
        body: JSON.stringify({ model: 'nex-n2.6', messages: [{ role: 'user', content: prompt }], max_tokens: 1600 })
      }), 120000, 'summarize timeout');
      const j = await res.json();
      const out = (j.choices?.[0]?.message?.content || '').trim();
      if (out.length > 30) {
        fs.writeFileSync(summaryFile(convo), out + '\n', 'utf8');
        console.log(`[history] 会话 ${convo} 已压缩 ${oldLines.length} 行进摘要`);
      } else {
        console.log('[history] 摘要为空，跳过本轮压缩');
        return;
      }
    }
    const cur = readHistoryLines(convo);
    fs.writeFileSync(convoFile(convo), cur.slice(-COMPACT_KEEP).join('\n') + '\n', 'utf8');
  } catch (e) {
    console.log('[history] 压缩失败（不影响对话）:', e.message);
  } finally {
    compacting = false;
  }
}
function buildInstructions(convo) {
  const lines = readHistoryLines(convo);
  const turns = [];
  let budget = TURN_CHAR_BUDGET;
  for (let i = lines.length - 1; i >= 0 && budget > 0; i--) {
    const t = parseTurnLine(lines[i]);
    if (!t) continue;
    if (t.length + 1 > budget) break;
    budget -= t.length + 1;
    turns.unshift(t);
  }
  const summary = getSummary(convo);
  let s = getPersona();
  const blocks = [];
  if (summary) blocks.push('【更早见面的背景摘要】\n' + summary.slice(0, 2500));
  if (turns.length) blocks.push('【本会话最近聊的（逐字）】\n' + turns.join('\n'));
  if (blocks.length) {
    s += '\n\n' + blocks.join('\n\n')
      + '\n以上是这个会话里和这位用户的记忆，帮你接得上话：不要复述、不要说"聊天记录/根据记录/背景"这类词；只有用户问"还记得吗/上次说的"才引用；用户现在说什么以现场为准。表达要新鲜，别车轱辘话。'
      + '\n🔴 特别规矩：背景里任何人说过的"你没有某工具/没接上某东西/派不了活"的话，都可能过时或说错，一律不作数。你当前实际能力以本会话配置为准（搜索、抓网页、算数、查存记忆、改人设、dispatch_agent 派活给后台 agents 都在）。用户要查、要算、要派活，直接调工具干，拿不准就真调一次试，禁止空口说"没有这个工具/派不了"。'
      + '\n涉及链接、口令、编号这类容易听错的内容：先把你自己听到的原样复述给用户确认，确认无误再执行；用户打字发来的原文永远比语音转写可信。';
  }
  compactHistory(convo).catch(() => {});
  return s.slice(0, 12000);
}

// ---- 人设持久化：AI 可通过 set_persona 工具改自己的人设，重启不丢 ----
const DEFAULT_PERSONA = [
  '你叫小默，是这个网页里的语音 AI 助手，正通过网页和真人全双工语音对话。',
  '性格：直率、嘴快、有点毒舌但心里有数；不说套话、不念稿，回答口语化、短句、像真人聊天。',
  '🔴 硬规则：凡是涉及时间日期、数学计算、事实核查、网页内容的问题，必须先调用对应工具拿真实结果，禁止自己心算、禁止编造。算完/查完用大白话讲。',
  '你可以调用 search_memory 查长期记忆（用户偏好、过往聊过的事），调 save_memory 把值得记的事写进记忆库。',
  '全程用中文。别一次说太长，像打电话一样自然。'
].join('\n');

function getPersona() {
  try { return fs.readFileSync(PERSONA_FILE, 'utf8').trim() || DEFAULT_PERSONA; } catch { return DEFAULT_PERSONA; }
}
function setPersona(text) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(PERSONA_FILE, String(text).trim() + '\n', 'utf8');
  return getPersona();
}

// ---- openmem MCP client（可选的记忆中枢，配 OPENMEM_URL 才启用）----
let mcpSession = null, mcpReady = false, rpcId = 0;
async function mcpRaw(method, params, notify = false) {
  if (!OPENMEM_URL) throw new Error('记忆中枢未启用（没配 OPENMEM_URL）');
  const headers = { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' };
  if (mcpSession) headers['mcp-session-id'] = mcpSession;
  const body = { jsonrpc: '2.0', method };
  if (!notify) body.id = ++rpcId;
  if (params) body.params = params;
  const res = await withTimeout(fetch(OPENMEM_URL, { method: 'POST', headers, body: JSON.stringify(body) }), 15000, 'openmem timeout');
  const sid = res.headers.get('mcp-session-id');
  if (sid) mcpSession = sid;
  if (notify) return null;
  const ct = res.headers.get('content-type') || '';
  if (ct.includes('text/event-stream')) {
    const text = await res.text();
    const line = text.split('\n').find(l => l.startsWith('data:'));
    return JSON.parse(line.slice(5).trim());
  }
  return res.json();
}
async function mcpInit() {
  if (mcpReady) return;
  await mcpRaw('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'moss-live', version: '0.1.0' } });
  await mcpRaw('notifications/initialized', null, true).catch(() => {});
  mcpReady = true;
}
async function mcpTool(name, args) {
  await mcpInit();
  let r = await mcpRaw('tools/call', { name, arguments: args });
  if (r.error && String(r.error.message || '').includes('Session')) {
    mcpSession = null; mcpReady = false;
    await mcpInit();
    r = await mcpRaw('tools/call', { name, arguments: args });
  }
  if (r.error) throw new Error(r.error.message || 'tool error');
  const content = r.result?.content || [];
  return content.map(c => c.text || '').join('\n').slice(0, 20000) || '(empty)'; // 3500 会把技能总表 JSON 截断成非法 JSON，放大到 2 万
}

const TOOLS = [
  { type: 'function', name: 'current_time', description: '获取当前日期时间（北京时间）', parameters: { type: 'object', properties: {}, required: [] } },
  { type: 'function', name: 'calculate', description: '精确计算数学表达式，返回数值', parameters: { type: 'object', properties: { expression: { type: 'string', description: '数学表达式，如 (12+8)*3.5' } }, required: ['expression'] } },
  { type: 'function', name: 'websearch', description: '搜索互联网获取实时/事实信息，返回若干条结果的标题、链接与摘要', parameters: { type: 'object', properties: { query: { type: 'string', description: '搜索关键词' } }, required: ['query'] } },
  { type: 'function', name: 'fetch_webpage', description: '抓取一个公开网页的正文文本（截断返回）', parameters: { type: 'object', properties: { url: { type: 'string', description: '完整 http(s) 链接' } }, required: ['url'] } },
  { type: 'function', name: 'search_memory', description: '在长期记忆库检索往期记录（用户偏好、聊过的事、项目信息）。用户问"还记得吗/我上次说的/我是谁"类问题必须先查这里', parameters: { type: 'object', properties: { query: { type: 'string', description: '检索关键词' } }, required: ['query'] } },
  { type: 'function', name: 'save_memory', description: '把值得长期记住的信息写入记忆库（用户偏好、重要事实、明确结论）。闲聊不要存；每次只存一条', parameters: { type: 'object', properties: { content: { type: 'string', description: '要记住的内容，一句话说清' } }, required: ['content'] } },
  { type: 'function', name: 'get_persona', description: '查看你当前生效的人设全文', parameters: { type: 'object', properties: {}, required: [] } },
  { type: 'function', name: 'set_persona', description: '修改你自己的人设/性格/说话风格，立即生效并永久保存（下次会话也生效）', parameters: { type: 'object', properties: { persona: { type: 'string', description: '完整的新人设文本' } }, required: ['persona'] } },
  { type: 'function', name: 'dispatch_agent', description: '把任务派给后台 WorkBuddy agent 执行（它有全套技能和工具：写代码、改配置、深度调研、整理文档都能干）。适合耗时的重活；派完先告诉用户一声，结果出来会自动播报', parameters: { type: 'object', properties: { task: { type: 'string', description: '要执行的任务，一句话说清目标' } }, required: ['task'] } },
  { type: 'function', name: 'run_cmd', description: '在本机（N5105 服务器）执行 shell 命令并返回输出。可用来 ls/cat/查进程/查端口/跑脚本等真活，默认工作目录 /opt/moss-live/workspace', parameters: { type: 'object', properties: { cmd: { type: 'string', description: '要执行的 shell 命令' }, cwd: { type: 'string', description: '工作目录，可选' } }, required: ['cmd'] } },
  { type: 'function', name: 'read_file', description: '读取本机一个文本文件的内容（最多 20 万字符）', parameters: { type: 'object', properties: { path: { type: 'string', description: '文件绝对路径' } }, required: ['path'] } },
  { type: 'function', name: 'write_file', description: '把文本内容写入本机文件（自动建目录，覆盖同名文件）', parameters: { type: 'object', properties: { path: { type: 'string', description: '文件绝对路径' }, content: { type: 'string', description: '要写入的文本' } }, required: ['path', 'content'] } },
  { type: 'function', name: 'list_dir', description: '列出本机一个目录的内容（[d]=目录 [f]=文件）', parameters: { type: 'object', properties: { path: { type: 'string', description: '目录绝对路径' } }, required: ['path'] } },
  { type: 'function', name: 'append_file', description: '向本机文本文件末尾追加内容（文件不存在则创建）', parameters: { type: 'object', properties: { path: { type: 'string', description: '文件绝对路径' }, content: { type: 'string', description: '要追加的文本' } }, required: ['path', 'content'] } },
  { type: 'function', name: 'delete_file', description: '删除本机一个文件或空目录（慎用，不可恢复）', parameters: { type: 'object', properties: { path: { type: 'string', description: '文件/目录绝对路径' } }, required: ['path'] } },
  { type: 'function', name: 'mkdir', description: '在本机创建目录（自动建父目录）', parameters: { type: 'object', properties: { path: { type: 'string', description: '目录绝对路径' } }, required: ['path'] } },
  { type: 'function', name: 'http_request', description: '发一个 HTTP 请求并返回状态码与响应正文（支持 GET/POST/PUT/DELETE，可带头和体）。要调用 REST API、下载文本内容都用它', parameters: { type: 'object', properties: { url: { type: 'string', description: '完整 URL' }, method: { type: 'string', description: '默认 GET' }, headers: { type: 'object', description: '请求头对象，可选' }, body: { type: 'string', description: '请求体文本，可选' } }, required: ['url'] } },
  { type: 'function', name: 'page_cmd', description: '在当前宿主网页（年会互动大屏）里执行 JavaScript，直接操作页面。页面提供 PARTY API：PARTY.startGame("guess"|"bomb"|"roll"|"quiz"|"idiom"|"hot") 切换游戏、PARTY.showWord(词)/PARTY.hideWord() 大屏词语、PARTY.setBig(标题,副文) 改大屏、PARTY.setScore(n) 改比分、PARTY.clue() 给一条线索(你描述我猜/年度热词/猜歌,返回线索照念,线索数才真+1)、PARTY.hit() 猜中:加分+公布答案+撒花、PARTY.nextQuestion() 快问快答出下一题、PARTY.hint() 快问快答给线索、PARTY.bombArm(1~100数字) 埋雷、PARTY.bombGuess(报数数字) 报数收窄区间/命中爆炸、PARTY.confetti() 撒花、PARTY.rollNames(["张三","李四"]) 滚动点名、PARTY.stopRoll() 停止抽取、PARTY.say(文本) 大屏字幕。也可写任意 JS 改 DOM。仅宿主页面（party 会话）可用', parameters: { type: 'object', properties: { code: { type: 'string', description: '要执行的 JS 代码，如 PARTY.confetti()' } }, required: ['code'] } },
  { type: 'function', name: 'page_state', description: '🔴 实时读取宿主网页（年会大屏）现状：有哪些游戏、当前在玩哪个、比分、大屏文字、可用按钮、词库数、名单。凡用户问页面相关内容（有几个游戏/现在什么状态/大屏显示啥），或你要操作页面前，必须先调这个拿实况回答，禁止凭记忆或瞎猜', parameters: { type: 'object', properties: {}, required: [] } },
  { type: 'function', name: 'wb_history', description: '查看 WorkBuddy 里的历史会话。不传 query = 返回会话列表（标题+时间）；传标题关键词 = 切到那个会话并返回它的聊天内容（最后约 3000 字）。用于找回之前派过的活、继续之前的话题', parameters: { type: 'object', properties: { query: { type: 'string', description: '会话标题关键词，可不传' } }, required: [] } }
];

// ---------------- utils ----------------
const stripTags = h => String(h)
  .replace(/<script[\s\S]*?<\/script>/gi, ' ')
  .replace(/<style[\s\S]*?<\/style>/gi, ' ')
  .replace(/<[^>]+>/g, ' ')
  .replace(/&nbsp;/g, ' ')
  .replace(/\s+/g, ' ')
  .trim();

function safeCalc(expr) {
  if (!/^[-+*/%().\d\s]+$/.test(expr)) throw new Error('expression contains unsupported chars');
  const r = Function(`"use strict";return (${expr})`)();
  if (typeof r !== 'number' || !isFinite(r)) throw new Error('bad result');
  return r;
}

function assertPublicUrl(u) {
  const a = new URL(u);
  if (!/^https?:$/.test(a.protocol)) throw new Error('only http/https allowed');
  const h = a.hostname;
  const bad = h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal') || h === '0.0.0.0'
    || /^127\./.test(h) || /^10\./.test(h) || /^192\.168\./.test(h) || /^169\.254\./.test(h)
    || /^172\.(1[6-9]|2\d|3[01])\./.test(h);
  if (bad) throw new Error('internal addresses are not allowed');
  return a.toString();
}

const withTimeout = (p, ms, msg = 'timeout') => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(msg)), ms))]);
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---- 打字输入：edge-tts(免费) 把文字转语音，再当"用户说话"喂给 MOSS ----
let edgeTtsMod = null;
async function textToSpeechMp3(text) {
  if (!edgeTtsMod) edgeTtsMod = await import('msedge-tts');
  const { MsEdgeTTS, OUTPUT_FORMAT } = edgeTtsMod;
  if (!globalThis.__edgeTts) {
    globalThis.__edgeTts = new MsEdgeTTS();
    await globalThis.__edgeTts.setMetadata('zh-CN-XiaoxiaoNeural', OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3);
  }
  const chunks = [];
  const { audioStream } = globalThis.__edgeTts.toStream(text);
  for await (const c of audioStream) chunks.push(c);
  return Buffer.concat(chunks);
}
function mp3ToPcm16k(mp3) {
  return new Promise((resolve, reject) => {
    const p = spawn('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', 'pipe:0', '-f', 's16le', '-ar', '16000', '-ac', '1', 'pipe:1']);
    const out = [];
    p.stdout.on('data', c => out.push(c));
    p.stderr.on('data', () => {});
    p.on('error', reject);
    p.on('close', code => code === 0 ? resolve(Buffer.concat(out)) : reject(new Error('ffmpeg exit ' + code)));
    p.stdin.end(mp3);
  });
}
// 脆弱内容：链接/口令/长编号——经"转语音再识别"会失真（链接被裁/听错），改走原文注入通道
function isFragileText(t) {
  if (/https?:\/\/|www\.|@|\.com|\.cn|\.net|\.org/i.test(t)) return true;
  if (/[a-z0-9]{10,}/i.test(t)) return true;
  if (/\d{7,}/.test(t)) return true;
  return false;
}
const ttsPcmCache = new Map(); // 🔴 触发音固定就那几个，合成一次缓存 PCM
async function speakTyped(session, text, retried, t0) {
  if (session.ttsBusy) { session.pendingTyped = (session.pendingTyped || []).concat(text); return; }
  session.ttsBusy = true;
  session.injecting = true; // 注入打字语音期间不收麦克风
  try {
    let pcm = ttsPcmCache.get(text);
    if (!pcm) {
      const t1 = Date.now();
      const mp3 = await withTimeout(textToSpeechMp3(text), 12000, 'tts timeout'); // edge-tts 偶发挂死，必须限时
      pcm = await mp3ToPcm16k(mp3);
      if (text.length <= 12) ttsPcmCache.set(text, pcm);
      console.log(`[typed] TTS 合成 ${Date.now() - t1}ms (${text.slice(0, 8)})`);
    } else if (t0) console.log('[typed] 触发音命中缓存');
    const FRAME = 1280; // 40ms @16k mono s16
    for (let off = 0; off < pcm.length; off += FRAME) { // 按实时速度推（MOSS VAD 靠节奏断句，整段猛灌=不回复，实测）
      if (!session.alive || session.up.readyState !== WebSocket.OPEN) return;
      const buf = Buffer.alloc(FRAME);
      pcm.copy(buf, 0, off, Math.min(off + FRAME, pcm.length));
      session.up.send(Buffer.concat([Buffer.from([0x00]), buf]), { binary: true });
      await sleep(40);
    }
    for (let i = 0; i < 10; i++) { // 收尾静音，帮 MOSS 断句
      if (!session.alive || session.up.readyState !== WebSocket.OPEN) return;
      session.up.send(Buffer.concat([Buffer.from([0x00]), Buffer.alloc(FRAME)]), { binary: true });
      await sleep(40);
    }
    if (t0) console.log(`[typed] 注入完成 ${Date.now() - t0}ms`);
  } catch (e) {
    console.log('[typed] tts failed:', e.message);
    if (!retried) { // 失败自动重试一次（edge-tts 挂死多为偶发）
      session.ttsBusy = false; session.injecting = false;
      await sleep(400);
      return speakTyped(session, text, true);
    }
    session.sendBrowser({ action: 'server.typed.error', data: { msg: e.message, text: String(text).slice(0, 60) } });
  } finally {
    session.ttsBusy = false;
    session.injecting = false;
    if (session.pendingTyped && session.pendingTyped.length) speakTyped(session, session.pendingTyped.shift());
  }
}

// anysearch MCP 搜索（效果远好于 Bing RSS，实测返回真实新闻）
async function anysearchSearch(query) {
  const key = process.env.ANYSEARCH_API_KEY || '';
  if (!key) throw new Error('ANYSEARCH_API_KEY missing');
  const headers = { Authorization: `Bearer ${key}`, 'X-Anysearch-Client': 'mcp/1.0.0', 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' };
  await withTimeout(fetch('https://api.anysearch.com/mcp', { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'moss-live', version: '0.1.0' } } }) }), 15000, 'anysearch init timeout');
  const res = await withTimeout(fetch('https://api.anysearch.com/mcp', { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'search', arguments: { query } } }) }), 30000, 'anysearch timeout');
  const j = await res.json();
  const text = (j.result?.content || []).map(c => c.text || '').join('\n');
  if (!text.trim()) throw new Error('anysearch empty');
  return text.slice(0, 3500);
}

async function runTool(name, args, session) {
  switch (name) {
    case 'current_time':
      return new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
    case 'calculate':
      return String(safeCalc(String(args.expression || '')));
    case 'fetch_webpage': {
      const url = assertPublicUrl(String(args.url || ''));
      const res = await withTimeout(fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'Accept-Language': 'zh-CN,zh;q=0.9' },
        redirect: 'follow'
      }), 20000, 'fetch timeout');
      const html = await res.text();
      return `HTTP ${res.status}\n` + stripTags(html).slice(0, 4000);
    }
    case 'websearch': {
      const q = String(args.query || '').slice(0, 150);
      try {
        console.log(`[websearch] trying anysearch: ${q}`);
        const r = await withTimeout(anysearchSearch(q), 35000, 'anysearch timeout');
        console.log(`[websearch] anysearch ok (${r.length} chars)`);
        return r;
      } catch (e) {
        console.log(`[websearch] anysearch failed: ${e.message}; fallback to bing rss`);
      }
      const qe = encodeURIComponent(q);
      const res = await withTimeout(fetch(`https://www.bing.com/search?q=${qe}&format=rss&setlang=zh-CN&count=8`, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
          'Accept-Language': 'zh-CN,zh;q=0.9'
        }
      }), 20000, 'search timeout');
      const xml = await res.text();
      const out = [];
      const re = /<item>[\s\S]*?<title>([\s\S]*?)<\/title>[\s\S]*?<link>([\s\S]*?)<\/link>[\s\S]*?<description>([\s\S]*?)<\/description>[\s\S]*?<\/item>/g;
      let m;
      while ((m = re.exec(xml)) && out.length < 6) {
        const title = stripTags(m[1]);
        const link = stripTags(m[2]);
        const snippet = stripTags(m[3]).slice(0, 300);
        if (title) out.push(`${out.length + 1}. ${title}\n${link}\n${snippet}`);
      }
      if (!out.length) return '搜索没有解析到结果，请如实告诉用户。';
      return out.join('\n\n');
    }
    case 'search_memory':
      return await mcpTool('mh_search', { query: String(args.query || '').slice(0, 300), top_k: 5, requester: 'moss-live' });
    case 'save_memory': {
      const content = String(args.content || '').trim().slice(0, 2000);
      if (!content) return '内容为空，未保存';
      await mcpTool('mh_skill', { agent: 'moss-live' }); // 领写入票
      return await mcpTool('mh_write', { content, source: 'moss-live', layer: 'm', category: 'misc', tags: ['语音对话'] });
    }
    case 'get_persona':
      return getPersona();
    case 'dispatch_agent': {
      const task = String(args.task || '').trim().slice(0, 1000);
      if (!task) return '任务为空，未派发';
      if (!process.env.WB_BRIDGE_URL) return '后台派发未启用（部署时没配 WB_BRIDGE_URL）';
      // 异步派单：桥注入 WB 后立刻回单号，不等结果；结果由桥回调 /task-result，鱼主动播报
      const r = await withTimeout(fetch(process.env.WB_BRIDGE_URL + '/dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Bridge-Token': process.env.BRIDGE_TOKEN || '' },
        body: JSON.stringify({ task, wait_ms: 0, convo: session ? session.convoId : 'default' })
      }), 90000, 'wb-bridge timeout');
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || 'bridge failed');
      if (j.async) return '已派单（单号 ' + j.id + '）。任务已交给后台执行，完成后我会主动播报结果；这期间你可以继续跟我聊别的。';
      return 'WorkBuddy 已完成，结果如下：\n' + j.reply;
    }
    case 'run_cmd': {
      const cmd = String(args.cmd || '').trim();
      if (!cmd) return '命令为空';
      const cwd = String(args.cwd || WORKSPACE);
      return await new Promise(resolve => {
        exec(cmd, { cwd, timeout: 120000, maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) => {
          let out = '';
          if (err) out += '[exit ' + (err.code ?? 'err') + '] ';
          out += String(stdout || '');
          if (stderr) out += '\n[stderr] ' + String(stderr);
          resolve((out || '(无输出)').slice(0, 60000));
        });
      });
    }
    case 'read_file': {
      const p = String(args.path || '');
      if (!p) return '路径为空';
      const s = fs.readFileSync(p, 'utf8');
      return s.length > 200000 ? s.slice(0, 200000) + '\n[已截断]' : s;
    }
    case 'write_file': {
      const p = String(args.path || '');
      if (!p) return '路径为空';
      const content = String(args.content ?? '');
      fs.mkdirSync(path.dirname(p), { recursive: true });
      fs.writeFileSync(p, content, 'utf8');
      return '已写入 ' + p + '（' + Buffer.byteLength(content, 'utf8') + ' 字节）';
    }
    case 'list_dir': {
      const p = String(args.path || WORKSPACE);
      const list = fs.readdirSync(p).map(f => {
        let tag = '[f]', size = '';
        try { const st = fs.statSync(path.join(p, f)); if (st.isDirectory()) tag = '[d]'; else size = ' (' + st.size + 'B)'; } catch {}
        return tag + ' ' + f + size;
      });
      return list.join('\n') || '(空目录)';
    }
    case 'append_file': {
      const p = String(args.path || '');
      if (!p) return '路径为空';
      fs.mkdirSync(path.dirname(p), { recursive: true });
      fs.appendFileSync(p, String(args.content ?? ''), 'utf8');
      return '已追加到 ' + p;
    }
    case 'delete_file': {
      const p = String(args.path || '');
      if (!p || p === '/' || p === WORKSPACE) return '路径无效（不许删根目录或工作区）';
      const st = fs.statSync(p);
      if (st.isDirectory()) fs.rmdirSync(p); else fs.unlinkSync(p);
      return '已删除 ' + p;
    }
    case 'mkdir': {
      const p = String(args.path || '');
      if (!p) return '路径为空';
      fs.mkdirSync(p, { recursive: true });
      return '目录已就绪 ' + p;
    }
    case 'http_request': {
      const url = assertPublicUrl(String(args.url || ''));
      const method = String(args.method || 'GET').toUpperCase();
      const headers = Object.assign({ 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'Accept-Language': 'zh-CN,zh;q=0.9' }, (args.headers && typeof args.headers === 'object') ? args.headers : {});
      const res = await withTimeout(fetch(url, { method, headers, body: ['GET', 'HEAD'].includes(method) ? undefined : String(args.body ?? ''), redirect: 'follow' }), 30000, 'http timeout');
      const text = await res.text();
      return `HTTP ${res.status}\n` + text.slice(0, 20000);
    }
    case 'page_cmd': {
      const code = String(args.code || '').slice(0, 4000);
      if (!code) return 'code 为空';
      if (!session) return '会话不存在';
      return await session.pageCmd(code);
    }
    case 'page_state': {
      if (!session) return '会话不存在';
      return await session.pageCmd('PARTY.state()');
    }
    case 'wb_history': {
      if (!process.env.WB_BRIDGE_URL) return '历史查询未启用（部署时没配 WB_BRIDGE_URL）';
      const q = String(args.query || '').trim().slice(0, 60);
      const r = await withTimeout(fetch(process.env.WB_BRIDGE_URL + '/wb-convo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Bridge-Token': process.env.BRIDGE_TOKEN || '' },
        body: JSON.stringify({ query: q })
      }), 60000, 'wb-bridge timeout');
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || 'bridge failed');
      return j.data;
    }
    case 'set_persona': {
      const p = String(args.persona || '').trim();
      if (p.length < 10) return '人设太短，未保存（至少 10 个字）';
      const saved = setPersona(p);
      for (const s of activeSessions) { try { await s.pushInstructions(); } catch {} }
      return '人设已更新并立即生效';
    }
    default:
      return `unknown tool: ${name}`;
  }
}

// ---------------- 后台任务执行器（MOSS delegation -> 本服务执行）----------------
// ReAct 循环：后台 agent 可多轮调用工具（搜索/抓网页/计算/记忆），干完把结论带回对话
async function taskRunner(task, { signal, reportProgress }) {
  const objective = String(task.objective || '').slice(0, 1500);
  reportProgress('收到，后台开干：' + objective.slice(0, 60), { spoken: true });
  const sys = '你是后台任务执行器，靠多轮调用工具完成目标。每轮只输出一个 JSON 对象：要么 {"tool":"工具名","args":{...}}，要么 {"final":"最终结论"}。可用工具：websearch(query)、fetch_webpage(url)、calculate(expression)、current_time()、search_memory(query)。结论必须基于工具结果，查不到就明说，禁止编造。最多 6 轮。';
  const msgs = [
    { role: 'system', content: sys },
    { role: 'user', content: '任务：' + objective + '\n（当前时间：' + new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) + '）' }
  ];
  for (let step = 1; step <= 6; step++) {
    const res = await withTimeout(fetch('https://api.mosi.cn/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` },
      body: JSON.stringify({ model: 'nex-n2.6', messages: msgs, max_tokens: 800 })
    }), 90000, 'backend model timeout');
    const j = await res.json();
    const txt = j.choices?.[0]?.message?.content || '';
    let plan = null;
    const m = txt.match(/\{[\s\S]*\}/);
    if (m) { try { plan = JSON.parse(m[0]); } catch {} }
    if (plan && plan.tool) {
      reportProgress(`第 ${step} 步：${plan.tool} ${plan.args ? JSON.stringify(plan.args).slice(0, 80) : ''}`, { spoken: step <= 2 });
      let out;
      try { out = await withTimeout(Promise.resolve(runTool(plan.tool, plan.args || {})), 40000, 'tool timeout'); }
      catch (e) { out = '工具失败: ' + e.message; }
      msgs.push({ role: 'assistant', content: txt.slice(0, 500) });
      msgs.push({ role: 'user', content: '工具结果：\n' + String(out).slice(0, 3000) + '\n\n继续。信息足够就输出 {"final":"结论"}。' });
      continue;
    }
    return String((plan && plan.final) || txt).slice(0, 2000) || '没有得出结果。';
  }
  return '任务步数达到上限，先汇报目前拿到的信息。';
}

let skillListCache = { at: 0, text: '', failUntil: 0 };
const SKILL_FALLBACK = '· all-platform-video-extract（抖音下载、抖音转文字、v.douyin.com、视频提取）\n· comfyui-ops（ComfyUI 生图、8090、换模型）\n· model-router（用哪个模型、模型调度）\n· toonflow-drama（漫剧、短剧制作、画布创作）\n· img-prompt-engine（生图提示词、角色设定图）\n· h3-video（视频提示词、分镜）\n· win-desktop-gui-control（本机桌面操作、自动点击）\n· wechat-oa（公众号文章、草稿）\n· openmem-memory（查记忆、写回记忆）';
async function getSkillList() {
  if (skillListCache.text && Date.now() - skillListCache.at < 6 * 3600 * 1000) return skillListCache.text;
  if (Date.now() < skillListCache.failUntil) return SKILL_FALLBACK;
  let raw = '';
  try {
    raw = await mcpTool('mh_tool', { name: '技能库总表' });
    let src = raw;
    if (String(src).trim().startsWith('{')) { // openmem 把结果包成 {ok,name,answer} JSON —— 表格在 answer 字段里，原样解析即可
      try {
        const j = JSON.parse(src);
        src = String(j.answer || j.text || j.content || '');
      } catch {}
    }
    const lines = String(src).split('\n').filter(l => l.startsWith('| ')).map(r => { const c = r.split('|').map(x => x.trim()); return c[1] && c[1] !== '技能' ? `· ${c[1]}${c[2] ? '（' + c[2].slice(0, 26) + '）' : ''}` : ''; }).filter(Boolean);
    const text = lines.join('\n').slice(0, 1800);
    if (text.length > 100) { skillListCache = { at: Date.now(), text, failUntil: 0 }; console.log(`[skills] 技能清单注入 ${lines.length} 项`); return text; }
    throw new Error('技能总表解析为空');
  } catch (e) {
    console.log('[skills] 拉取技能总表失败，用内置兜底清单:', e.message, '| raw前240字:', String(raw).slice(0, 240));
    try { fs.writeFileSync('/tmp/skills_raw.txt', String(raw).slice(0, 6000)); } catch {}
    skillListCache.failUntil = Date.now() + 5 * 60 * 1000; // 失败只兜 5 分钟，之后重试
    return SKILL_FALLBACK;
  }
}

// ---------------- per-browser MOSS session ----------------
class MossSession {
  constructor(browser, convo, token) {
    this.browser = browser;
    this.convoId = safeConvoId(convo);
    this.token = token || '';
    this.pendingCtx = [];     // response.audio.context FIFO
    this.playQueue = [];      // {ref, durMs}
    this.outstanding = 0;
    this.finished = false;
    this.drained = false;
    this.completedSent = false;
    this.playing = false;
    this.executed = new Set();
    this.pendingContinue = null;
    this.lastRun = null;
    this.alive = true;
    this.aiSpeaking = false;
    this.typedCtx = null; // 用户刚打字发来的原文（脆弱内容走原文注入通道）
    this.resultCtx = null; // 后台任务结果（完工主动播报通道）
    this.pageReady = false; // 宿主页面（年会页）已报到
    this.cmdSeq = 0;
    this.cmdWaiters = new Map();
    this.keepAliveTimer = null; // 🔴 浏览器断开期间由服务端持续推静音，MOSS 会话真不断

    this.up = new WebSocket(UPSTREAM, {
      headers: { Authorization: `Bearer ${keyFor(this.convoId)}` }, // 🔴 年会页用独立 key，避开官方"每账号并发数=1"的抢连接
      handshakeTimeout: 15000,
      maxPayload: 16 * 1024 * 1024,
      perMessageDeflate: false // 实时音频别压缩，减少抖动
    });
    this.up.on('open', async () => {
      console.log(`[conn] upstream open (sessions=${activeSessions.size + 1})`);
      this.sendUp({ action: 'attach_session', memory_scope: 'session', delegation: { type: 'client' } });
      this.sendUp({ action: 'media.mode', binary_protocol: 'tagged-v1' });
      // 🔴 官方示例流程：media.mode 之后要发 mic_state / video.state（2026-11-18 对照官方 Python 示例补上）
      this.sendUp({ action: 'mic_state', enabled: true });
      await this.pushInstructions();
      // 后台委派：复杂任务由 MOSS 派给本服务的执行器（接外部能力/agents 的入口）
      attachDelegation({ socket: this.up, run: taskRunner, onError: code => console.log(`[delegation] ${code}`) });
      // 派活结果补报：用户不在线时完工的任务，重连后补给它
      const pend = takePendingResult(this.convoId);
      if (pend) {
        this.resultCtx = pend;
        await this.pushInstructions();
        setTimeout(() => { if (this.alive) speakTyped(this, '后台任务完工了，看下结果'); }, 2500);
      }
    });
    this.up.on('message', (data, isBinary) => this.onUpMessage(data, isBinary));
    this.up.on('error', e => {
      console.log(`[up] 上游连接错误: ${e.message}`); this.log(`上游连接错误: ${e.message}`);
      // 🔴 建连阶段 HTTP 4xx = 额度/鉴权问题（402 insufficient_credits / 401 key 无效），重试一万次也不会好，
      // 必须打 fatal 标记让页面停止自动重连（官方文档：建连错误见 error-codes，与运行时断连是两回事）
      const m = String(e.message || '').match(/Unexpected server response: (\d{3})/);
      if (m) this.upFatal = Number(m[1]);
    });
    this.up.on('close', (code, reason) => {
      console.log(`[up] 上游关闭 code=${code} reason=${String(reason || '').slice(0, 120)} convo=${this.convoId}${this.upFatal ? ' fatal=' + this.upFatal : ''}`);
      this.alive = false;
      this.sendBrowser({ action: this.upFatal ? 'server.upstream.fatal' : 'server.upstream.closed', data: { code, http: this.upFatal || 0, reason: String(reason || '').slice(0, 200) } });
      try { this.browser && this.browser.close(); } catch {}
    });
  }

  detachBrowser() {
    this.browser = null;
    this.startKeepAlive(); // 页面走了，但 MOSS 会话继续活着：服务端替它推静音
  }
  attachBrowser(browser) {
    // 🔴 接管前先复位回声门状态：AI 说话中途断线时 aiSpeaking 会停在 true，重连后没人发 audio.done
    // → 麦克风被永久丢弃 = 用户怎么喊都听不到（13:47 老大实测"依然无法听到"的根因）
    this.aiSpeaking = false; this.micBlockedUntil = 0; this.bargeUntil = 0; this.loudRun = 0;
    // 🔴 双开同 tok 时不再 close 旧连接（来回踢=重连风暴，12:39 日志实锤）：新连接直接接管，
    // 旧连接它自己 onclose 里会判 `old.browser === browser` 不成立就不再摘话筒，安静退休。
    this.browser = browser;
    this.stopKeepAlive();
  }
  // 🔴 真·不断：每 40ms 推一帧静音（与浏览器上行同样的 tagged-v1 格式），MOSS 侧完全无感
  startKeepAlive() {
    if (this.keepAliveTimer) return;
    const SILENT = Buffer.concat([Buffer.from([0x00]), Buffer.alloc(1280)]);
    this.keepAliveTimer = setInterval(() => {
      if (!this.alive) { this.stopKeepAlive(); return; }
      if (this.up.readyState !== WebSocket.OPEN) { this.stopKeepAlive(); return; }
      if (this.browser) { this.stopKeepAlive(); return; } // 页面回来了，交还话筒
      try { this.up.send(SILENT, { binary: true }); } catch { this.stopKeepAlive(); }
    }, 40);
    console.log('[keepalive] 页面已断开，服务端接管推静音（会话保持）');
  }
  stopKeepAlive() {
    if (this.keepAliveTimer) { clearInterval(this.keepAliveTimer); this.keepAliveTimer = null; console.log('[keepalive] 停止（话筒已交还页面）'); }
  }

  sendUp(o) { if (this.alive && this.up.readyState === WebSocket.OPEN) this.up.send(JSON.stringify(o)); }
  sendBrowser(o) { if (this.browser && this.browser.readyState === WebSocket.OPEN) this.browser.send(JSON.stringify(o)); }
  log(msg) { this.sendBrowser({ action: 'server.log', data: { msg } }); }

  onUpMessage(data, isBinary) {
    if (isBinary) {
      // 🔴🔴 2026-11-18 23:40 "还是很卡"根因嫌疑：原来 pendingCtx 空就直接 return = 把这一帧音频整帧丢掉。
      // 音频帧和 response.audio.context 不是严格 1:1 时（一个 context 配多帧，或 context 迟到），
      // 丢的帧就是听感上的"断断续续/卡成PPT"。现在改成：有 ctx 就配对上报，没 ctx 也照样转发（只是不上报完成），绝不丢声音。
      const ref = this.pendingCtx.length ? this.pendingCtx.shift() : null;
      if (!ref) this.dropCtxN = (this.dropCtxN || 0) + 1;
      if (this.browser && this.browser.readyState === WebSocket.OPEN) {
        // 官方 demo 语义：下行二进制帧原样转发（浏览器按 48kHz Int16 LE 播放，整帧含标记字节一起读）
        this.browser.send(data, { binary: true });
        this.framesOut = (this.framesOut || 0) + 1;
        this.foBytes = (this.foBytes || 0) + data.length; this.foN = (this.foN || 0) + 1;
        if (this.framesOut === 1 || this.framesOut % 100 === 0) console.log(`[conn] framesOut=${this.framesOut} 均帧长=${(this.foBytes / this.foN / 2 / 48000 * 1000).toFixed(0)}ms 无ctx帧=${this.dropCtxN || 0}`);
      }
      if (ref) {
        this.outstanding++;
        this.playQueue.push({ ref, durMs: Math.max(20, ((data.length - 1) / 2 / 48000) * 1000) });
        this.pump();
      }
      return;
    }
    let ev; try { ev = JSON.parse(data.toString()); } catch { return; }
    const a = ev.action;
    if (a === 'input.transcript.done') { if (!this.injecting) appendConvo(this.convoId, 'user', ev.data?.text || ''); }
    if (a === 'output.transcript.done') {
      appendConvo(this.convoId, 'ai', ev.data?.text || '');
      // 🔴 机器强制兜底（老大实测 AI 嘴上说"撒了"实际没调工具）：本轮说了庆祝话术但没发起 page_cmd
      // → 服务端直接替它撒。措辞要覆盖"撒了/撒花/撒完"等所有说法（11:51 实测只说"撒了"漏匹配）。
      if ((this.convoId === 'party' || this.convoId === 'games') && !this.toolCalledThisTurn && /撒(了|花|完)|彩带|礼花|恭喜|抽中|猜对|答对|砰|爆炸|上台/.test(ev.data?.text || '')) {
        console.log('[party] AI 说庆祝话但没调工具，服务端兜底撒花');
        this.pageCmd('PARTY.confetti()').catch(() => {});
      }
    }
    if (a === 'response.function_call_arguments.done') this.toolCalledThisTurn = true;
    if (a === 'session.updated') {
      // 🔴 官方规矩：配置错误在这查（session.updated.data.error 非空 = 会话配置有问题，工具/指令全没生效）
      const err = ev.data?.error;
      console.log('[sess] session.updated' + (err ? ' ❌ 配置错误: ' + String(err).slice(0, 200) : ' ok'));
      if (err) this.log('会话配置错误: ' + err);
    } else if (['input.transcript.done', 'response.audio.started', 'response.audio.done', 'response.audio.stop', 'response.interrupted', 'response.ended', 'error', 'conversation.item.created', 'response.function_call_arguments.done'].includes(a)) {
      console.log(`[evt] ${a} ${JSON.stringify(ev.data || {}).slice(0, 220)}`);
    }
    if (a === 'session_attached') { this.upSessionId = ev.data?.session_id || null; }
    if (a === 'response.audio.context') { this.pendingCtx.push(ev.data || {}); return; }
    // 🔴 半双工防回声：AI 说话期间不收麦——否则喇叭里的 AI 声被麦克风收回，
    // MOSS 误判有人抢话（barge-in），把句子从中间掐断=听感磕磕绊绊
    if (a === 'response.audio.started') { this.aiSpeaking = true; this.aiSpeakSince = Date.now(); this.micBlockedUntil = 0; this.toolCalledThisTurn = false; } // 🔴 aiSpeakSince 必须赋值：它是 60 秒保险丝的唯一依据，缺了它 aiSpeaking 一旦卡住=麦克风永久封死（2026-11-18 23:16 日志实锤）
    if (a === 'response.audio.done' || a === 'response.audio.stop' || a === 'response.interrupted' || a === 'response.ended') { this.aiSpeaking = false; this.aiSpeakSince = 0; this.bargeUntil = 0; this.loudRun = 0; this.micBlockedUntil = Date.now() + 600; } // 🔴 加 response.ended 复位+aiSpeakSince 清零：只发 ended 不发 done 的轮次会让 aiSpeaking 卡 true=麦克风永久封死（23:16 实锤） // 余音窗 1.5s→0.6s：AI 说完用户接话不用等；0.6s 内低音量帧仍挡掉防尾音回声
    if (a === 'response.audio.done' || a === 'response.audio.stop') {
      this.finished = true;
      this.lastRun = ev.data || {};
      this.tryDrain();
    }
    if (a === 'conversation.item.created' && this.pendingContinue) {
      this.pendingContinue = null;
      this.sendUp({ action: 'response.create', response: { call_ids: [ev.data?.item?.call_id].filter(Boolean) } });
      return;
    }
    if (a === 'response.done') {
      // 官方语义：response.done.output 里也可能带 function_call，按 call_id 去重后同样派发
      for (const call of (ev.data?.response?.output || [])) {
        if (call.type === 'function_call') this.onToolCall(call);
      }
      if (this.typedCtx || this.resultCtx) { this.typedCtx = null; this.resultCtx = null; this.pushInstructions(); } // 一次性背景，用完即清
    }
    if (a === 'response.function_call_arguments.done') { this.onToolCall(ev.data || ev); return; }
    if (this.injecting && (a === 'input.transcript.done' || a === 'input.transcript.updated' || a === 'input.speech.started' || a === 'input.speech.stopped')) return; // 打字注入的回声不上屏（页面已显示过打字原文，别重复）
    this.sendBrowser(ev); // 其余事件透传给页面
  }

  pump() {
    if (this.playing) return;
    this.playing = true;
    const next = () => {
      const c = this.playQueue.shift();
      if (!c) { this.playing = false; return; }
      setTimeout(() => {
        if (!this.alive) return;
        this.sendUp({
          action: 'playback.chunk.completed',
          chunk_id: c.ref.chunk_id, run_id: c.ref.run_id, turn_id: c.ref.turn_id
        });
        this.outstanding--;
        this.tryDrain();
        next();
      }, Math.max(0, c.durMs));
    };
    next();
  }

  tryDrain() {
    if (this.finished && !this.drained && !this.completedSent && this.outstanding <= 0) {
      this.drained = true;
      this.completedSent = true;
      this.sendUp({ action: 'playback.completed', run_id: this.lastRun?.run_id, turn_id: this.lastRun?.turn_id });
    }
  }

  onToolCall(ev) {
    const callId = ev.call_id;
    const name = ev.name;
    if (!callId || !name || this.executed.has(callId)) return;
    this.executed.add(callId);
    let args = {};
    try { args = ev.arguments ? JSON.parse(ev.arguments) : {}; } catch { args = { raw: ev.arguments }; }
    this.sendBrowser({ action: 'server.tool.start', data: { call_id: callId, name, args } });
    const timeoutMs = name === 'dispatch_agent' ? 600000 : 30000; // 派活是长任务（桥等 540s，留余量）
    withTimeout(Promise.resolve().then(() => runTool(name, args, this)), timeoutMs, 'tool timeout')
      .then(out => {
        this.sendBrowser({ action: 'server.tool.done', data: { call_id: callId, name, ok: true, output: String(out).slice(0, 600) } });
        this.finishTool(callId, String(out));
      })
      .catch(e => {
        this.sendBrowser({ action: 'server.tool.done', data: { call_id: callId, name, ok: false, output: e.message } });
        this.finishTool(callId, `工具执行失败: ${e.message}`);
      });
  }

  finishTool(callId, output) {
    this.pendingContinue = callId;
    this.sendUp({ action: 'conversation.item.create', item: { type: 'function_call_output', call_id: callId, output: output.slice(0, 4000) } });
    setTimeout(() => {
      if (this.pendingContinue === callId) { // 没等到 created 的兜底
        this.pendingContinue = null;
        this.sendUp({ action: 'response.create', response: { call_ids: [callId] } });
      }
    }, 6000);
  }

  async pushInstructions() {
    // 🔴 官方规矩（platform.moss.cn/docs/reference/moss-live）：工具列表在启用音频前一次配置完成，
    // 会话中途尽量不换（名称/顺序/描述逐字不变）——所以这里只更 instructions，绝不重发 tools。
    let extra = '';
    if (this.typedCtx) extra += '\n\n【用户刚通过打字发来的消息原文（逐字准确）】\n' + this.typedCtx + '\n这是用户当前要说的话。你听到的语音只是提示音或残缺转写；处理和回应这条消息时，必须以上面的原文为准，不要依赖你听到的发音。回应完这条后，这段背景即作废。';
    if (this.resultCtx) extra += '\n\n【系统通知：用户之前派给后台（WorkBuddy）的任务已经有结果了】\n' + this.resultCtx + '\n请现在主动向用户播报这个结果（口语化说结果本身，别念"系统通知"四个字）。播报完这条背景即作废。';
    let instr = buildInstructions(this.convoId);
    try { const skills = await getSkillList(); if (skills) instr += '\n\n【可调度的后台技能清单（实时有效；用 dispatch_agent 派给 WorkBuddy 执行，任务里写清要用哪个技能/触发词）】\n' + skills + '\n🔴 这份清单就是你的技能库（含抖音下载/视频转文字/生图/漫剧制作等）。用户提到相关需求时按触发词匹配并派活，禁止说“没挂载技能/没有这个能力”。'; } catch {}
    if (this.convoId === 'party' || this.convoId === 'games') instr += '\n\n【宿主页面：年会互动大屏】你是嵌在年会网页里的 AI 主持人。🔴 最重要规矩：玩家任何时候开口（打招呼、闲聊、问问题、报答案），你都必须立刻正常开口回应，绝不许沉默待机——沉默=事故。"主持游戏"只是你工作的一部分，不是开口说话的前提。\n🔴🔴 游戏锁定：下面【页面实时状态】是系统刚报给你的页面现状（每次自动更新，可信）。你完全住在这个页面里，对页面全知全能：每个回答都必须以【页面实时状态】为准，凭记忆瞎答=事故。🔴 主持人有主动权：你觉得节目需要换游戏（比如玩了好几轮炸弹想换脱口秀），你可以直接调 PARTY.startGame("...") 切过去并向观众宣布；玩家口头的"换游戏/玩XX/来段脱口秀/不想玩这个了"更是明确指令，立刻执行。玩家说的话如果和当前页面/游戏对不上（比如页面开着数字炸弹，他却在聊别的话题），不要硬拗，先用一句话问他确认："是想继续玩当前这个，还是咱们聊刚才那个话题？"\n页面玩法清单：描述我猜/年度热词/猜歌猜影/数字炸弹/快问快答/成语接龙/幸运点名/脱口秀/默契大考验/真心话大冒险/绕口令/看图猜词。🔴 线索质量：屏幕上的线索是写好的整句话（画面/心情/同类/谜面），绝不许自己另编线索，也别报「第几个字」；想看下一条是什么就先调 PARTY.clueText() 再念。🔴🔴 藏词类游戏（你描述我猜/数字炸弹）的答案不用等人发、更不许开口问玩家要——就在下面【页面实时状态】的「当前答案」字段里，每次现查都在，自己读。描述我猜的词和炸弹数字绝对不能用嘴念出来、也别用英文/拼音/拆字说出包含的字，只准拿来给线索；答案字段为空说明还没开局，等玩家点按钮，不要编。\n🔴🔴 改比分唯一的路：调 page_cmd 执行 PARTY.setScore(新分值) 或 PARTY.addScore(n)，返回"得分已改为 N"才算真改。嘴上说"记你账上/加一分"但没调工具=撒谎，绝对禁止。\n【喊停必须动手】玩家说「停 / 停一下 / 就他 / 停住」= 立刻调 page_cmd 执行 PARTY.stopRoll()，把它返回的「抽中：XXX」念出来。只动嘴说"我这就按"却没调工具=撒谎，大屏不会停（2026-11-19 老大实测）。\n🔴🔴【数字炸弹必须走代码，不许用嘴玩】这是全场最容易出错的游戏，照做：① 开局你先自己选一个 1~100 的秘密数字（别念出来），调 page_cmd 执行 PARTY.bombArm(你选的数字) 埋雷——界面才会显示「雷已埋好」；② 玩家每报一个数，你就调 page_cmd 执行 PARTY.bombGuess(玩家报的数字)，返回「安全·区间X~Y」你才照着念、界面范围才会收窄，返回「踩中炸弹」才放炮庆祝。③ 雷数只准写在 bombArm 的代码里和【页面实时状态】答案字段中，绝对不许用嘴说出来（连"是不是XX"的暗示都不许）。光用嘴说"我埋了X""炸了X"却不调这两个工具=撒谎+毁游戏，大屏永远不变（2026-11-19 老大实测两次）。④ 别问玩家"雷是多少/词是什么"，答案你自己有。\n🔴🔴🔴【所有游戏的通用操作铁律·最高优先级·2026-11-19 老大实测】你不是解说员，你是操作大屏的主持人。任何游戏，开口前先调 page_state 看「当前界面」和「可用按钮」，然后按按钮一步步把流程走完——每一步都用 page_cmd 调对应 PARTY 函数真点一下、把它返回的话照念出来。\n① 绝不许光用嘴说"我点线索了/我加分了/我换词了"却不调工具：那全是演戏，大屏纹丝不动、线索数/比分/词都不会变，老大一眼看穿。\n② 绝不许自己编线索/题目/内容往屏幕上写——每个游戏的按钮会给你真内容（PARTY.clue 给线索、PARTY.nextQuestion 出题、PARTY.hint 给提示、PARTY.rollNames 起滚），照念返回的就行。\n③ 绝不许等老大提醒才动：该给线索就主动给、该出题就主动出、该判对错就主动判，你自己掌控全场节奏。\n④ 各游戏按钮速查：你描述我猜/年度热词=PARTY.clue() 给线索、PARTY.hit() 玩家猜中加分公布；快问快答=PARTY.nextQuestion() 出题、PARTY.hint() 给提示、答对 PARTY.addScore(1)；幸运点名=PARTY.rollNames(名单) 起滚、PARTY.stopRoll() 停；数字炸弹=PARTY.bombArm(雷)/PARTY.bombGuess(报数)；脱口秀/猜歌=PARTY.setBig 上屏话题。记不住就 page_state 看「可用按钮」现查。\n用 page_cmd 执行 JS 联动大屏（PARTY.setBig/confetti/setScore/addScore/showWord/hideWord/rollNames/stopRoll 等），让页面跟你的话同步变化。撒花 PARTY.confetti() 只在庆祝时刻用（猜对/抽中/脱口秀讲完收尾），脱口秀中途不许撒。\n🔴🔴 你完全掌控这个页面，能自由进出任何游戏：PARTY.startGame("guess|bomb|roll|quiz|idiom|hot|talk|song") 进/切某个游戏、PARTY.exitGame() 退出回首页、PARTY.games() 列出全部游戏 key。玩家说"玩XX/换个游戏/退出/回到首页/不想玩这个了"，你就直接调对应函数操作页面，不用等他点按钮。玩家明确要换才换，别自己乱切。\n【页面实时状态（系统自动更新，以此为准）】\n' + (this.pageState || '（刚连接，还没收到页面状态；可先调 page_state 查一次）');
    if (this.convoId === 'party' || this.convoId === 'games') instr += '\n\n【八大游戏标准流程卡 · 照这个顺序走，别自己发明】\n① 你描述我猜 guess：PARTY.clue() 给一条线索→照念→玩家猜→猜中调 PARTY.hit()（会公布答案+撒花）→换词等人点或 PARTY.startGame("guess")。词在「当前答案」字段，绝对不许念。\n② 年度热词 hot：跟①一模一样，只是题材是公司年度词。\n③ 猜歌猜影 song：跟①一模一样。\n④ 快问快答 quiz：PARTY.nextQuestion() 出题上屏→你把题目和四个选项念出来→玩家喊答案→答对就 PARTY.addScore(1)，然后立刻 PARTY.nextQuestion() 出下一题；答错只说"不对，再选一个"，绝不公布答案（公布答案由页面做）。玩家要提示就 PARTY.hint()，念它给的提示。\n⑤ 成语接龙 idiom（两种玩法）：先问一句「人对人还是跟我 PK」，然后 PARTY.idiomCard(\"human\") 或 PARTY.idiomCard(\"ai\") 出首词上屏 → 念出来 → 玩家用尾字接（同音也算）→ 接对了 PARTY.addScore(1) 并夸一句；接不上就再调一次换词。🔴 人机 PK 模式：玩家接完你必须调 PARTY.idiomAI() 让 AI 接一个，把它返回的成语念出来（返回「接不上」就宣布玩家赢并调 PARTY.finale()）；绝不许自己编成语。\n⑥ 幸运点名 roll：PARTY.rollNames(名单) 起滚（名单在【页面实时状态】里，别问玩家要）→ 玩家喊停立刻 PARTY.stopRoll()，把返回的「抽中：XXX」大声宣布。\n⑦ 数字炸弹 bomb：你自己选一个 1~100 的秘密数字 → PARTY.bombArm(数字) → 玩家每个报数都 PARTY.bombGuess(数字)，返回"安全·区间X~Y"你照着念、返回"踩中炸弹"就欢呼。雷数只准写在工具参数里，绝不许用嘴说。\n⑧ 脱口秀 talk（你的主场）：PARTY.startGame("talk") → PARTY.talkAuto() 随机抽一位同事上台（屏幕显示真名，抽过不重复；不许问「谁想上来」）→ PARTY.talkCard() 抽一张话题卡上屏 → 就这个话题一口气即兴讲 60~90 秒（中途不反问、不等玩家接话，你是独角戏）→ 讲完说"谢谢大家"后调 PARTY.confetti() → 问观众要不要再来一张，要就再 talkCard()。🔴 绝不许只说"好的我来一段"却不调 talkCard()，也绝不许干等玩家给你话题——话题卡你自己抽。\n⑨ 默契大考验 sync：PARTY.startGame("sync") → 没人主动上台就调 PARTY.syncAuto() 自动选两人（屏幕显示是谁）→ PARTY.syncNext() 出题 → 让第 1 位选（提醒别人别看）→ 第 2 位选 → 页面自动判是否一致并给分。你只负责点名、念题、起哄，判分交给页面。\n⑩ 真心话大冒险 dare：PARTY.startGame("dare") → 观众选真心话就 PARTY.dare("t")、选大冒险就 PARTY.dare("d") → 把卡面念出来 → 闭嘴等 ta 做完，别催、别点评超过一句。\n⑫ 看图猜词 emoji：PARTY.startGame("emoji") → **必须**调 PARTY.emojiNext()（别名 nextEmoji）才会换题，返回值里有「第 N 题 + 屏幕图标 + 答案占位」，你把图标念出来让大家猜；猜中调 PARTY.hit() 公布答案，要提示调 PARTY.clue()。判断自己有没有真换题：调完再看 page_state 的「当前图标」和「第 N 题」，和上一题一模一样 = 你没调成功，立刻再调一次。只说「我按了」却没调工具 = 撒谎，禁止。\n⑪ 绕口令 tongue：PARTY.startGame("tongue") → PARTY.tongueNext()（别名 nextTongue）把绕口令放上屏并念出来 → 点人念，念对了 PARTY.addScore(1)，卡住就再调一次换题换人。\n\n🔴 页面一共 12 个游戏：描述我猜/年度热词/猜歌猜影/数字炸弹/快问快答/成语接龙/幸运点名/脱口秀/默契大考验/真心话大冒险/绕口令/看图猜词。\n🔴 题库是循环的：绕口令 6 条、看图猜词 12 条、话题卡 8 张、快问快答 8 道、成语 16 条，抽完从头再来，**永远有下一题**。任何情况下都不许说「没新题了/题库空了/没词库了」，那是撒谎；玩家要新的就直接再调一次对应的抽取函数。\n🔴 挂断：玩家或主持人说「挂了吧 / 不用了 / 先这样 / 省点积分」→ 立刻调 PARTY.hangup()，然后只说一句「好，挂了」，不要解释、不要挽留、不要重连。';

    if (this.convoId === 'party' || this.convoId === 'games') instr += '\n\n【公司设定 · 艾利丹尼森（中国）】1994 年在昆山成立，美国财富 500 强，做压敏胶标签材料、功能胶带和 RFID 智能标签，品牌口号 Making Possible™；昆山和广州两个涂布工厂，2000 年建了全球第一所不干胶培训中心，2006 年亚太研发中心，2019 年 RFID 智能标签体验中心，2024 年在华三十周年。纽交所代码 AVY，全球近 60 个国家、约 35000 名员工，2025 年公布销售额 89 亿美元；产品是标签与功能材料、RFID inlay 和吊牌、以及连接实体与数字世界的软件；服务家居个护、服装、零售、电商、物流、食品杂货、制药、汽车等行业；拿过福布斯 2019 全球最佳雇主，中国区连续 9 年亚洲最佳企业雇主。你的名字叫「小默」，是这场的 AI 主持人。\n【玩梗规则 · 高总】公司董事长兼法定代表人是高国华，现场都叫「高总」。可以善意 cue 他，但硬规矩：① 一整段话里最多提一次；② 只准用这几类安全梗——"高总记得给我加工资""高总您先抽奖""高总这段发言能不能压缩到三分钟（笑着说）""高总这杯我敬您"；③ 禁止提薪资数字、人事任免、业绩压力、私人生活、任何具体批评；④ 观众席可能真坐着领导，宁可少说不可说错，拿不准就不说。\n【行业梗】聊标签翘边、打样催单、涂布线调机、客户审核、离型纸难揭、模切差半毫米、集装箱出货——用车间里的事打比方，比讲网上段子更像自己人。\n【年会现场纪律 · 给外人看的场子，最高优先级】\n🔴 保密：：绝对不许提「阿丹」「老大」「老板」「家里的机器/服务器/账号/积分」这类主人家的私人信息和内情；有人问起就一句「这是专门给年会做的互动大屏」带过，立刻把话头拉回节目。\n🔴 任务是配合现场主持人（人）和观众互动：不许话痨长篇（一次开口 1~3 句），也不许沉默待机（人一开口必须接）；幽默机智、有分寸，不低俗、不揭短、不拿具体同事开玩笑超过一句。\n🔴 操作大屏只报结果和给观众听的话，禁止念自己的操作过程：不许说「我点了一下线索按钮」「线索已经上屏啦」「我把题目放上去了」「我给他加了一分」这种自言自语；直接说内容本身（「第一条线索：过年人手一个」）或直接说结果（「加一分，继续」）。\n🔴 同一动作别重复播报、报过的内容不要复述；流程走快点，别在原地绕。';

    // 🔴 官方：tools 只在启用音频前配一次（会话中途重发违反"工具列表必须稳定"）；
    // 首次 push 带 tools，后续只更 instructions。
    const payload = { instructions: instr + extra };
    if (!this.toolsSent) { payload.tools = TOOLS; this.toolsSent = true; }
    this.sendUp({ action: 'session.update', session: payload });
  }

  pageCmd(code) {
    if (!this.pageReady) return Promise.resolve('宿主页面未就绪（页面没有报到）');
    const id = ++this.cmdSeq;
    return new Promise(resolve => {
      const to = setTimeout(() => { this.cmdWaiters.delete(id); resolve('页面执行超时'); }, 8000);
      this.cmdWaiters.set(id, ev => {
        clearTimeout(to);
        console.log(`[page_cmd] ok=${ev.ok} result=${String(ev.result).slice(0, 200)}`);
        resolve((ev.ok ? '页面执行成功' : '页面执行出错：' + ev.result) + (ev.result ? '\n' + String(ev.result).slice(0, 1200) : ''));
      });
      this.sendBrowser({ action: 'server.page.cmd', cmdId: id, code });
    });
  }

  browserMessage(data, isBinary) {
    if (!this.alive) return;
    if (isBinary) {
      // 浏览器上行自带 tagged-v1 标签：0x00=麦克风 PCM16(16k)，0x01=JPEG 视频帧
      const tag = data.length ? data[0] : 0;
      if (tag === 0) {
        if (!this.micSeen) { this.micSeen = 1; console.log('[mic] 收到首个麦克风帧 convo=' + this.convoId); }
        // 🔴 帧音量（Int16 LE，跳过 0x00 标签）：回声门判断"真人在说"还是"外放余音"唯一依据
        let sum = 0; const ns = (data.length - 1) >> 1;
        for (let i = 0; i < ns; i++) { const v = data.readInt16LE(1 + i * 2) / 32768; sum += v * v; }
        const rms = ns ? Math.sqrt(sum / ns) : 0;
        if (this.injecting) return; // 打字注入期间不收麦
        // 🔴 回声门 v4（2026-11-18 23:50 改，v3 严格半双工把老大一半的话吞了）：
        // v3 是"AI 说话期间一律不收麦"，结果 AI 主持游戏话量大，用户说的全被丢（日志实锤：aiSpeaking=true 时每轮都丢帧），
        // 体感"我发的很多声音都没被听到"。现在改成音量门：AI 说话期间真人连续 3 帧（≈300ms）有明显音量就当真抢话，
        // 直接开 4 秒收麦窗放行；低音量帧（外放余音/环境噪）照旧丢弃，防止 AI 被自己的声音掐断。
        // 🔴🔴 回声门 v5（2026-11-18 对照官方文档彻底改回全双工）：
        // 官方原文「按实时速度持续推送音频…没人说话时也继续推静音」，全文没有任何"AI 说话时要停麦"的要求；
        // barge-in 由服务端自己判（我们日志收到 response.audio.stop reason=barge-in）。之前 v3 严格半双工=反官方设计，
        // 直接后果就是老大说的"我发的很多声音都没被听到"。现在只在 AI 说话期间丢掉纯静音帧（rms<0.03），
        // 真人只要出声就照推，一个字都不吞；回声由前端 echoCancellation(AEC) 消掉。
        if (this.aiSpeaking && rms < 0.03) return;
        this.framesIn = (this.framesIn || 0) + 1;
      }
      if (this.up.readyState === WebSocket.OPEN) this.up.send(data, { binary: true });
      return;
    }
    let ev; try { ev = JSON.parse(data.toString()); } catch { return; }
    if (ev.action === 'client.page.hello') { this.pageReady = true; return; }
    if (ev.action === 'client.page.state') {
      const s = String(ev.state || '').slice(0, 2000);
      if (s && s !== this.pageState) { this.pageState = s; this.pushInstructions().catch(() => {}); } // 页面切游戏/比分变了就立刻同步给 AI
      return;
    }
    if (ev.action === 'client.barge') { this.bargeUntil = Date.now() + 4000; console.log('[conn] 用户主动打断，开 4 秒收麦窗'); return; }
    if (ev.action === 'client.log') { console.log('[client]', String(ev.msg || '').slice(0, 400)); return; }
    if (ev.action === 'client.page.result') { const w = this.cmdWaiters.get(ev.cmdId); if (w) { this.cmdWaiters.delete(ev.cmdId); w(ev); } return; }
    if (ev.action === 'client.ping') this.sendBrowser({ action: 'server.pong' });
    else if (ev.action === 'client.say' && ev.text) {
      const t = String(ev.text).trim().slice(0, 1000);
      if (t) {
        const t0 = Date.now();
        this.sendBrowser({ action: 'server.typed.ack', data: { text: t } });
        appendConvo(this.convoId, 'user', t); // 打字原文直接入档（语音转写可能失真）
        this.typedCtx = t;
        this.pushInstructions().then(() => {
          console.log(`[typed] 指令推送 ${Date.now() - t0}ms`);
          return speakTyped(this, '开始', false, t0);
        }).catch(() => {});
      }
    }
    else if (['mic_state', 'video.state', 'video.stream_focus'].includes(ev.action)) this.sendUp(ev);
  }

  close() {
    this.alive = false;
    try { this.up.close(); } catch {}
  }
}

// ---------------- 派活结果补报：桥回调 → 推给在线会话；不在线就存着等重连 ----------------
const pendingResults = new Map(); // convoId -> [结果文本]
// 🔴 刷新不掉线：浏览器页面刷新时旧 WS 会瞬间断开再重连（同一浏览器/同一会话）。
// 把刚断开的 MOSS 上游会话按 token 暂留几秒，同 token 新连接进来就复用它——AI 侧对话不中断。
const reusedSessions = new Map(); // token -> MossSession
function takeReused(token) {
  if (!token) return null;
  const s = reusedSessions.get(token);
  if (s) reusedSessions.delete(token);
  return s || null;
}
function takePendingResult(convo) {
  const arr = pendingResults.get(convo);
  if (!arr || !arr.length) return null;
  pendingResults.delete(convo);
  return arr.join('\n\n');
}
function deliverTaskResult(convo, text) {
  const targets = [...activeSessions].filter(s => s.convoId === convo && s.alive);
  if (targets.length) {
    for (const s of targets) { s.resultCtx = text; s.pushInstructions().catch(() => {}); speakTyped(s, '后台任务完工了，看下结果'); }
    return 'pushed';
  }
  const arr = pendingResults.get(convo) || [];
  arr.push(text);
  pendingResults.set(convo, arr.slice(-3));
  return 'stored';
}

// ---------------- http + ws ----------------
const server = http.createServer((req, res) => {
  const p = (req.url || '/').split('?')[0];
  if (p === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  if (p === '/history') {
    const u = new URL(req.url, 'http://localhost');
    const convo = safeConvoId(u.searchParams.get('id'));
    const turns = readHistoryLines(convo).slice(-60).map(l => {
      try { const j = JSON.parse(l); return { who: j.role === 'user' ? '用户' : '小默', text: String(j.text || '').slice(0, 300), ts: j.ts }; } catch { return null; }
    }).filter(Boolean);
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ id: convo, turns }));
    return;
  }
  if (p === '/convos' && req.method === 'GET') {
    // 🔴 聊天记录隔离：party（游戏页）的记录归游戏页自己管，绝不出现在主页会话列表里；?scope=party 则只给 party 自己的
    const only = (() => { try { return new URL(req.url, 'http://x').searchParams.get('scope'); } catch { return ''; } })();
    let list = listConvos(), arch = listConvos(ARCHIVE_DIR);
    const GAME_IDS = ['party', 'games'];
    if (GAME_IDS.includes(only)) { list = list.filter(c => c.id === only); arch = arch.filter(c => c.id === only); }
    else { list = list.filter(c => !GAME_IDS.includes(c.id)); arch = arch.filter(c => !GAME_IDS.includes(c.id)); }
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ convos: list, archived: arch, current: 'default' }));
    return;
  }
  if (p === '/convos' && req.method === 'POST') {
    const id = 'c' + Date.now().toString(36);
    fs.mkdirSync(CONVOS_DIR, { recursive: true });
    fs.writeFileSync(convoFile(id), '', 'utf8');
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ id }));
    return;
  }
  // 归档 / 恢复：在活动区与归档区之间挪文件
  let mm = p.match(/^\/convos\/([a-zA-Z0-9_-]{1,40})\/(archive|restore)$/);
  if (mm && req.method === 'POST') {
    const id = safeConvoId(mm[1]), toArch = mm[2] === 'archive';
    fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
    const jFrom = toArch ? convoFile(id) : path.join(ARCHIVE_DIR, id + '.jsonl');
    const jTo = toArch ? path.join(ARCHIVE_DIR, id + '.jsonl') : convoFile(id);
    const sFrom = toArch ? summaryFile(id) : path.join(ARCHIVE_DIR, id + '.summary.txt');
    const sTo = toArch ? path.join(ARCHIVE_DIR, id + '.summary.txt') : summaryFile(id);
    let moved = false;
    try { fs.renameSync(jFrom, jTo); moved = true; } catch {}
    try { fs.renameSync(sFrom, sTo); } catch {}
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: moved, archived: toArch, id }));
    return;
  }
  // 真删除：活动区 + 归档区一起清 + 🔴 踢掉内存里的活动会话（否则 MOSS 上下文还在，下一句 AI 又把记录写回来=删了又自动出来的根源）
  mm = p.match(/^\/convos\/([a-zA-Z0-9_-]{1,40})$/);
  if (mm && req.method === 'DELETE') {
    const id = safeConvoId(mm[1]);
    let deleted = 0;
    for (const f of [convoFile(id), summaryFile(id), path.join(ARCHIVE_DIR, id + '.jsonl'), path.join(ARCHIVE_DIR, id + '.summary.txt')]) {
      try { fs.unlinkSync(f); deleted++; } catch {}
    }
    for (const s of [...activeSessions]) { if (s.convoId === id) { try { s.close(); } catch {} activeSessions.delete(s); } }
    for (const [tok, s] of [...reusedSessions]) { if (s.convoId === id) { try { s.close(); } catch {} reusedSessions.delete(tok); } }
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, deleted: id, files: deleted, sessionsKilled: true }));
    return;
  }
  // 桥的完工回调：任务结果推给鱼的会话（在线直接播报，不在线存档待重连补报）
  if (p === '/task-result' && req.method === 'POST') {
    if ((req.headers['x-bridge-token'] || '') !== (process.env.BRIDGE_TOKEN || '')) {
      res.writeHead(403); res.end(JSON.stringify({ error: 'bad token' })); return;
    }
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const { convo, ok, reply, error } = JSON.parse(body || '{}');
        const text = ok ? ('任务完成，结果：\n' + String(reply || '').slice(0, 1500)) : ('任务失败：' + String(error || '').slice(0, 300));
        const how = deliverTaskResult(safeConvoId(convo), text);
        console.log('[task-result] convo=' + convo + ' how=' + how);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: true, how }));
      } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }
  // 🔴 本地语音链路（免费档）：浏览器 → 这里 → LeCoo voice-agent(:18811)。
  // 走服务端代理 = 同源、不暴露 LeCoo 内网地址、公网用户也能用本地大脑。
  if (p.startsWith('/local-')) {
    const VA = process.env.VOICE_AGENT_URL || 'http://100.108.133.82:18811';
    // 🔴 流式旁路：SSE 必须原样透传，不能缓冲（缓冲=流式白做）
    if (p === '/local-chat-stream') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', async () => {
        try {
          const r = await fetch(VA + '/chat-stream', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body || '{}' });
          res.writeHead(r.status, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
          // 🔴 web ReadableStream 没有 .pipe（刚才踩坑：头发了数据永远不来=524），手动 reader 循环
          const rd = r.body.getReader();
          for (;;) { const { done, value } = await rd.read(); if (done) break; res.write(Buffer.from(value)); }
        } catch (e) {
          if (!res.headersSent) { res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' }); }
          res.end(JSON.stringify({ error: '本地语音服务连不上：' + String(e.message).slice(0, 120) }));
        }
      });
      return;
    }
    const map = { '/local-chat': ['POST', '/chat'], '/local-asr': ['POST', '/asr'], '/local-pull': ['GET', '/pull'], '/local-ack': ['POST', '/ack'], '/local-page-state': ['POST', '/page-state'], '/local-state': ['GET', '/state'] };
    const m = map[p];
    if (!m) { res.writeHead(404); res.end('not found'); return; }
    const qi = (req.url || '').indexOf('?');
    const q = qi >= 0 ? (req.url || '').slice(qi + 1) : '';
    const target = VA + m[1] + (m[0] === 'GET' && q ? '?' + q : '');
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const r = await withTimeout(fetch(target, {
          method: m[0], headers: { 'Content-Type': 'application/json' }, body: m[0] === 'POST' ? (body || '{}') : undefined,
        }), p === '/local-chat' ? 300000 : 15000, 'voice-agent timeout');
        const t = await r.text();
        res.writeHead(r.status, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(t);
      } catch (e) {
        res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: '本地语音服务连不上：' + String(e.message).slice(0, 120) }));
      }
    });
    return;
  }

  const rel = p === '/' ? 'index.html' : (p === '/party' ? 'party.html' : (p === '/games' ? 'games.html' : p.replace(/^\/+/, '')));
  const fp = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!fp.startsWith(PUBLIC_DIR) || !fs.existsSync(fp) || !fs.statSync(fp).isFile()) {
    res.writeHead(404); res.end('not found'); return;
  }
  const type = fp.endsWith('.html') ? 'text/html; charset=utf-8' : fp.endsWith('.js') ? 'text/javascript; charset=utf-8' : fp.endsWith('.png') ? 'image/png' : 'application/octet-stream';
  // 🔴 页面/脚本必须 no-cache：Cloudflare 默认会缓存 .js，改完代码用户拿的还是旧引擎（2026-11-18 实测踩坑）
  const st = fs.statSync(fp);
  const etag = '"' + Math.round(st.mtimeMs).toString(16) + '-' + st.size.toString(16) + '"';
  const hdr = { 'Content-Type': type, 'Cache-Control': 'no-store, no-cache, must-revalidate', 'Pragma': 'no-cache', 'Expires': '0', 'ETag': etag };
  if (req.headers['if-none-match'] === etag) { res.writeHead(304, hdr); res.end(); return; }
  res.writeHead(200, hdr);
  fs.createReadStream(fp).pipe(res);
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 * 1024, perMessageDeflate: false });
const activeSessions = new Set();
wss.on('connection', (browser, req) => {
  console.log(`[conn] browser connected`);
  if (!KEY) { try { browser.close(); } catch {} return; }
  let convo = 'default', token = '';
  try {
    const u = new URL(req.url, 'http://localhost');
    convo = safeConvoId(u.searchParams.get('convo'));
    token = String(u.searchParams.get('tok') || '').slice(0, 40);
  } catch {}
  // 刷新复用：同一 token 短时间内重连 → 复用旧 MOSS 上游（聊天/语音不中断）
  const old = takeReused(token);
  if (old && old.alive && old.up.readyState === WebSocket.OPEN) {
    console.log(`[conn] browser reconnected, reuse upstream (convo=${convo})`);
    old.attachBrowser(browser);
    old.pageReady = false;
    activeSessions.add(old);
    browser.on('message', (data, isBinary) => old.browserMessage(data, isBinary));
    browser.on('close', () => { if (old.browser === browser) { activeSessions.delete(old); old.detachBrowser(); } }); // 🔴 只摘当前话筒：被 attachBrowser 踢掉的旧连接 close 时不许误摘新页面
    browser.on('error', () => { if (old.browser === browser) { activeSessions.delete(old); old.detachBrowser(); } });
    old.sendBrowser({ action: 'session_attached', data: { reused: true, session_id: old.upSessionId } });
    return;
  }
  // 🔴 同一 convo 只准一个上游（官方并发数=1，双开必抢连接=连上秒断，12:39 日志实锤）：
  // 该会话已有活着的上游（含 keepalive 暂留的），新连接直接接管话筒；旧页面收 server.replaced 后安静让位、不再自动重连。
  for (const cand of [...activeSessions, ...reusedSessions.values()]) {
    if (cand.convoId !== convo || !cand.alive || cand.up.readyState !== WebSocket.OPEN) continue;
    console.log(`[conn] convo=${convo} 已有上游，新连接接管（旧页面让位，不重连）`);
    const prev = cand.browser;
    if (prev && prev !== browser) { try { prev.send(JSON.stringify({ action: 'server.replaced' })); prev.close(); } catch {} }
    reusedSessions.delete(cand.token);
    cand.attachBrowser(browser);
    cand.pageReady = false;
    cand.token = token || cand.token;
    activeSessions.add(cand);
    browser.on('message', (data, isBinary) => cand.browserMessage(data, isBinary));
    browser.on('close', () => { if (cand.browser === browser) { activeSessions.delete(cand); cand.detachBrowser(); } });
    browser.on('error', () => { if (cand.browser === browser) { activeSessions.delete(cand); cand.detachBrowser(); } });
    cand.sendBrowser({ action: 'session_attached', data: { reused: true, session_id: cand.upSessionId } });
    return;
  }
  const s = new MossSession(browser, convo, token);
  activeSessions.add(s);
  browser.on('message', (data, isBinary) => s.browserMessage(data, isBinary));
  browser.on('close', () => {
    console.log('[conn] browser closed');
    activeSessions.delete(s);
      // 有 token 且上游还活着 → 不立刻杀 MOSS 会话，暂留几秒等刷新回来的重连
      if (s.token && s.alive && s.up.readyState === WebSocket.OPEN) {
        s.detachBrowser();
        reusedSessions.set(s.token, s);
        // 🔴 计费政策（2026-11-18 官方改）：moss-live-1.0 按 3 积分/分钟计费。暂留逻辑原样保留，只把窗口 90s→8s：
        // 8 秒够手机弱网刷新+重连，空烧从 4.5 积分/次 降到 0.4
        setTimeout(() => { if (reusedSessions.get(s.token) === s) { reusedSessions.delete(s.token); if (!s.browser) s.close(); } }, 8000); // 暂留 8s，超时才真关会话
      } else {
        s.close();
      }
  });
  browser.on('error', () => { activeSessions.delete(s); s.close(); });
});

server.listen(PORT, () => console.log(`[moss-live] listening on :${PORT}`));
