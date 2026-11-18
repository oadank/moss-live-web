// localvoice.js：本地免费语音链路前端引擎（moss-live 主页 + 年会页共用）· 流式版
// 链路：浏览器录音(webm) → 同源 /local-chat-stream(SSE) → voice-agent
//      asr→识别文字立刻上屏；AI 边想边 delta 打字机；每凑满一句 Hojo 合成 → sentence 事件 → 队列开播。
// 等待反馈：状态行「在听 / 在想 · 已等Xs / 在说」，用户始终知道现在在哪一步、等了多久。
window.LocalVoice = (function () {
  const S = { on: false, busy: false, pending: [], rec: null, chunks: [], mic: null, vactx: null, vadTimer: null, pullTimer: null, stateTimer: null, tickTimer: null, speaking: false, silenceMs: 0, sinceSend: 0, noMic: false, sendAt: 0, played: 0, queue: [], playing: false, mutedUntil: 0, lastShown: '' };
  const O = {}; // opts

  function b64Of(buf) {
    const u8 = new Uint8Array(buf); let s = ''; const CH = 0x8000;
    for (let i = 0; i < u8.length; i += CH) s += String.fromCharCode.apply(null, u8.subarray(i, i + CH));
    return btoa(s);
  }
  function sliceAudio() {
    return new Promise(res => {
      if (!S.rec || S.rec.state !== 'recording') return res(null);
      const old = S.rec, oldChunks = S.chunks;
      old.ondataavailable = e => { if (e.data.size) oldChunks.push(e.data); };
      const nr = new MediaRecorder(S.mic);
      S.chunks = [];
      nr.ondataavailable = e => { if (e.data.size) S.chunks.push(e.data); };
      let done = false;
      const finish = () => { if (done) return; done = true; S.rec = nr; try { nr.start(); } catch {} res(new Blob(oldChunks, { type: (oldChunks[0] && oldChunks[0].type) || 'audio/webm' })); };
      old.onstop = finish;
      try { old.stop(); } catch { finish(); }
      setTimeout(finish, 1500);
    });
  }

  // ===== 等待状态：已等 X 秒实时跳动 =====
  function stageText() {
    const waited = S.sendAt ? Math.max(0, Math.round((Date.now() - S.sendAt) / 1000)) : 0;
    if (!S.busy) return '本地模式 · 说完自动发送';
    if (S.playing || S.queue.length) return '🔊 正在说 · 已等 ' + waited + 's';
    if (S.gotDelta) return '💭 在想 · 已等 ' + waited + 's';
    if (S.asrDone) return '💭 在想 · 已等 ' + waited + 's';
    return '🎤 在听（识别中）· 已等 ' + waited + 's';
  }
  function refreshStage() { O.onStatus && O.onStatus(stageText()); }
  function startTick() { clearInterval(S.tickTimer); S.tickTimer = setInterval(() => { if (S.busy) refreshStage(); }, 1000); }

  // ===== 语音播放队列：sentence 事件入队，播完一句播下一句；播报期间本地丢回声 =====
  function playNext() {
    if (S.playing || !S.queue.length) return;
    const item = S.queue.shift();
    S.playing = true;
    S.mutedUntil = Date.now() + 800; // 外放回声窗：播报后 0.8s 内的声音不算用户插话
    const a = new Audio('data:audio/wav;base64,' + item.audio);
    S.media = a;
    a.onended = () => { S.playing = false; S.mutedUntil = Date.now() + 500; playNext(); };
    a.onerror = () => { S.playing = false; playNext(); };
    a.play().catch(() => { S.playing = false; });
    refreshStage();
  }
  let playUrlRef = null;

  // ===== 流式请求：SSE 事件 asr / delta / sentence / done / error =====
  async function stream(payload) {
    S.busy = true; S.sendAt = Date.now(); S.gotDelta = false; S.asrDone = false;
    S.queue = []; S.played = 0; S.lastShown = '';
    refreshStage(); startTick();
    let full = '';
    try {
      const r = await fetch('/local-chat-stream', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (!r.ok || !r.body) { const t = await r.text(); let m = {}; try { m = JSON.parse(t); } catch {} O.onTip && O.onTip('本地链路出错：' + (m.error || ('HTTP ' + r.status))); return; }
      const rd = r.body.getReader(); const td = new TextDecoder(); let buf = '';
      for (;;) {
        const { done, value } = await rd.read();
        if (done) break;
        buf += td.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const block = buf.slice(0, i); buf = buf.slice(i + 2);
          let name = 'message', data = null;
          for (const ln of block.split('\n')) {
            if (ln.startsWith('event:')) name = ln.slice(6).trim();
            else if (ln.startsWith('data:')) { try { data = JSON.parse(ln.slice(5)); } catch {} }
          }
          if (!data) continue;
          if (name === 'asr') { S.asrDone = true; if (data.text) O.onUser && O.onUser(data.text); refreshStage(); }
          else if (name === 'delta') {
            if (data.t) { S.gotDelta = true; full += data.t; if (O.onAiLive) O.onAiLive(full); }
            if (data.final) full = data.reply || full;
            refreshStage();
          }
          else if (name === 'sentence') { S.queue.push(data); playNext(); }
          else if (name === 'done') {
            full = data.reply || full;
            if (O.onAi) O.onAi(full);
            O.onTip && O.onTip('⏱ 听' + (data.ms?.asr || 0) + 'ms · 想' + (data.ms?.llm || 0) + 'ms（语音边想边播）');
          }
          else if (name === 'error') { O.onTip && O.onTip('本地链路出错：' + data.error); }
        }
      }
    } catch (e) { O.onTip && O.onTip('本地链路请求失败：' + e.message); }
    S.busy = false; S.sendAt = 0; clearInterval(S.tickTimer);
    refreshStage();
    if (S.on && S.pending.length) stream(S.pending.shift()); // 队列还有（它忙时又说/打了字）→ 立刻接发
    if (S.gotDelta || full) { /* 文本已由 onAiLive/onAi 上屏 */ }
  }

  function startVad() {
    S.vactx = new AudioContext();
    const src = S.vactx.createMediaStreamSource(S.mic);
    const an = S.vactx.createAnalyser(); an.fftSize = 512; src.connect(an);
    const buf = new Float32Array(an.fftSize);
    S.vadTimer = setInterval(async () => {
      if (!S.on) return;
      an.getFloatTimeDomainData(buf);
      let sum = 0; for (let i = 0; i < buf.length; i++) sum += buf[i] * buf[i];
      const rms = Math.sqrt(sum / buf.length);
      S.sinceSend += 200;
      // 播报期间的回声不触发判停
      if (S.playing || Date.now() < S.mutedUntil) { S.speaking = false; S.silenceMs = 0; return; }
      if (S.busy) return;
      if (rms > 0.02) { S.speaking = true; S.silenceMs = 0; }
      else if (S.speaking) S.silenceMs += 200;
      if ((S.speaking && S.silenceMs >= 1200) || (S.speaking && S.sinceSend >= 12000)) {
        S.speaking = false; S.silenceMs = 0; S.sinceSend = 0;
        const data = await sliceAudio();
        if (data && data.size > 20000) {
          const pl = { convo: O.convo, audioBase64: b64Of(await data.arrayBuffer()), audioMime: data.type || 'audio/webm', model: (O.getModel && O.getModel()) || 'hy4f' };
          // 🔴 busy（上一条在想/在说）时不丢麦克风：进队列，上一条一完立刻发（老大：它想的时候收不到我说话）
          if (S.busy) { S.pending.push(pl); O.onTip && O.onTip('听到了 · 排在上一条后面，它一忙完就问它'); }
          else stream(pl);
        }
      }
    }, 200);
  }

  async function start() {
    if (S.on) return;
    S.on = true;
    S.noMic = false;
    try {
      S.mic = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true } });
    } catch (e) {
      if (e && (e.name === 'NotFoundError' || e.name === 'OverconstrainedError' || e.name === 'NotReadableError')) { S.noMic = true; }
      else { S.on = false; throw e; }
    }
    if (!S.noMic) {
      S.rec = new MediaRecorder(S.mic);
      S.chunks = [];
      S.rec.ondataavailable = e => { if (e.data.size) S.chunks.push(e.data); };
      S.rec.start();
      startVad();
    }
    S.pullTimer = setInterval(async () => {
      if (!O.allowPageCmd) return;
      try {
        const r = await fetch('/local-pull?convo=' + O.convo);
        const j = await r.json();
        for (const job of (j.jobs || [])) {
          let ok = true, result = '';
          try { result = String(eval(job.code)); } catch (e) { ok = false; result = String(e.message || e).slice(0, 200); }
          fetch('/local-ack', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: job.id, ok, result }) }).catch(() => {});
        }
      } catch {}
    }, 1200);
    if (O.getPageState) S.stateTimer = setInterval(() => {
      try { const st = O.getPageState(); fetch('/local-page-state', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ convo: O.convo, state: typeof st === 'string' ? st : JSON.stringify(st) }) }).catch(() => {}); } catch {}
    }, 2000);
    O.onStatus && O.onStatus(S.noMic ? '本地模式 · 此设备没麦克风，用打字' : '本地模式 · 说完自动发送');
  }

  function stop() {
    S.on = false;
    clearInterval(S.vadTimer); clearInterval(S.pullTimer); clearInterval(S.stateTimer); clearInterval(S.tickTimer);
    try { S.rec && S.rec.state === 'recording' && S.rec.stop(); } catch {}
    try { S.media && S.media.pause(); } catch {}
    try { S.vactx && S.vactx.close(); } catch {}
    S.mic = null; S.queue = []; S.playing = false; S.busy = false; S.sendAt = 0;
  }

  function chatText(t) { // 打字：不投喂任何语音，直接进脑子（后端不再回显 text，前端不会显示两遍）
    O.onUser && O.onUser(t);
    const pl = { convo: O.convo, text: t, model: (O.getModel && O.getModel()) || 'hy4f' };
    if (S.busy) { S.pending.push(pl); O.onTip && O.onTip('已排队 · 上一条一忙完就问这个'); return Promise.resolve(); }
    return stream(pl);
  }

  return {
    init(opts) { Object.assign(O, opts); },
    get on() { return S.on; },
    start, stop, chatText,
    playUrl(url) { playUrlRef = url; const a = new Audio(url); S.media = a; a.play().catch(() => {}); return a; },
    pauseMedia() { try { S.media && S.media.pause(); } catch {} S.playing = false; S.queue = []; }, // 用户打断：停播+清队列
    get mic() { return S.mic; },
    get busy() { return S.busy; },
  };
})();
