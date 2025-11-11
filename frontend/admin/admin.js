// frontend/admin/admin.js — DEBUG READY
const $ = (s, r=document) => r.querySelector(s);
const API_BASE  = "/api/admin";
const TOKEN_KEY = "tmu_admin_token";
const DEBUG_ON  = true;                // bật/tắt overlay debug

let authToken   = null;
let currentPage = 1;

// ---------- debug helpers ----------
const dbgEl = $("#debugPanel");
function dbg(...args){
  // console
  console.log("[ADMIN]", ...args);
  // overlay
  if (!DEBUG_ON || !dbgEl) return;
  const line = document.createElement("div");
  line.textContent = args.map(a => {
    try { return typeof a === "string" ? a : JSON.stringify(a); }
    catch { return String(a); }
  }).join(" ");
  dbgEl.appendChild(line);
  dbgEl.scrollTop = dbgEl.scrollHeight;
  dbgEl.classList.remove("hidden");
}
function dbgHtml(html, cls){
  if (!DEBUG_ON || !dbgEl) return;
  const el = document.createElement("div");
  if (cls) el.className = cls;
  el.innerHTML = html;
  dbgEl.appendChild(el);
  dbgEl.scrollTop = dbgEl.scrollHeight;
  dbgEl.classList.remove("hidden");
}

// Global error taps
window.addEventListener("error", (e) => {
  dbgHtml(`<b class="err">window.error:</b> ${e.message}`, "err");
});
window.addEventListener("unhandledrejection", (e) => {
  dbgHtml(`<b class="err">unhandledrejection:</b> ${e.reason}`, "err");
});

// ---------- utils ----------
function setLoading(btn, on=true){
  if (!btn) return;
  btn.classList.toggle("loading", on);
  btn.toggleAttribute("disabled", on);
}

async function jsonOrThrow(res){
  let data; 
  try { data = await res.json(); } catch { data = null; }
  if (!res.ok) {
    const msg = (data && (data.detail || data.error)) || `${res.status} ${res.statusText}`;
    dbgHtml(`<b class="err">HTTP ${res.status}:</b> ${escapeHtml(msg)}`, "err");
    throw new Error(msg);
  }
  return data;
}

function escapeHtml(s){
  return String(s ?? "").replace(/[&<>"']/g, c => (
    { "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c]
  ));
}

// base64url helpers
function b64urlDecode(str){
  if (!str) return "";
  const padLen = (4 - (str.length % 4)) % 4;
  const s = str.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat(padLen);
  try { return atob(s); } catch { return ""; }
}
function parseUserFromToken(token){
  try{
    const [payload] = token.split(".");
    const decoded = b64urlDecode(payload);
    const [user] = decoded.split("|");
    return user || null;
  }catch{ return null; }
}
function getTokenExp(token){
  try{
    const [payload] = token.split(".");
    const decoded = b64urlDecode(payload);
    const parts = decoded.split("|");
    const exp = parseInt(parts[1], 10);
    return Number.isFinite(exp) ? exp : null;
  }catch{ return null; }
}
function isTokenExpired(token){
  const exp = getTokenExp(token);
  if (!exp) return true;
  const now = Math.floor(Date.now()/1000);
  return now >= exp;
}
function saveToken(t){ try { localStorage.setItem(TOKEN_KEY, t); } catch {} }
function loadToken(){ try { return localStorage.getItem(TOKEN_KEY) || null; } catch { return null; } }
function clearToken(){ try { localStorage.removeItem(TOKEN_KEY); } catch {} }

// ---------- elements ----------
const loginCard  = $("#login");
const appMain    = $("#app");
const fLogin     = $("#fLogin");
const btnLogin   = $("#btnLogin");
const loginMsg   = $("#loginMsg");

const fPreview   = $("#fPreview");
const previewPre = $("#preview");
const fIngest    = $("#fIngest");

const tbl        = $("#tbl");
const btnRefresh = $("#btnRefresh");

const pgPrev     = $("#pgPrev");
const pgNext     = $("#pgNext");
const pgInfo     = $("#pgInfo");
const pgSize     = $("#pgSize");

const btnLogout  = $("#btnLogout");
const whoSpan    = $("#who");

// sanity check elements
(function checkEls(){
  const must = [
    ["#login", loginCard],
    ["#app", appMain],
    ["#fLogin", fLogin],
    ["#btnLogin", btnLogin],
    ["#btnLogout", btnLogout],
    ["#tbl", tbl],
  ];
  must.forEach(([sel, el]) => {
    if (!el) dbgHtml(`<b class="err">MISSING:</b> ${sel} not found in DOM`, "err");
  });
})();

// ---------- UI state ----------
function setLoggedIn(on){
  loginCard?.classList.toggle("hidden", on);
  appMain?.classList.toggle("hidden", !on);
  btnLogout?.classList.toggle("hidden", !on);
  whoSpan?.classList.toggle("hidden", !on);
  if (on) {
    const user = parseUserFromToken(authToken) || "admin";
    if (whoSpan) whoSpan.textContent = `Xin chào, ${user}`;
  } else {
    if (whoSpan) whoSpan.textContent = "";
  }
}

// ---------- boot: restore session ----------
document.addEventListener("DOMContentLoaded", async () => {
  dbgHtml(`<b class="ok">BOOT</b> DOMContentLoaded`, "ok");
  const stored = loadToken();
  dbg("stored token:", stored ? stored.slice(0, 16) + "..." : null);

  if (stored && !isTokenExpired(stored)) {
    authToken = stored;
    setLoggedIn(true);
    try {
      currentPage = 1;
      await loadUploads();
    } catch (e) {
      dbgHtml(`<b class="err">Restore session failed:</b> ${escapeHtml(String(e))}`, "err");
      doLogout(false);
    }
  } else {
    if (stored && isTokenExpired(stored)) dbgHtml(`<span class="muted">Token expired → logout</span>`);
    doLogout(false);
  }
});

// ---------- login ----------
fLogin?.addEventListener("submit", async (e) => {
  e.preventDefault();
  loginMsg.textContent = "";
  setLoading(btnLogin, true);
  try{
    const fd  = new FormData(fLogin);
    dbg("POST /login …");

    const res = await fetch(`${API_BASE}/login`, { method: "POST", body: fd });
    const data= await jsonOrThrow(res);

    dbgHtml(`<span class="ok">/login OK</span> token=${escapeHtml(data.token.slice(0, 16))}…`, "ok");

    authToken = data.token;
    saveToken(authToken);

    setLoggedIn(true);
    currentPage = 1;
    await loadUploads();
    appMain?.scrollIntoView({ behavior: "smooth", block: "start" });
  }catch(err){
    const msg = String(err.message || err);
    loginMsg.textContent = msg;
    loginMsg.classList.add("error");
    dbgHtml(`<b class="err">/login ERROR:</b> ${escapeHtml(msg)}`, "err");
    loginCard?.classList.remove("shake");
    void loginCard?.offsetWidth;
    loginCard?.classList.add("shake");
  }finally{
    setLoading(btnLogin, false);
  }
});

// ---------- logout ----------
function doLogout(alertUser=true){
  dbg("doLogout()");
  authToken = null;
  clearToken();

  // dọn UI
  if (previewPre) previewPre.textContent = "";
  fIngest?.classList.add("hidden");
  if (tbl) tbl.innerHTML = "";
  if (pgInfo) pgInfo.textContent = "—";

  setLoggedIn(false);
  if (alertUser) alert("Đã đăng xuất.");
}

btnLogout?.addEventListener("click", (e) => {
  dbg("btnLogout clicked");
  e.preventDefault();   // chặn bất kỳ submit nào
  e.stopPropagation();  // chặn bubble
  doLogout(true);
});

// ---------- preview .docx ----------
fPreview?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const btn = fPreview.querySelector("button");
  setLoading(btn, true);
  previewPre && (previewPre.textContent = "Đang parse...");
  try{
    if (!authToken || isTokenExpired(authToken)) { dbg("preview: token invalid"); return doLogout(true); }
    const fd  = new FormData(fPreview);

    dbg("POST /upload/preview …");
    const res = await fetch(`${API_BASE}/upload/preview`, {
      method: "POST",
      headers: { "Authorization": `Bearer ${authToken}` },
      body: fd
    });
    const data = await jsonOrThrow(res);

    dbgHtml(`<span class="ok">/upload/preview OK</span> count=${data.count}`, "ok");

    if (previewPre) {
      previewPre.textContent = JSON.stringify(
        { file: data.file, count: data.count, events: data.events },
        null, 2
      );
    }
    const tp = fIngest?.querySelector("[name='temp_path']");
    if (tp) tp.value = data.temp_path;
    fIngest?.classList.remove("hidden");
  }catch(err){
    const msg = String(err.message || err);
    if (previewPre) previewPre.textContent = "Preview error: " + msg;
    dbgHtml(`<b class="err">/upload/preview ERROR:</b> ${escapeHtml(msg)}`, "err");
  }finally{
    setLoading(btn, false);
  }
});

// ---------- ingest ----------
fIngest?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const btn = fIngest.querySelector("button");
  setLoading(btn, true);
  try{
    if (!authToken || isTokenExpired(authToken)) { dbg("ingest: token invalid"); return doLogout(true); }
    const fd  = new FormData(fIngest);

    dbg("POST /ingest …");
    const res = await fetch(`${API_BASE}/ingest`, {
      method: "POST",
      headers: { "Authorization": `Bearer ${authToken}` },
      body: fd
    });
    const data = await jsonOrThrow(res);
    dbgHtml(`<span class="ok">/ingest OK</span> task_id=${data.task_id}`, "ok");

    alert("Đã đưa vào hàng đợi (task " + data.task_id + ").");
    currentPage = 1;
    await loadUploads();
  }catch(err){
    const msg = String(err.message || err);
    dbgHtml(`<b class="err">/ingest ERROR:</b> ${escapeHtml(msg)}`, "err");
    alert("Ingest error: " + msg);
  }finally{
    setLoading(btn, false);
  }
});

// ---------- uploads + pagination ----------
btnRefresh?.addEventListener("click", async () => {
  dbg("btnRefresh clicked");
  currentPage = 1;
  await loadUploads();
});
pgPrev?.addEventListener("click", async () => {
  if (currentPage > 1) {
    currentPage--;
    dbg("pgPrev → page", currentPage);
    await loadUploads();
  }
});
pgNext?.addEventListener("click", async () => {
  currentPage++;
  dbg("pgNext → page", currentPage);
  await loadUploads();
});
pgSize?.addEventListener("change", async () => {
  currentPage = 1;
  dbg("pgSize change → size", pgSize.value);
  await loadUploads();
});

async function loadUploads(){
  if (!authToken || isTokenExpired(authToken)) { dbg("loadUploads: token invalid"); return doLogout(true); }

  const size = parseInt(pgSize?.value || "8", 10);
  const url  = new URL(`${API_BASE}/uploads`, window.location.origin);
  url.searchParams.set("page", String(currentPage));
  url.searchParams.set("page_size", String(size));

  try{
    dbg("GET /uploads", url.toString());
    const res  = await fetch(url.toString(), {
      headers: { "Authorization": `Bearer ${authToken}` }
    });
    const data = await jsonOrThrow(res);

    // ---- Phòng thủ: nếu backend chưa có meta, tự tạo meta tối thiểu ----
    const items = Array.isArray(data.items) ? data.items : (Array.isArray(data) ? data : []);
    const page        = Number.isFinite(+data.page)        ? +data.page        : 1;
    const totalPages  = Number.isFinite(+data.total_pages) ? +data.total_pages : 1;
    const total       = Number.isFinite(+data.total)       ? +data.total       : items.length;
    const hasPrev     = typeof data.has_prev === "boolean" ? data.has_prev     : false;
    const hasNext     = typeof data.has_next === "boolean" ? data.has_next     : false;

    dbg("uploads meta (normalized):", { page, totalPages, total, hasPrev, hasNext });

    // Nếu gọi vượt trang cuối -> kẹp lại và gọi lại
    if (page > totalPages && totalPages > 0) {
      currentPage = totalPages;
      dbg("clamp page →", currentPage);
      return loadUploads();
    }

    renderUploadsTable(items);

    if (pgInfo) pgInfo.textContent = `Trang ${page}/${Math.max(totalPages, 1)} — ${total} bản ghi`;
    if (pgPrev) pgPrev.disabled = !(page > 1 || hasPrev);
    if (pgNext) pgNext.disabled = !(page < totalPages || hasNext);
  }catch(err){
    const msg = String(err.message || err);
    if (tbl) tbl.innerHTML = `<tr><td>Lỗi tải uploads: ${escapeHtml(msg)}</td></tr>`;
    if (pgInfo) pgInfo.textContent = "—";
    if (pgPrev) pgPrev.disabled = true;
    if (pgNext) pgNext.disabled = true;

    dbgHtml(`<b class="err">/uploads ERROR:</b> ${escapeHtml(msg)}`, "err");
    if (msg.includes("401") || msg.includes("403")) doLogout(true);
  }
}

function renderUploadsTable(items){
  if (!tbl) return;
  if (!items || items.length === 0) {
    tbl.innerHTML = `
      <tr>
        <th>ID</th><th>File</th><th>Tag</th><th>Mode</th>
        <th>Added</th><th>Total after</th><th>Status</th><th>Created</th>
      </tr>
      <tr><td colspan="8" style="color:#9fb0c7">Chưa có bản ghi.</td></tr>`;
    return;
  }
  const thead = `
    <tr>
      <th>ID</th>
      <th>File</th>
      <th>Tag</th>
      <th>Mode</th>
      <th>Added</th>
      <th>Total after</th>
      <th>Status</th>
      <th>Created</th>
    </tr>`;
  const rows = items.map(it => `
    <tr>
      <td>${it.id ?? ""}</td>
      <td>${escapeHtml(it.filename ?? "")}</td>
      <td>${escapeHtml(it.tag ?? "")}</td>
      <td>${escapeHtml(it.mode ?? "")}</td>
      <td>${it.added_events ?? ""}</td>
      <td>${it.total_events ?? ""}</td>
      <td>${escapeHtml(it.status ?? "")}</td>
      <td>${escapeHtml(it.created_at ?? "")}</td>
    </tr>
  `).join("");
  tbl.innerHTML = thead + rows;
}

