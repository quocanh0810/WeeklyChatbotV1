// Minimal helpers
const $  = (s, r=document) => r.querySelector(s);
const $$ = (s, r=document) => [...r.querySelectorAll(s)];

let token = null;

const loginSec = $("#login");
const appSec   = $("#app");
const fLogin   = $("#fLogin");
const btnLogin = $("#btnLogin");
const loginMsg = $("#loginMsg");

const fPreview = $("#fPreview");
const fIngest  = $("#fIngest");
const preview  = $("#preview");
const tbl      = $("#tbl");
const btnRefresh = $("#btnRefresh");

// Fancy button loading toggler
function setLoading(btn, on=true){
  btn.classList.toggle("loading", on);
  btn.toggleAttribute("disabled", on);
}

async function jsonOrThrow(res){
  let data; try { data = await res.json(); } catch{ /* ignore */ }
  if (!res.ok) {
    const msg = (data && (data.detail || data.error)) || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return data;
}

// --- Login
fLogin.addEventListener("submit", async (e) => {
  e.preventDefault();
  loginMsg.textContent = "";
  setLoading(btnLogin, true);
  try{
    const fd = new FormData(fLogin);
    const res = await fetch("/api/admin/login", { method: "POST", body: fd });
    const data = await jsonOrThrow(res);

    token = data.token;
    document.body.classList.add("logged-in");

    // micro animation: lift the app in
    appSec.scrollIntoView({ behavior: "smooth", block: "start" });
  }catch(err){
    loginMsg.textContent = String(err.message || err);
    loginMsg.classList.add("error");
    // Shake card
    loginSec.classList.remove("shake");
    void loginSec.offsetWidth; // reflow
    loginSec.classList.add("shake");
  }finally{
    setLoading(btnLogin, false);
  }
});

// --- Preview upload
fPreview.addEventListener("submit", async (e) => {
  e.preventDefault();
  const btn = fPreview.querySelector("button");
  setLoading(btn, true);
  try{
    const fd = new FormData(fPreview);
    const res = await fetch("/api/admin/upload/preview", {
      method: "POST",
      headers: { Authorization: "Bearer " + token },
      body: fd
    });
    const data = await jsonOrThrow(res);

    preview.textContent = JSON.stringify({ file: data.file, count: data.count, events: data.events }, null, 2);
    fIngest.querySelector("[name='temp_path']").value = data.temp_path;
    fIngest.classList.remove("hidden");
    fIngest.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }catch(err){
    preview.textContent = "Preview error: " + String(err.message || err);
  }finally{
    setLoading(btn, false);
  }
});

// --- Ingest
fIngest.addEventListener("submit", async (e) => {
  e.preventDefault();
  const btn = fIngest.querySelector("button");
  setLoading(btn, true);
  try{
    const fd = new FormData(fIngest);
    const res = await fetch("/api/admin/ingest", {
      method: "POST",
      headers: { Authorization: "Bearer " + token },
      body: fd
    });
    const data = await jsonOrThrow(res);
    alert("Queued task " + data.task_id);
    await loadHistory();
  }catch(err){
    alert("Ingest error: " + String(err.message || err));
  }finally{
    setLoading(btn, false);
  }
});

// --- History
async function loadHistory(){
  const res = await fetch("/api/admin/uploads", { headers: { Authorization: "Bearer " + token }});
  const data = await jsonOrThrow(res);
  const rows = data.items || [];
  if (!rows.length){
    tbl.innerHTML = `<tr><th>ID</th><th>File</th><th>Mode</th><th>Status</th><th>Added</th><th>Total</th><th>Updated</th></tr>
                     <tr><td colspan="7" style="color:#9fb0c7">Chưa có bản ghi.</td></tr>`;
    return;
  }
  tbl.innerHTML = `
    <tr>
      <th>ID</th><th>File</th><th>Mode</th><th>Status</th>
      <th>Added</th><th>Total</th><th>Updated</th>
    </tr>
    ${rows.map(r=>`
      <tr>
        <td>${r.id}</td>
        <td>${escapeHtml(r.filename||"")}</td>
        <td>${escapeHtml(r.mode||"")}</td>
        <td>${escapeHtml(r.status||"")}</td>
        <td>${r.added_events ?? ""}</td>
        <td>${r.total_events ?? ""}</td>
        <td>${escapeHtml(r.updated_at||"")}</td>
      </tr>`).join("")}`;
}
btnRefresh?.addEventListener("click", loadHistory);

// Helpers
function escapeHtml(s){ return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])) }