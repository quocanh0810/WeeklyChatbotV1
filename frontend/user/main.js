// main.js — chống giật khi đang cuộn & bot gõ chữ

(() => {
  const chat  = document.getElementById("chat");
  const form  = document.getElementById("composer");
  const input = document.getElementById("prompt");

  // Neo scroll NẰM TRONG #chat
  let anchor = document.getElementById("scroll-anchor");
  if (!anchor) {
    anchor = document.createElement("div");
    anchor.id = "scroll-anchor";
    chat.appendChild(anchor);
  }

  // ---- Trạng thái auto-scroll ----
  let userScrolling = false;      // người dùng đang cuộn thủ công
  let autoScrollLocked = false;   // khóa auto-scroll cho tới khi quay về đáy
  let nearBottomThreshold = 80;   // px

  function isNearBottom(el, px = nearBottomThreshold) {
    return el.scrollTop + el.clientHeight >= el.scrollHeight - px;
  }
  function scrollToBottom(smooth = false) {
    anchor.scrollIntoView({ block: "end", behavior: smooth ? "smooth" : "auto" });
  }

  // ---- Nút “Xuống cuối” (hiện khi auto-scroll bị khóa) ----
  const jumpBtn = document.createElement("button");
  jumpBtn.textContent = "↓ Xuống cuối";
  Object.assign(jumpBtn.style, {
    position: "fixed",
    right: "16px",
    bottom: "84px",        // nằm trên thanh composer fixed
    zIndex: 60,
    padding: "10px 12px",
    borderRadius: "12px",
    border: "1px solid #1f2937",
    background: "rgba(15,23,42,.95)",
    color: "#e5e7eb",
    cursor: "pointer",
    display: "none",
  });
  document.body.appendChild(jumpBtn);

  function showJump(show) {
    jumpBtn.style.display = show ? "block" : "none";
  }
  jumpBtn.addEventListener("click", () => {
    autoScrollLocked = false;
    userScrolling = false;
    scrollToBottom(true);
    input.focus({ preventScroll: true });
    showJump(false);
  });

  // ---- Lắng nghe cuộn để khóa/mở auto-scroll ----
  let scrollDebounce;
  chat.addEventListener("scroll", () => {
    // người dùng vừa cuộn
    userScrolling = true;
    clearTimeout(scrollDebounce);
    // nếu đang không gần đáy → khóa auto-scroll và hiện nút
    if (!isNearBottom(chat)) {
      autoScrollLocked = true;
      showJump(true);
    } else {
      // quay lại đáy → mở khóa sau một nhịp nhỏ để tránh giật
      scrollDebounce = setTimeout(() => {
        userScrolling = false;
        autoScrollLocked = false;
        showJump(false);
      }, 120);
    }
  }, { passive: true });

  // ---- UI helpers ----
  function addBubble(html, who = "bot") {
    const wrap = document.createElement("div");
    wrap.className = `bubble ${who}`;
    wrap.innerHTML = html;
    chat.insertBefore(wrap, anchor);

    // chỉ kéo xuống nếu không khóa và đang ở gần đáy
    if (!autoScrollLocked && isNearBottom(chat)) {
      scrollToBottom(false);
    }
    return wrap;
  }

  function md(text) {
    try { return window.marked.parse(text); }
    catch { return `<p>${escapeHtml(text)}</p>`; }
  }

  // Gõ kiểu máy, chỉ auto-scroll khi hợp lệ
  function typeWriter(el, fullText, msPerTick = 12) {
    let i = 0;
    const len  = fullText.length;
    const step = Math.max(1, Math.floor(len / 800));

    (function tick() {
      i = Math.min(len, i + step);
      el.innerHTML = md(fullText.slice(0, i));

      // Kéo xuống chỉ khi:
      // - không bị khóa
      // - và người dùng đang ở gần đáy (trước tick này)
      if (!autoScrollLocked && isNearBottom(chat)) {
        scrollToBottom(false);   // dùng 'auto' để không đấu với wheel smooth
      }

      if (i < len) {
        setTimeout(tick, msPerTick);
      } else {
        // kết thúc, nếu không khóa thì bám đáy mượt
        if (!autoScrollLocked) scrollToBottom(true);
        input.focus({ preventScroll: true });
      }
    })();
  }

  // ---- Chào mừng ----
  function showWelcome() {
    const hello = [
      "Xin chào! Mình là **Chat Bot lịch tuần Đại học Thương Mại** 👋",
      "",
      "Bạn có thể hỏi về hoạt động/họp/sự kiện theo *ngày, thứ, giờ*.",
      "- VD: *\"Lịch tuần trường Đại học Thương Mại là gì?\"*",
      "- VD: *\"Thứ 5 tuần này lúc 9h30 có gì?\"*",
    ].join("\n");
    typeWriter(addBubble("", "bot"), hello);
  }

  // ---- Gọi backend ----
  async function askBackend(message) {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}${text ? ` - ${text}` : ""}`);
    }
    return res.json();
  }

  // ---- Form submit ----
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const msg = (input.value || "").trim();
    if (!msg) return;

    addBubble(`<p>${escapeHtml(msg)}</p>`, "user");
    input.value = "";
    input.focus({ preventScroll: true });

    const placeholder = addBubble(`<p>Đang soạn trả lời<span class="dots">...</span></p>`, "bot");
    if (!autoScrollLocked) scrollToBottom(true);

    let dotsOn = true;
    const dotsTimer = setInterval(() => {
      const el = placeholder.querySelector(".dots");
      if (el) el.textContent = dotsOn ? "…" : "....";
      dotsOn = !dotsOn;
    }, 400);

    try {
      const data = await askBackend(msg);
      clearInterval(dotsTimer);
      // trước khi type: nếu người dùng đang xem lịch sử, không kéo
      typeWriter(placeholder, data?.answer || "Xin lỗi, mình chưa có câu trả lời phù hợp.");
    } catch (err) {
      clearInterval(dotsTimer);
      placeholder.innerHTML = `<p class="error">⚠️ Lỗi: ${escapeHtml(String(err))}</p>`;
      if (!autoScrollLocked) scrollToBottom(true);
    }
  });

  // ---- UX nhỏ ----
  window.addEventListener("keydown", (e) => {
    if (e.key === "/" && document.activeElement !== input) {
      e.preventDefault();
      input.focus({ preventScroll: true });
    }
  });

  window.addEventListener("resize", () => {
    // nếu đang bám đáy và không khóa, tiếp tục bám đáy
    if (!autoScrollLocked && isNearBottom(chat)) scrollToBottom(false);
  });

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, (ch) => (
      { "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[ch]
    ));
  }

  document.addEventListener("DOMContentLoaded", () => {
    chat.appendChild(anchor); // đảm bảo neo ở cuối
    showWelcome();
    scrollToBottom(true);
  });
})();