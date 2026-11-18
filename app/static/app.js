"use strict";

const $ = (id) => document.getElementById(id);

async function call(method, url, payload) {
  const options = { method, headers: {} };
  if (payload !== undefined) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(payload);
  }
  const response = await fetch(url, options);
  let data = null;
  try {
    data = await response.json();
  } catch (_) {
    /* empty body */
  }
  if (!response.ok) {
    const detail = data && data.detail ? data.detail : `HTTP ${response.status}`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data;
}

function fmtTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

const STATUS_TEXT = {
  active: "已启用",
  building: "重建中",
  failed: "失败",
  retired: "已退役",
};

function renderStatus(status) {
  const box = $("status-box");
  const details = $("index-details");
  box.textContent = "";
  details.textContent = "";

  const active = status.active;
  if (!active) {
    box.innerHTML = '<span class="dot err"></span>没有可用索引';
  } else if (status.building) {
    const pct = status.progress ? status.progress.percent : 0;
    box.innerHTML =
      `<span class="dot warn"></span>索引重建中 ${pct}% · 检索仍使用旧索引 #${active.id}`;
  } else {
    box.innerHTML = `<span class="dot ok"></span>索引就绪 #${active.id}`;
  }
  if (status.open_alerts > 0) {
    const warn = document.createElement("span");
    warn.className = "open-alert-flag";
    warn.textContent = `未确认告警 ${status.open_alerts} 条`;
    box.appendChild(warn);
  }
  const badge = $("open-alert-badge");
  badge.hidden = !status.open_alerts;
  badge.textContent = String(status.open_alerts || 0);

  const add = (label, value) => {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = value;
    details.append(dt, dd);
  };

  add("日志总数", String(status.total_logs));
  if (active) {
    add("当前索引", `#${active.id}（${STATUS_TEXT[active.status] || active.status}）`);
    add("已收录", `${active.doc_count} 条`);
    add("启用时间", fmtTime(active.activated_at));
  }
  if (status.building) {
    const b = status.building;
    add("正在重建", `#${b.id}，已扫描 ${b.build_cursor} / ${b.start_id}`);
    const bar = document.createElement("progress");
    bar.max = 100;
    bar.value = status.progress ? status.progress.percent : 0;
    const dt = document.createElement("dt");
    dt.textContent = "进度";
    const dd = document.createElement("dd");
    dd.appendChild(bar);
    details.append(dt, dd);
  }
  const latest = status.latest;
  if (latest && latest.status === "failed") {
    add("最近一次重建", `失败：${latest.error || "未知错误"}`);
  }

  $("rebuild-button").disabled = Boolean(status.building);
}

function renderResults(result, q, service) {
  const list = $("results");
  const meta = $("search-meta");
  list.textContent = "";
  const filters = [];
  if (service) filters.push(`服务 ${service}`);
  if (q) filters.push(`关键词 ${q}`);
  meta.textContent =
    `命中 ${result.total} 条` +
    (filters.length ? `（${filters.join("，")}）` : "") +
    ` · 索引 #${result.generation.id}`;

  for (const item of result.items) {
    const li = document.createElement("li");
    const metaRow = document.createElement("div");
    metaRow.className = "meta";
    const svc = document.createElement("span");
    svc.className = "service";
    svc.textContent = item.service;
    const time = document.createElement("span");
    time.textContent = fmtTime(item.created_at);
    const id = document.createElement("span");
    id.textContent = `#${item.id}`;
    metaRow.append(svc, time, id);
    const pre = document.createElement("pre");
    pre.textContent = item.body;
    li.append(metaRow, pre);
    list.appendChild(li);
  }
  if (result.items.length === 0) {
    const li = document.createElement("li");
    li.className = "hint";
    li.textContent = "没有匹配的日志。";
    list.appendChild(li);
  }
}

async function loadServices() {
  try {
    const data = await call("GET", "/api/services");
    const list = $("service-list");
    list.textContent = "";
    for (const item of data.items) {
      const option = document.createElement("option");
      option.value = item.service;
      option.label = `${item.count} 条`;
      list.appendChild(option);
    }
  } catch (_) {
    /* service list is a convenience only */
  }
}

async function refreshStatus() {
  try {
    renderStatus(await call("GET", "/api/status"));
  } catch (error) {
    $("status-box").innerHTML =
      `<span class="dot err"></span>无法获取索引状态：${error.message}`;
  }
}

// ------------------------------------------------------------------ alerts
function renderRules(rules) {
  const list = $("rules");
  list.textContent = "";
  if (rules.length === 0) {
    const li = document.createElement("li");
    li.className = "hint";
    li.textContent = "尚未登记告警规则。";
    list.appendChild(li);
    return;
  }
  for (const rule of rules) {
    const li = document.createElement("li");
    const meta = document.createElement("div");
    meta.className = "meta";
    const svc = document.createElement("span");
    svc.className = "service";
    svc.textContent = rule.service;
    const desc = document.createElement("span");
    desc.textContent =
      `关键词「${rule.keyword}」 · ${rule.window_seconds} 秒内命中 ` +
      `${rule.threshold} 条开单 · 登记于 ${fmtTime(rule.created_at)}`;
    const del = document.createElement("button");
    del.className = "small ghost";
    del.textContent = "删除";
    del.addEventListener("click", async () => {
      if (!window.confirm(`删除服务 ${rule.service} 的告警规则？已开的告警会保留。`)) {
        return;
      }
      try {
        await call("DELETE", `/api/rules/${rule.id}`);
        $("rule-hint").textContent = `已删除服务 ${rule.service} 的规则。`;
        void loadRules();
      } catch (error) {
        $("rule-hint").textContent = `删除失败：${error.message}`;
      }
    });
    meta.append(svc, desc, del);
    li.appendChild(meta);
    list.appendChild(li);
  }
}

async function loadRules() {
  try {
    const data = await call("GET", "/api/rules");
    renderRules(data.items);
  } catch (_) {
    /* rule list is refreshed again shortly */
  }
}

function renderAlerts(data, showAll) {
  const list = $("alerts");
  list.textContent = "";
  const items = data.items;
  if (items.length === 0) {
    const li = document.createElement("li");
    li.className = "hint";
    li.textContent = showAll ? "没有任何告警。" : "没有未确认的告警。";
    list.appendChild(li);
  }
  for (const alert of items) {
    const li = document.createElement("li");
    if (alert.status === "open") li.classList.add("open");
    const meta = document.createElement("div");
    meta.className = "meta";
    const svc = document.createElement("span");
    svc.className = "service";
    svc.textContent = alert.service;
    const state = document.createElement("span");
    state.className = alert.status === "open" ? "alert-state open" : "alert-state";
    state.textContent = alert.status === "open" ? "未确认" : "已确认";
    const time = document.createElement("span");
    time.textContent = `开单 ${fmtTime(alert.opened_at)}`;
    meta.append(svc, state, time);
    const body = document.createElement("div");
    body.className = "alert-body";
    body.textContent =
      `关键词「${alert.keyword}」在 ${alert.window_seconds} 秒窗口内命中 ` +
      `${alert.hit_count} 条（门槛 ${alert.threshold} 条），` +
      `窗口起于 ${fmtTime(alert.window_start)}` +
      (alert.acked_at ? `，确认于 ${fmtTime(alert.acked_at)}` : "");
    li.append(meta, body);
    if (alert.status === "open") {
      const ack = document.createElement("button");
      ack.className = "small";
      ack.textContent = "确认";
      ack.addEventListener("click", async () => {
        ack.disabled = true;
        try {
          await call("POST", `/api/alerts/${alert.id}/ack`);
          void loadAlerts();
          void refreshStatus();
        } catch (error) {
          $("alerts-hint").textContent = `确认失败：${error.message}`;
          ack.disabled = false;
        }
      });
      li.appendChild(ack);
    }
    list.appendChild(li);
  }
}

async function loadAlerts() {
  const showAll = $("alerts-show-all").checked;
  const status = showAll ? "all" : "open";
  try {
    const data = await call("GET", `/api/alerts?status=${status}&limit=100`);
    renderAlerts(data, showAll);
    if (!showAll) {
      $("alerts-hint").textContent =
        data.total > data.items.length
          ? `共 ${data.total} 条未确认，仅显示最近 ${data.items.length} 条。`
          : "达到触发条数时每个窗口只开一条告警。";
    }
  } catch (error) {
    $("alerts-hint").textContent = `无法获取告警：${error.message}`;
  }
}

$("alerts-show-all").addEventListener("change", () => {
  void loadAlerts();
});

$("rule-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("submit-rule");
  const hint = $("rule-hint");
  button.disabled = true;
  hint.textContent = "登记中…";
  try {
    const rule = await call("POST", "/api/rules", {
      service: $("rule-service").value,
      keyword: $("rule-keyword").value,
      window_seconds: Number($("rule-window").value),
      threshold: Number($("rule-threshold").value),
    });
    hint.textContent =
      `已登记：${rule.service} 在 ${rule.window_seconds} 秒内命中 ` +
      `「${rule.keyword}」${rule.threshold} 条时开单。`;
    $("rule-keyword").value = "";
    void loadRules();
  } catch (error) {
    hint.textContent = `登记失败：${error.message}`;
  } finally {
    button.disabled = false;
  }
});

$("log-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("submit-log");
  const hint = $("log-hint");
  button.disabled = true;
  hint.textContent = "写入中…";
  try {
    const saved = await call("POST", "/api/logs", {
      service: $("log-service").value,
      body: $("log-body").value,
    });
    hint.textContent = `已写入 #${saved.id}（${saved.service}）`;
    $("log-body").value = "";
    void loadServices();
    void refreshStatus();
  } catch (error) {
    hint.textContent = `写入失败：${error.message}`;
  } finally {
    button.disabled = false;
  }
});

$("search-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const q = $("search-q").value.trim();
  const service = $("search-service").value.trim();
  const hint = $("search-meta");
  if (!q && !service) {
    hint.textContent = "请至少填写服务名或关键词。";
    $("results").textContent = "";
    return;
  }
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (service) params.set("service", service);
  try {
    const result = await call("GET", `/api/logs?${params}`);
    renderResults(result, q, service);
  } catch (error) {
    hint.textContent = `检索失败：${error.message}`;
    $("results").textContent = "";
  }
});

$("rebuild-button").addEventListener("click", async () => {
  const button = $("rebuild-button");
  const hint = $("rebuild-hint");
  button.disabled = true;
  try {
    const data = await call("POST", "/api/reindex");
    hint.textContent = `已发起重建 #${data.building.id}，期间可继续录入和检索。`;
  } catch (error) {
    hint.textContent = `无法发起重建：${error.message}`;
  } finally {
    await refreshStatus();
  }
});

void refreshStatus();
void loadServices();
void loadRules();
void loadAlerts();
setInterval(refreshStatus, 1000);
setInterval(loadAlerts, 2000);
setInterval(loadServices, 15000);
setInterval(loadRules, 15000);
