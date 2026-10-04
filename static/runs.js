(() => {
  const panel = document.querySelector("[data-runs-url]");
  if (!panel) return;
  const content = panel.querySelector("[data-runs-content]");
  const error = panel.querySelector("[data-poll-error]");
  const form = document.querySelector("[data-run-form]");
  const announce = document.createElement("span");
  announce.className = "sr-only";
  announce.setAttribute("role", "status");
  panel.append(announce);

  let timer;
  let request;
  let generation = 0;
  let params = query(location.href);
  let latest = state();

  function query(href) {
    const source = new URL(href, location.href).searchParams;
    const result = new URLSearchParams();
    for (const key of ["before", "run_id"]) {
      if (source.has(key)) result.set(key, source.get(key));
    }
    return result;
  }

  function state() {
    return content.querySelector("[data-run-state]")?.dataset || {};
  }

  function update(fragment) {
    const next = fragment.querySelector("[data-run-state]");
    if (!next) throw new Error("Missing run state");
    const previous = content.querySelector("[data-run-state]");
    if (!previous) {
      content.replaceChildren(next);
    } else {
      for (const selector of ["[data-run-summary]", "[data-run-meta]", "[data-run-nav]", "[data-run-history]"]) {
        const target = previous.querySelector(selector);
        const source = next.querySelector(selector);
        if (target.innerHTML === source.innerHTML) continue;
        const top = target.scrollTop;
        const left = target.scrollLeft;
        const focused = target.contains(document.activeElement) ? document.activeElement.getAttribute("href") : null;
        target.innerHTML = source.innerHTML;
        target.scrollTop = top;
        target.scrollLeft = left;
        if (focused) {
          const link = Array.from(target.querySelectorAll("a")).find((a) => a.getAttribute("href") === focused);
          link?.focus({ preventScroll: true });
        }
      }
      const output = previous.querySelector("[data-run-output]");
      const source = next.querySelector("[data-run-output]");
      const switched = output.dataset.runId !== source.dataset.runId;
      const follow = switched || output.scrollHeight - output.scrollTop - output.clientHeight < 32;
      if (output.textContent !== source.textContent) output.textContent = source.textContent;
      output.dataset.runId = source.dataset.runId;
      if (follow) output.scrollTop = output.scrollHeight;
      for (const [key, value] of Object.entries(next.dataset)) previous.dataset[key] = value;
    }
    // Copy values; DOMStringMap itself changes when the next poll is applied.
    const current = { ...state() };
    if (latest.latestId !== current.latestId || latest.latestStatus !== current.latestStatus) {
      const labels = { pending: "Queued", running: "RUNNING", succeeded: "SUCCESS", failed: "FAILED" };
      announce.textContent = `Run #${current.latestId}: ${labels[current.latestStatus] || current.latestStatus}`;
      if (current.latestStatus === "succeeded") document.dispatchEvent(new Event("pompos:run-succeeded"));
    }
    latest = current;
    const editRun = document.querySelector('[data-edit-run]');
    if (editRun) editRun.value = content.querySelector('[data-run-output]')?.dataset.runId || '';
    if (form) form.querySelector("button").disabled = form.dataset.configUnavailable === "true" || current.active === "true";
  }

  async function poll() {
    clearTimeout(timer);
    request?.abort();
    const version = ++generation;
    const controller = new AbortController();
    request = controller;
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(`${panel.dataset.runsUrl}?${params}`, {
        signal: controller.signal, cache: "no-store", redirect: "error",
      });
      if (!response.ok) throw new Error("Run history unavailable");
      const html = await response.text();
      if (version !== generation) return;
      const template = document.createElement("template");
      template.innerHTML = html;
      update(template.content);
      error.hidden = true;
    } catch {
      if (version !== generation) return;
      error.textContent = "Updates unavailable. Retrying…";
      error.hidden = false;
    } finally {
      clearTimeout(timeout);
      if (version === generation) {
        timer = setTimeout(() => {
          if (!document.hidden) poll();
        }, error.hidden && latest.active === "true" ? 1500 : 8000);
      }
    }
  }

  panel.addEventListener("click", (event) => {
    const link = event.target.closest("[data-run-link]");
    if (!link || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    params = query(link.href);
    history.pushState(null, "", link.href);
    poll();
  });
  window.addEventListener("popstate", () => {
    params = query(location.href);
    poll();
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) poll();
    else clearTimeout(timer);
  });
  form?.addEventListener("submit", () => { form.querySelector("button").disabled = true; });
  latest = { ...latest };
  const output = content.querySelector("[data-run-output]");
  if (output) output.scrollTop = output.scrollHeight;
  poll();
})();
