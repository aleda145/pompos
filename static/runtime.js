(() => {
  const panel = document.querySelector("[data-runtime-url]");
  if (!panel) return;
  const content = panel.querySelector("[data-runtime-content]");
  const error = panel.querySelector("[data-runtime-error]");
  let timer;
  let request;

  async function poll() {
    clearTimeout(timer);
    if (request || document.hidden) return;
    request = new AbortController();
    const timeout = setTimeout(() => request?.abort(), 10000);
    try {
      const response = await fetch(panel.dataset.runtimeUrl, { cache: "no-store", signal: request.signal });
      if (!response.ok || response.redirected) throw new Error("Runtime unavailable");
      const fragment = document.createElement("template");
      fragment.innerHTML = await response.text();
      const next = fragment.content.querySelector("[data-runtime-state]");
      if (!next) throw new Error("Missing runtime status");
      window.pomposTimezone?.render(next);
      const focused = content.contains(document.activeElement) ? document.activeElement : null;
      const href = focused?.getAttribute("href");
      const region = focused?.getAttribute("aria-label");
      const scrolls = new Map(Array.from(content.querySelectorAll(".table-scroll"), (table) => [
        table.getAttribute("aria-label"), { top: table.scrollTop, left: table.scrollLeft },
      ]));
      content.replaceChildren(next);
      for (const table of content.querySelectorAll(".table-scroll")) {
        const position = scrolls.get(table.getAttribute("aria-label"));
        if (position) {
          table.scrollTop = position.top;
          table.scrollLeft = position.left;
        }
      }
      if (focused) {
        const target = href
          ? Array.from(content.querySelectorAll("a")).find((link) => link.getAttribute("href") === href)
          : Array.from(content.querySelectorAll(".table-scroll")).find((table) => table.getAttribute("aria-label") === region);
        (target || content.querySelector(".table-scroll"))?.focus({ preventScroll: true });
      }
      error.hidden = true;
    } catch {
      error.textContent = "Runtime update failed. Showing the last snapshot; retrying.";
      error.hidden = false;
    } finally {
      clearTimeout(timeout);
      request = null;
      if (!document.hidden) timer = setTimeout(poll, 5000);
    }
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) clearTimeout(timer);
    else poll();
  });
  window.addEventListener("pagehide", () => {
    clearTimeout(timer);
    request?.abort();
  });
  timer = setTimeout(poll, 5000);
})();
