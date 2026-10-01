(() => {
  const panel = document.querySelector("[data-preview-url]");
  if (!panel) return;

  async function loadPreview() {
    if (panel.getAttribute("aria-busy") === "true") return;
    panel.setAttribute("aria-busy", "true");
    panel.innerHTML = '<h2>Table preview</h2><p class="hint" role="status">Loading preview…</p>';
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(panel.dataset.previewUrl, {
        signal: controller.signal,
        cache: "no-store",
        redirect: "error",
      });
      if (!response.ok) throw new Error("Preview unavailable");
      panel.innerHTML = await response.text();
    } catch {
      panel.innerHTML = '<h2>Table preview</h2><p class="hint" role="status">Preview unavailable.</p><button type="button" data-preview-retry>Retry</button>';
    } finally {
      clearTimeout(timeout);
      panel.setAttribute("aria-busy", "false");
    }
  }

  panel.addEventListener("click", (event) => {
    if (event.target.closest("[data-preview-retry]")) loadPreview();
  });
  loadPreview();
})();
