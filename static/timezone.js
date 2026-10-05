(() => {
  const selectedTimezone = document.body.dataset.timezone || "";
  let formatter;
  try {
    formatter = new Intl.DateTimeFormat("en-GB", {
      timeZone: selectedTimezone || "UTC", year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
    });
  } catch {
    // Leave the server's UTC timestamps and labels intact if unsupported.
    return;
  }

  function render(root = document) {
    root.querySelectorAll("[data-display-timezone]").forEach((label) => {
      label.hidden = selectedTimezone !== "";
    });
    root.querySelectorAll("time[data-display-time]").forEach((element) => {
      const date = new Date(element.dateTime);
      if (!Number.isFinite(date.getTime())) return;
      const parts = Object.fromEntries(formatter.formatToParts(date).map(({ type, value }) => [type, value]));
      element.textContent = `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
    });
  }

  window.pomposTimezone = { render };
  render();

  const picker = document.querySelector("#display-timezone");
  if (!picker) return;
  const control = document.querySelector("#timezone-control");
  const toggle = document.querySelector("#timezone-toggle");
  const popup = document.querySelector("#timezone-picker");
  const value = document.querySelector("#timezone-value");
  const current = document.querySelector("#timezone-current");
  const search = document.querySelector("#timezone-search");
  const empty = document.querySelector("#timezone-empty");
  const now = new Date();
  const zones = new Set(Intl.supportedValuesOf("timeZone"));
  if (selectedTimezone) zones.add(selectedTimezone);
  const choices = [{ value: "", city: "UTC (default)", offset: 0, label: "UTC (default)" }];
  for (const zone of zones) {
    const name = new Intl.DateTimeFormat("en-US", { timeZone: zone, timeZoneName: "longOffset" })
      .formatToParts(now).find((part) => part.type === "timeZoneName").value;
    const match = name.match(/GMT([+-])(\d{2}):(\d{2})/);
    const offset = match ? (Number(match[2]) * 60 + Number(match[3])) * (match[1] === "-" ? -1 : 1) : 0;
    const city = zone.split("/").at(-1).replaceAll("_", " ");
    const hours = String(Math.floor(Math.abs(offset) / 60)).padStart(2, "0");
    const minutes = String(Math.abs(offset) % 60).padStart(2, "0");
    choices.push({ value: zone, city, offset, label: `UTC${offset < 0 ? "−" : "+"}${hours}:${minutes} · ${city}` });
  }
  choices.sort((a, b) => a.offset - b.offset || a.city.localeCompare(b.city));

  function showCurrent() {
    current.textContent = value.value ? value.value.split("/").at(-1).replaceAll("_", " ") : "UTC";
  }

  function filter() {
    const query = search.value.trim().toLocaleLowerCase();
    const matches = choices.filter((choice) => choice.city.toLocaleLowerCase().includes(query));
    picker.replaceChildren(...matches.map((choice) => new Option(choice.label, choice.value)));
    picker.selectedIndex = matches.findIndex((choice) => choice.value === value.value);
    empty.hidden = matches.length !== 0;
  }

  function close(focusToggle = false) {
    popup.hidden = true;
    toggle.setAttribute("aria-expanded", "false");
    if (focusToggle) toggle.focus();
  }

  function choose() {
    if (picker.selectedIndex < 0) return;
    value.value = picker.value;
    showCurrent();
    close(true);
  }

  toggle.addEventListener("click", () => {
    if (!popup.hidden) {
      close(true);
      return;
    }
    popup.hidden = false;
    toggle.setAttribute("aria-expanded", "true");
    search.value = "";
    filter();
    search.focus();
  });
  search.addEventListener("input", filter);
  search.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (picker.selectedIndex < 0 && picker.options.length) picker.selectedIndex = 0;
      picker.focus();
    } else if (event.key === "Enter") {
      event.preventDefault();
      if (picker.selectedIndex < 0 && picker.options.length) picker.selectedIndex = 0;
      choose();
    }
  });
  picker.addEventListener("click", choose);
  picker.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      choose();
    }
  });
  control.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !popup.hidden) {
      event.preventDefault();
      close(true);
    }
  });
  control.addEventListener("focusout", (event) => {
    if (!control.contains(event.relatedTarget)) close();
  });
  document.addEventListener("pointerdown", (event) => {
    if (!control.contains(event.target)) close();
  });
  showCurrent();
  toggle.disabled = false;
})();
