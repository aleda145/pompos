(() => {
  const toggle = document.getElementById('toggle-destination');
  const picker = document.getElementById('destination-picker');
  if (!toggle || !picker) return;
  const destination = document.getElementById('change-destination');
  const formatField = document.getElementById('destination-format-field');
  const format = document.getElementById('destination-format');
  const fileNote = document.getElementById('destination-file-note');
  function updateFormat() {
    if (!destination || !formatField || !format || !fileNote) return;
    const files = destination.selectedOptions[0]?.dataset.type === 'files';
    formatField.hidden = !files;
    format.disabled = !files || destination.disabled;
    fileNote.hidden = !files;
  }
  destination?.addEventListener('change', updateFormat);
  updateFormat();
  toggle.addEventListener('click', () => {
    picker.hidden = !picker.hidden;
    toggle.setAttribute('aria-expanded', String(!picker.hidden));
    if (!picker.hidden) picker.querySelector('select')?.focus();
  });
})();
