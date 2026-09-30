(() => {
  const form = document.querySelector('#secret-form');
  const name = document.querySelector('#secret_name');
  const value = document.querySelector('#secret_value');
  const cancel = document.querySelector('#cancel-secret-update');
  const links = [...document.querySelectorAll('[data-secret-name]')];
  const existingNames = new Set(links.map(link => link.dataset.secretName));

  function updateMode() {
    const editing = existingNames.has(name.value.trim());
    form.querySelector('h2').textContent = editing ? 'Update secret' : 'Add secret';
    form.querySelector('label[for="secret_value"]').textContent = editing ? 'New value' : 'Value';
    form.querySelector('button[type="submit"]').textContent = editing ? 'Update secret' : 'Add secret';
    cancel.hidden = !editing;
  }

  function selectSecret(secretName) {
    name.value = secretName;
    value.value = '';
    updateMode();
    form.scrollIntoView({block: 'start'});
    (secretName ? value : name).focus({preventScroll: true});
  }

  name.addEventListener('input', updateMode);
  updateMode();
  for (const link of links) {
    link.addEventListener('click', (event) => {
      if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
      event.preventDefault();
      selectSecret(link.dataset.secretName);
    });
  }
  cancel.addEventListener('click', (event) => {
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    selectSecret('');
  });
})();
