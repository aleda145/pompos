(() => {
  const form = document.querySelector('#destination-form');
  const name = document.querySelector('#destination_name');
  const type = document.querySelector('#destination_type');
  const path = document.querySelector('#destination_path');
  const cancel = document.querySelector('#cancel-destination-update');
  const links = [...document.querySelectorAll('[data-destination-name]')];
  const existingNames = new Set(links.map(link => link.dataset.destinationName));

  function updateMode() {
    const editing = existingNames.has(name.value.trim());
    const label = editing ? 'Update destination' : 'Add destination';
    form.querySelector('h2').textContent = label;
    form.querySelector('button[type="submit"]').textContent = label;
    cancel.hidden = !editing;
  }

  function selectDestination(destinationName, destinationType, destinationPath) {
    name.value = destinationName;
    type.value = destinationType;
    path.value = destinationPath;
    updateMode();
    form.scrollIntoView({block: 'start'});
    (destinationName ? type : name).focus({preventScroll: true});
  }

  name.addEventListener('input', updateMode);
  updateMode();
  for (const link of links) {
    link.addEventListener('click', (event) => {
      if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
      event.preventDefault();
      selectDestination(link.dataset.destinationName, link.dataset.destinationType, link.dataset.destinationPath);
    });
  }
  cancel.addEventListener('click', (event) => {
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    selectDestination('', 'duckdb', '');
  });
})();
