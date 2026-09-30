const fs = require('node:fs');
const path = require('node:path');
const source = process.argv[2];
const version = JSON.parse(fs.readFileSync(path.join(source, 'package.json'), 'utf8')).version;
if (version !== '11.11.1') throw new Error('Expected highlight.js 11.11.1');
const wrap = file => `(() => { const module = {exports: {}};\n${fs.readFileSync(path.join(source, file), 'utf8')}\nreturn module.exports; })()`;
const bundle = `/* highlight.js ${version} | BSD-3-Clause | https://highlightjs.org/ */
(() => {
  const hljs = ${wrap('lib/core.js')};
  hljs.registerLanguage('python', ${wrap('lib/languages/python.js')});
  hljs.registerLanguage('yaml', ${wrap('lib/languages/yaml.js')});
  window.hljs = hljs;
})();
`;
fs.writeFileSync(path.join(__dirname, 'highlight.js'), bundle);
fs.copyFileSync(path.join(source, 'LICENSE'), path.join(__dirname, 'LICENSE'));
