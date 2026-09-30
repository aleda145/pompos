// Highlight only explicitly marked source blocks; logs and errors stay plain text.
if (window.hljs) {
  hljs.configure({cssSelector: 'pre code.language-python, pre code.language-yaml'});
  hljs.highlightAll();
}
