// populate_languages.js - fills .lang-select selects with LANGUAGES
(function () {
  function makeOption(lang) {
    const opt = document.createElement("option");
    opt.value = lang.code;
    opt.textContent = lang.name + ` (${lang.code})`;
    return opt;
  }

  function populate() {
    if (!window.LANGUAGES) return;
    const selects = document.querySelectorAll("select.lang-select");
    selects.forEach((sel) => {
      // Some selects use the same styling but have fixed (non-language) options.
      // Example: DOCX OCR mode selector.
      if (sel.dataset && sel.dataset.staticOptions === "true") {
        return;
      }
      // preserve current value
      const cur = sel.value;
      // clear existing
      sel.innerHTML = "";

      // Add a placeholder option
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = sel.dataset.placeholder || "Chọn ngôn ngữ";
      placeholder.disabled = true;
      placeholder.selected = !cur;
      sel.appendChild(placeholder);

      // If select supports auto-detect, add 'auto' option at top
      if (sel.dataset.autodetect === "true") {
        const autoOpt = document.createElement("option");
        autoOpt.value = "auto";
        autoOpt.textContent =
          sel.dataset.autodetectLabel || "Tự động (Auto-detect)";
        autoOpt.selected = !cur;
        sel.appendChild(autoOpt);
      }

      window.LANGUAGES.forEach((lang) => {
        sel.appendChild(makeOption(lang));
      });

      // restore value if present
      if (cur) {
        sel.value = cur;
      }
      // If this is the upload target select and no prior value, default to English (so uploaded files are translated)
      if (!cur && sel.id === "uploadTargetLang") {
        sel.value = "en";
      } else if (!cur && sel.dataset.default === "vi") {
        // legacy behaviour: default to Vietnamese for other selects marked with data-default="vi"
        sel.value = "vi";
      }
    });
  }

  if (document.readyState === "loading")
    document.addEventListener("DOMContentLoaded", populate);
  else populate();
})();
