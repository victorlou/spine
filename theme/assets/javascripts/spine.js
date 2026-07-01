// Spine landing — copy-to-clipboard for the install command.
// Delegated so it survives Material's instant navigation.
document.addEventListener("click", function (event) {
  const btn = event.target.closest("[data-copy]");
  if (!btn) return;
  const target = document.querySelector(btn.getAttribute("data-copy"));
  if (!target) return;
  const text = target.textContent.trim();
  navigator.clipboard.writeText(text).then(function () {
    btn.classList.add("spine-copied");
    const original = btn.getAttribute("aria-label");
    btn.setAttribute("aria-label", "Copied");
    setTimeout(function () {
      btn.classList.remove("spine-copied");
      btn.setAttribute("aria-label", original || "Copy command");
    }, 1400);
  });
});
