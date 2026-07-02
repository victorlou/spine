// Spine landing — copy-to-clipboard for the install command.
// Delegated so it survives Material's instant navigation.
document.addEventListener("click", function (event) {
  const btn = event.target.closest("[data-copy]");
  if (!btn) return;
  const target = document.querySelector(btn.getAttribute("data-copy"));
  if (!target) return;

  // Clipboard API is unavailable on non-secure (plain HTTP, non-localhost) origins.
  if (!navigator.clipboard || !navigator.clipboard.writeText) return;

  const text = target.textContent.trim();
  navigator.clipboard.writeText(text).then(function () {
    // Stash the pristine label once so overlapping clicks can't latch "Copied".
    if (!btn.dataset.label) {
      btn.dataset.label = btn.getAttribute("aria-label") || "Copy command";
    }
    if (btn._resetTimer) clearTimeout(btn._resetTimer);
    btn.classList.add("spine-copied");
    btn.setAttribute("aria-label", "Copied");
    btn._resetTimer = setTimeout(function () {
      btn.classList.remove("spine-copied");
      btn.setAttribute("aria-label", btn.dataset.label);
    }, 1400);
  }).catch(function () {
    // Permission denied or blocked by policy — fail quietly rather than throw.
  });
});
