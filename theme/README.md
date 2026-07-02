# `theme/` — documentation site chrome

Everything that makes the [documentation site](https://victorlou.github.io/spine/) look like Spine lives here, so `docs/` can stay **pure Markdown**. MkDocs loads this folder as the Material theme's `custom_dir` (see `custom_dir: theme` in `mkdocs.yml`).

You only need to touch this folder to change the site's *look or landing page* — never to add or edit a documentation page (those are Markdown files under `docs/`).

```
theme/
  main.html              Base template override: favicons, web manifest, Open Graph tags
  home.html              The custom landing page (used by docs/index.md via `template: home.html`)
  partials/
    hero-mark.svg        Animated Spine mark (idle "alive" motion), inlined into the hero
  assets/
    stylesheets/spine.css   Brand layer over Material: colours, type, landing styles
    javascripts/spine.js    Small enhancements (e.g. hero copy-to-clipboard)
    fonts/                  IBM Plex Sans + Mono (woff2) + @font-face CSS (SIL OFL)
    brand/                  Logo marks and lockups
    favicon/                Favicons, app icons, site.webmanifest
```

Template overrides (`*.html`) must live outside `docs/`, which is why this folder exists at the repo root rather than under `docs/`. Static assets here are copied into the built site and referenced from `mkdocs.yml` (`extra_css`, `extra_javascript`, `theme.logo`, `theme.favicon`) and the templates with root-relative paths like `assets/...`.

Preview changes with `uv run --group docs mkdocs serve`. See [`docs/development.md`](../docs/development.md#documentation-site) for the full workflow.
