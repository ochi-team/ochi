# Ochi web page

Holds documentation, blog, landing.

## 🧞 Commands

| Command                   | Action                                           |
| :------------------------ | :----------------------------------------------- |
| `npm install`             | Installs dependencies                            |
| `npm run dev`             | Starts local dev server at `localhost:4321`      |
| `npm run build`           | Build your production site to `./dist/`          |
| `npm run preview`         | Preview your build locally, before deploying     |
| `npm run astro ...`       | Run CLI commands like `astro add`, `astro check` |
| `npm run astro -- --help` | Get help using the Astro CLI                     |

## Changelog guide

Expected to see on the top
- date
- version

The following categories are allowed:
- query
- ingest
- config
- UI
- installation
- others

Every category may have a verb and a change description, for example:

Added: new thing is possible

Possible verbs:
- Added
- Changed
- Deprecated
- Removed
- Fixed
- Security
- Performance

A verb may be prefixed with **BREAKING:** if the change is breaking.
Highly encouraged to review on breaking change the verbs Changed, Deprecated, Removed, Fixed, Security.

The following information must be attached:
- a link to a PR/commit
- a link to a documentation


##### Documentation todo list

TODO: setup spellchecker for docs
TODO: extract website to its repo, leave only related content: docs, changelog, api reference, etc.
TODO: make the changelog date used from the frontmatter, not the direct paragraph
TODO: leave a link "edit on github" on a doc page
TODO: add a linter to validate all the links are accessible in the content

