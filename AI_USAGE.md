# AI Usage Disclosure

This repository was developed with assistance from AI coding tools, in line
with the assignment's transparency requirement.

## Tool used
- Claude LLM 

## What AI was used for
- Drafting the project structure and the `docker-compose.yml` skeleton.
- Generating the initial Luigi `Task` boilerplate (class hierarchy, `requires`
  / `output` / `run` patterns) which was then reviewed and adapted.
- Writing the seed CSV column layout
- Producing the first draft of this README.

## What AI was *not* used for
- Final selection of dependency versions in `requirements.txt` (verified
  against PyPI release pages and the Elasticsearch Python client
  compatibility matrix).
- The decision to use `pd.cut` for bucketing and `to_sql` for the Postgres
  load — both are deliberate choices reviewed for correctness.
- The Kibana data-view bootstrap call (verified against the Kibana 8.15 REST
  API documentation).

## Review process
Every AI-generated file was read, executed locally (`docker compose up`,
`make run`, manual checks of Postgres rows and Elasticsearch documents),
and edited where needed before being committed. No file in this repository
was committed unread.
