# Example source definitions

These files are **templates** only. Copy them into `config/sources/` and adjust names, credentials, and URLs for your environment.

| File | Description |
|------|-------------|
| [`jsonplaceholder.yml`](jsonplaceholder.yml) | REST source against [JSONPlaceholder](https://jsonplaceholder.typicode.com/) (public test API). Dependencies between resources demonstrate `SOURCE`-style parameters. |
| [`jsonplaceholder.iceberg.append.yml`](jsonplaceholder.iceberg.append.yml) | REST source example showing Iceberg append writes to a warehouse-relative path that resolves to a catalog-backed table (for example, `jsonplaceholder/posts` -> `iceberg.jsonplaceholder.posts`). |
| [`postgres.example.yml`](postgres.example.yml) | PostgreSQL / relational source shape (placeholders for host, database, credentials). |
