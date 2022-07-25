# DepMap-BioCypher

This is the adapter for the BioCypher representation of the Dependency
Map dataset. It assumes presence of the raw (v0.5) input data in the
`data/v0.5/` folder (in the respective subfolders `cellModels`,
`compounds`, and `genes`). I made one change to remove a multi-line
field from `cellModels_all.txt` in line 832 (simply removing the
newline). The adapter uses the ontological mapping specified in
`config/schema_config.yaml` to assign input data onto the ontological
hierarchy supplied by Biolink (flexibly extended using various BioCypher
methods).

The adapter (`adapter.py`) currently only generates `neo4j-admin
import`-ready files via the batch writer class (to `biocypher-out` or
any specified location). Interaction with the graph via the driver will
be implemented next. The process can be run using `script.py`. If all
goes well, the output folder will contain a `neo4j-admin-import-call.sh`
file which, when run in the terminal in the database folder, will create
a new database under the name specified in the `db_name` argument of the
`BioCypherAdapter` instantiation in `script.py`. The database does not
need to be running at this point (but it can). However, it is important
that the target database (in the current example the one named "import")
is either completely empty or does not exist yet.

After import, and in case the database in point did not exist yet, the
database can be created and activated in the Neo4j browser with `:use
system`, `create database <db_name>`, `:use <db_name>`. At this point,
the DBMS needs to be running.