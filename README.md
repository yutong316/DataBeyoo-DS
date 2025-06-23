# DataBeyoo-DS

## Project Architecture

The project processes journal metadata from two different sources—CSV and JSON files—and stores them in two separate but connected databases:
Graph database (Blazegraph): Stores semantic journal metadata (e.g., title, ISSNs, languages, publisher, license, DOAJ Seal, APC status).
Relational database (SQLite): Stores subject categories and areas, and models many-to-many relationships between journals, categories, and areas.

### Unified Data Structure

There are three core entities: `Journal`, `Category`, and `Area`, all inheriting from a common base class `IdentifiableEntity`.
This base class provides a shared `id` field and a `getIds()` method, enabling consistent handling of identifiers and future scalability.

* **Journal**: Includes metadata such as title, languages, publisher, license, APC, DOAJ seal, categories, and areas.
* **Category**: Represents journal subject classification and includes an optional `quartile` (e.g., Q1–Q4).
* **Area**: Denotes broad academic domains such as Medicine, Social Sciences, etc.

### Modular Upload Framework

We introduce a class hierarchy to support uploading from different formats:

* `Handler`: Base class to manage database path or URL.
* `UploadHandler`: Abstract subclass that defines a unified interface `pushDataToDb()`.
* `JournalUploadHandler`: Uploads journal metadata from a CSV file to Blazegraph using SPARQL.
* `CategoryUploadHandler`: Uploads classification and area data from a JSON file to SQLite using SQL.

This design enables consistent interfaces and easy extensibility.

### Dual Database Strategy

* **Blazegraph (Graph DB)**: Stores semantically rich journal metadata and supports SPARQL queries.
* **SQLite (Relational DB)**: Stores structured relationships using normalized tables.

### Query & Integration Modules

* **Query Module**: Handles single-database queries via SPARQL (for Blazegraph) or SQL (for SQLite).
* **Integration Engine**: Combines data from both sources, constructs unified objects, and exposes a final query interface.

---

## Team Responsibilities

### Yutong Li

* Designed the entity classes: `IdentifiableEntity`, `Journal`, `Category`, `Area`
* Implemented data upload handlers: `JournalUploadHandler`, `CategoryUploadHandler`
* Handled CSV/JSON parsing and uploading to Blazegraph & SQLite
* Led final integration, test runs, and debugging of the entire system

### Yuming Lian

* Implemented SPARQL and SQL query logic
* Developed the classes: `QueryHandler`, `JournalQueryHandler`, `CategoryQueryHandler`
* Focused on single-database querying and data retrieval

### Xinyi Guo

* Built the cross-database query engine
* Implemented: `BasicQueryEngine`, `FullQueryEngine`
* Responsible for data matching, object construction, and final query interface




