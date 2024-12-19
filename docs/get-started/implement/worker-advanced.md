# Advanced Datashare worker

In this section we'll augment the worker template app (translation and classification) with 
[vector store](https://en.wikipedia.org/wiki/Vector_database) to allow us to perform semantic similarity searches
between queries and Datashare docs.

Make sure you've followed the [basic worker example](worker-basic.md) to understand the basics ! 

## Clone the template repository

Start over and clone the [template repository](https://github.com/ICIJ/datashare-python) once again:

<!-- termynal -->
```console
$ git clone git@github.com:ICIJ/datashare-python.git
---> 100%
```

## Install extra dependencies

We'll use [LanceDB](https://lancedb.github.io/lancedb/) to implement our vector store, we need to add it as well as
the [sentence-transformers](https://github.com/UKPLab/sentence-transformers) to our dependencies: 

<!-- termynal -->
```console
$ uv add lancedb sentence-transformers
---> 100%
```

!!! note
    In a production setup, since elasticsearch implements its [own vector database](https://www.elastic.co/elasticsearch/vector-database)
    it might have been convenient to use it. For this examples, we're using LanceDB as it's embedded and doesn't require
    any deployment update.

## Embedding Datashare documents

For the demo purpose, we'll split the task of embedding docs into two tasks:

- the `create_vectorization_tasks` which scans the index, get IDs of Datashare docs and batch them and create `vectorize_docs` tasks
- the `vectorize_docs` tasks (triggered by the `create_vectorization_tasks` task) receives docs IDs, 
fetch the doc contents from the index and add them to vector database

!!! note
    We could have performed vectorization in a single task, having first task splitting a large tasks into batches/chunks
    is a commonly used pattern to distribute heavy workloads across workers (learn more in the
    [task workflow guide](../../guides/task-workflows.md)).


### The `create_vectorization_tasks` task

The `create_vectorization_tasks` is defined in the `tasks/vectorize.py` file as following:
```python title="tasks/vectorize.py"
--8<--
vectorize.py:create_vectorization_tasks
--8<--
```


The function starts by creating a schema for our vector DB table using the convenient
[LanceDB embedding function](https://lancedb.github.io/lancedb/embeddings/embedding_functions/) feature,
which will automatically create the record `vector field from the provided source field (`content` in our case) using
our HuggingFace embedding model:
```python title="tasks/vectorize.py" hl_lines="2 6 7"
--8<--
vectorize.py:embedding-schema
--8<--
```

We then (re)-create a vector table using the **DB connection provided by dependency injection** (see the next section to learn more):
```python title="tasks/vectorize.py" hl_lines="4"
--8<--
vectorize.py:create-table
--8<--
```

Next `create_vectorization_tasks` queries the index matching all documents:
```python title="tasks/vectorize.py"
--8<--
vectorize.py:query-docs
--8<--
```
and scroll through results pages creating batches of `batch_size`:
```python title="tasks/vectorize.py"
--8<--
vectorize.py:retrieve-docs
--8<--
```

Finally, for each batch, it spawns a vectorization task using the datashare task client and returns the list of created tasks:
```python title="tasks/vectorize.py" hl_lines="5 6 7 8 10"
--8<--
vectorize.py:batch-vectorization
--8<--
```

### The `lifespan_vector_db` dependency injection

In order to avoid to re-create a DB connection each time the worker processes a task, we leverage 
[dependency injection](../../guides/dependency-injection.md) in order to create the connection at start up and
retrieve it inside our function.

This pattern is already used for the elasticsearch client and the datashare task client, to use it for the vector DB
connection, we'll need to update the
[dependencies.py](https://github.com/ICIJ/datashare-python/blob/main/ml_worker/tasks/dependencies.py) file.

First we need to implement the dependency setup function:
```python title="dependencies.py" hl_lines="10 11"
--8<--
vector_db_dependencies.py:setup
--8<--
```

The function creates a connection to the vector DB located on the filesystem and stores the connection to a
global variable.

We then have to implement a function to make this global available to the rest of the codebase:
```python title="dependencies.py" hl_lines="4"
--8<--
vector_db_dependencies.py:provide
--8<--
```
We also need to make sure the connection is properly exited when the worker stops by implementing the dependency tear down.
We just call the `:::python AsyncConnection.__aexit__` methode:
```python title="dependencies.py" hl_lines="2"
--8<--
vector_db_dependencies.py:teardown
--8<--
```

Read the [dependency injection guide](../../guides/dependency-injection.md) to learn more !


### The `vectorize_docs` task

Next we implement the `vectorize_docs` as following:

```python title="tasks/vectorize.py"
--8<--
vectorize.py:vectorize_docs
--8<--
```

The task function starts by retriving the batch document contents, querying the index by doc IDs:
```python title="tasks/vectorize.py" hl_lines="1-4"
--8<--
vectorize.py:retrieve-doc-content
--8<--
```

Finally, we add each doc content to the vector DB table, because we created table using a schema and the
[embedding function](https://lancedb.github.io/lancedb/embeddings/embedding_functions/) feature, the embedding vector 
will be automatically created from the `content` source field:
```python title="tasks/vectorize.py" hl_lines="5-7"
--8<--
vectorize.py:vectorization
--8<--
```


## Semantic similarity search

Now that we've built a vector store from Datashare's docs, we need to query it. Let's create a `find_most_similar`
task which find the most similar docs for a provided set of queries.

The task function starts by loading the embedding model and vectorizes the input queries: 

```python title="tasks/vectorize.py"  hl_lines="13-14"
--8<--
vectorize.py:find_most_similar
--8<--
```

it then performs an [hybrid search](https://lancedb.github.io/lancedb/hybrid_search/hybrid_search/), using both the
input query vector and its text:
 
```python title="tasks/vectorize.py"   hl_lines="4-11"
--8<--
vectorize.py:hybrid-search
--8<--
```

## Registering the new tasks

In order to turn our function into a Datashare [task](../../learn/concepts-basic.md#tasks), we have to register it into the 
`:::python app` [async app](../../learn/concepts-basic.md#app) variable of the
[app.py](https://github.com/ICIJ/datashare-python/blob/main/ml_worker/app.py) file, using the `:::python @task` decorator:
 
```python title="app.py"   hl_lines="16 17 18 19 20 25 32 37"
--8<--
vectorize_app.py:vectorize-app
--8<--
```

## Testing

Finally, we implement some tests in the `tests/tasks/test_vectorize.py` file:

```python title="tests/tasks/test_vectorize.py"
--8<--
test_vectorize.py:test-vectorize
--8<--
```

We can then run the tests after starting test services using the `ml-worker` Docker Compose wrapper:
<!-- termynal -->
```console
$ ./ml-worker up -d postgresql redis elasticsearch rabbitmq datashare_web
$ uv run --frozen pytest ml_worker/tests/tasks/test_vectorize.py
===== test session starts =====
collected 3 items

ml_worker/tests/tasks/test_vectorize.py ...                                                                                                                                                                                          [100%]

====== 3 passed in 6.87s ======
....
```

## Summary

We've successfully added a vector store to Datashare !

Rather than copy-pasting the above code blocks, you can replace/update your codebase with the following files:

- [`ml_worker/tasks/vectorize.py`](https://github.com/ICIJ/datashare-python/blob/main/docs/src/vectorize.py)
- [`ml_worker/tasks/dependencies`](https://github.com/ICIJ/datashare-python/blob/main/docs/src/vector_db_dependencies.py)
- [`ml_worker/app.py`](https://github.com/ICIJ/datashare-python/blob/main/docs/src/vectorize_app.py)
- [`ml_worker/tests/tasks/test_vectorize.py](https://github.com/ICIJ/datashare-python/blob/main/docs/src/test_vectorize.py)

