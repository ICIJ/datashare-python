# Implement your own Datashare worker

## Clone the template repository

Start by cloning the [template repository](https://github.com/ICIJ/datashare-python):

<!-- termynal -->
```console
$ git clone git@github.com:ICIJ/datashare-python.git
---> 100%
```

## Install dependencies

Install [`uv`](https://docs.astral.sh/uv/getting-started/installation/) and install dependencies:
<!-- termynal -->
```console
$ curl -LsSf https://astral.sh/uv/install.sh | sh
$ uv sync --frozen --group dev
```
