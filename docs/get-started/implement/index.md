# Install the [`datashare-python`](https://pypi.org/project/datashare-python/) CLI

We recommend running the [`datashare-python`](https://pypi.org/project/datashare-python/) CLI using [uv](https://docs.astral.sh/uv).

If you don't have [uv](https://docs.astral.sh/uv) yet, start by installing it:

=== "Linux, MacOS"
    <!-- termynal -->
    ```console
    $ curl -LsSf https://astral.sh/uv/install.sh | sh
    ---> 100%
    ```
=== "Windows"
    <!-- termynal -->
    ```console
    $ powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ---> 100%
    ```

Then verify the installation:
<!-- termynal -->
```console
uvx datashare-python --version
```

Feel also free to install and run the [`datashare-python`](https://pypi.org/project/datashare-python/) CLI with the
package/tool manager of your choice.

## Explore the [datashare-python](https://github.com/ICIJ/datashare-python) codebase

[datashare-python](https://github.com/ICIJ/datashare-python) contains all worker implementations powering Datashare's backend 
(speech-to-text, translation...), don't hesitate to have a look at the existing codebase before starting 
(or get back to it later on) !

## Get started...
...and see how to implement a [Basic Worker](./worker-basic.md) or a more [Advanced Worker](./worker-advanced.md) !