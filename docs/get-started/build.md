# Build your worker image

## Local docker build

To build a docker image of the worker we've previously built, we can use the 
[`datashare-python`](https://github.com/ICIJ/datashare-python/blob/main/datashare-python) script.

It's a tiny wrapper around the `docker compose` CLI:
<!-- termynal -->
```console
$ ./datashare-python build datashare-python
=> [datashare-python internal] booting buildkit                                                                                                                                                                                        0.9s
=> => starting container buildx_buildkit_strange_curran0                                                                                                                                                                        0.9s
=> [datashare-python internal] load build definition from Dockerfile
...
=> => exporting layers                                                                                                                                                                                                         56.1s
=> => exporting manifest sha256:7462a3f43df6073c57fc2482726a65d43e4f83f68ccd098ec0804b8b959d9a17                                                                                                                                0.0s
=> => exporting config sha256:184ed641f1ee82d4eb068143702d3cbec32b25413051764000353b02458e12a1                                                                                                                                  0.0s
=> => sending tarball                                                                                                                                                                                                          38.7s
=> [datashare-python datashare-python] importing to docker
```

## Publish to [Docker Hub](https://hub.docker.com/)

You can also fork the template repository and use the CI to build and publish the worker image to [Docker Hub](https://hub.docker.com/).

To publish, make sure to set the `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` ([see documentation](https://docs.docker.com/security/for-developers/access-tokens/))
and then just create a tag to trigger the build.
