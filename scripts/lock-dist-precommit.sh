#!/bin/bash
set -e

projects=()

for f in "$@"; do
    if [[ "$f" == worker-template/* ]]; then
        projects+=("worker-template")
    elif [[ "$f" == workers/*/* ]]; then
        project="${f#workers/}"
        project="${project%%/*}"
        projects+=("$project")
    fi
done

if [[ ${#projects[@]} -eq 0 ]]; then
    exit 0
fi

unique_projects=($(printf '%s\n' "${projects[@]}" | sort -u))

for project in "${unique_projects[@]}"; do
    echo "lock-dist: rebuilding uv.dist.lock for ${project}"
    make lock-dist project="${project}"
    if [[ "${project}" == "worker-template" ]]; then
        git add worker-template/uv.lock worker-template/uv.dist.lock
    else
        git add "workers/${project}/uv.lock" "workers/${project}/uv.dist.lock"
    fi
done
