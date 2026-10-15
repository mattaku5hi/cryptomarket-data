#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: tools/cargo_budget.sh [--dry-run | [+toolchain] cargo-args...]

Runs Cargo with bounded workspace artifacts. --dry-run reports the target size
and asks cargo-sweep to show what it would remove, without running Cargo.
EOF
}

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
workspace_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
target_dir="$workspace_root/target"
max_size=${CARGO_BUDGET_MAXSIZE:-8GB}

if [[ ! $max_size =~ ^[1-9][0-9]*(B|KB|MB|GB|TB)?$ ]]; then
    printf 'invalid CARGO_BUDGET_MAXSIZE: %s\n' "$max_size" >&2
    exit 64
fi

if [[ -L $target_dir ]]; then
    printf 'refusing symlink target directory: %s\n' "$target_dir" >&2
    exit 73
fi
if [[ -e $target_dir && ! -d $target_dir ]]; then
    printf 'target path is not a directory: %s\n' "$target_dir" >&2
    exit 73
fi
if ! command -v cargo-sweep >/dev/null 2>&1; then
    printf 'cargo-sweep is required; install it before using this wrapper\n' >&2
    exit 69
fi

report_target_size() {
    local label=$1
    local size
    if [[ -d $target_dir ]]; then
        size=$(du -sh -- "$target_dir" | cut -f1)
    else
        size=missing
    fi
    printf 'workspace target %s: %s (cap %s)\n' "$label" "$size" "$max_size"
}

sweep_target() {
    local dry_run=$1
    local -a args=(sweep --maxsize "$max_size")
    if [[ $dry_run == true ]]; then
        args+=(--dry-run)
    fi
    # Pass the workspace root only. In particular, do not use --recursive.
    CARGO_TARGET_DIR="$target_dir" cargo-sweep "${args[@]}" "$workspace_root"
}

if [[ ${1-} == --dry-run ]]; then
    if [[ $# -ne 1 ]]; then
        usage >&2
        exit 64
    fi
    report_target_size before-dry-run
    sweep_target true
    report_target_size after-dry-run
    exit 0
fi
if [[ $# -eq 0 ]]; then
    usage >&2
    exit 64
fi

mkdir -p -- "$target_dir"
if [[ $(CDPATH= cd -- "$target_dir" && pwd -P) != "$target_dir" ]]; then
    printf 'target directory resolved outside the workspace target path\n' >&2
    exit 73
fi

exec 9>"$workspace_root/.cargo-budget.lock"
if ! flock -x 9; then
    printf 'could not acquire workspace build lock\n' >&2
    exit 75
fi

report_target_size before

args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    arg=${args[$i]}
    case $arg in
        --target-dir|--target-dir=*|--manifest-path|--manifest-path=*|--config|--config=*)
            printf 'refusing Cargo path or config override: %s\n' "$arg" >&2
            exit 64
            ;;
    esac
done

if sweep_target false; then
    :
else
    status=$?
    printf 'pre-build cargo-sweep failed with status %s\n' "$status" >&2
    exit "$status"
fi
report_target_size after-pre-build-sweep

set +e
(
    cd -- "$workspace_root"
    CARGO_TARGET_DIR="$target_dir" CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo "${args[@]}"
)
cargo_status=$?
set -e

report_target_size after-build
mkdir -p -- "$target_dir"
if sweep_target false; then
    sweep_status=0
else
    sweep_status=$?
    printf 'post-build cargo-sweep failed with status %s\n' "$sweep_status" >&2
fi
report_target_size after-post-build-sweep

if [[ $cargo_status -ne 0 ]]; then
    exit "$cargo_status"
fi
exit "$sweep_status"
