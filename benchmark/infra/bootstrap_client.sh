#!/usr/bin/env bash
set -euo pipefail

BENCH_PREFIX="${BENCH_PREFIX:-/usr/local/bench}"
BENCH_BIN="${BENCH_PREFIX}/bin"
CORPUS_DIR="${BENCH_PREFIX}/corpus"
BUILD_ROOT="${BUILD_ROOT:-/tmp/bowl-bench-build}"
XFSTESTS_COMMIT="22348afe338c0f6d540c0d7ef0db749eaa51b218"
SMALLFILE_COMMIT="aa2519de"
LIBFUSE_TAG="${LIBFUSE_TAG:-fuse-3.16.2}"
JUICEFS_VERSION="${JUICEFS_VERSION:-1.2.1}"
IOR_TAG="${IOR_TAG:-3.3.0}"
CORPUS_TAR_FILE_BYTES="${CORPUS_TAR_FILE_BYTES:-30720}"

log() {
  printf '[%s] [bootstrap-client] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

wait_apt_lock() {
  while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do
    sleep 2
  done
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) echo "amd64" ;;
  esac
}

mkdir -p "${BENCH_BIN}" "${CORPUS_DIR}" "${BUILD_ROOT}"

log "Installing Ubuntu 24.04 client benchmark packages..."
wait_apt_lock
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq \
  fio \
  mpich \
  git \
  golang-go \
  sqlite3 \
  python3 \
  python3-yaml \
  fuse3 \
  libfuse3-dev \
  iperf3 \
  strace \
  jq \
  build-essential \
  meson \
  ninja-build \
  pkg-config \
  autoconf \
  automake \
  libtool \
  uuid-dev \
  libaio-dev \
  xfslibs-dev \
  perl \
  curl \
  ca-certificates \
  util-linux \
  psmisc \
  attr \
  libattr1-dev \
  libacl1-dev >/dev/null

apt-get install -y -qq liburing-dev >/dev/null 2>&1 || true

grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf
sysctl -qw net.ipv4.tcp_autocorking=0 2>/dev/null || true
ulimit -n 1048576 2>/dev/null || true

ARCH="$(detect_arch)"

if [[ ! -x "${BENCH_BIN}/juicefs" ]]; then
  log "Installing JuiceFS v${JUICEFS_VERSION} (${ARCH})..."
  curl -fsSL "https://github.com/juicedata/juicefs/releases/download/v${JUICEFS_VERSION}/juicefs-${JUICEFS_VERSION}-linux-${ARCH}.tar.gz" \
    -o "${BUILD_ROOT}/juicefs.tar.gz"
  tar -xzf "${BUILD_ROOT}/juicefs.tar.gz" -C "${BUILD_ROOT}" juicefs
  install -m 0755 "${BUILD_ROOT}/juicefs" "${BENCH_BIN}/juicefs"
  ln -sf "${BENCH_BIN}/juicefs" /usr/local/bin/juicefs
  rm -f "${BUILD_ROOT}/juicefs.tar.gz" "${BUILD_ROOT}/juicefs"
fi

if [[ ! -x "${BENCH_BIN}/mdtest" ]]; then
  if apt-cache show mdtest >/dev/null 2>&1 && apt-get install -y -qq mdtest >/dev/null 2>&1 && command -v mdtest >/dev/null 2>&1; then
    ln -sf "$(command -v mdtest)" "${BENCH_BIN}/mdtest"
  else
    log "Building mdtest from hpc/ior (${IOR_TAG})..."
    rm -rf "${BUILD_ROOT}/ior"
    git clone --depth 1 --branch "${IOR_TAG}" https://github.com/hpc/ior.git "${BUILD_ROOT}/ior" \
      || git clone --depth 1 https://github.com/hpc/ior.git "${BUILD_ROOT}/ior"
    (
      cd "${BUILD_ROOT}/ior"
      ./bootstrap
      ./configure MPICC=mpicc
      make -j"$(nproc)"
      install -m 0755 src/mdtest "${BENCH_BIN}/mdtest"
      ln -sf "${BENCH_BIN}/mdtest" /usr/local/bin/mdtest
    )
    rm -rf "${BUILD_ROOT}/ior"
  fi
fi

if [[ ! -x "${BENCH_BIN}/fsx" || ! -x "${BENCH_BIN}/fsstress" ]]; then
  log "Building xfstests-dev ltp/fsx and ltp/fsstress (${XFSTESTS_COMMIT})..."
  rm -rf "${BUILD_ROOT}/xfstests"
  mkdir -p "${BUILD_ROOT}/xfstests"
  (
    cd "${BUILD_ROOT}/xfstests"
    git init -q
    git remote add origin https://github.com/kdave/xfstests.git
    git fetch --depth 1 origin "${XFSTESTS_COMMIT}"
    git checkout -q FETCH_HEAD
    make configure >/dev/null 2>&1 || autoreconf -fiv
    ./configure >/dev/null
    make -C include -j"$(nproc)" >/dev/null 2>&1 || true
    make -C lib -j"$(nproc)" >/dev/null
    make -C ltp fsx fsstress -j"$(nproc)" >/dev/null
    install -m 0755 ltp/fsx "${BENCH_BIN}/fsx"
    install -m 0755 ltp/fsstress "${BENCH_BIN}/fsstress"
  )
  rm -rf "${BUILD_ROOT}/xfstests"
fi

if [[ ! -f "${BENCH_PREFIX}/smallfile/smallfile_cli.py" ]]; then
  log "Installing distributed-system-analysis/smallfile (${SMALLFILE_COMMIT})..."
  rm -rf "${BENCH_PREFIX}/smallfile"
  git clone --quiet https://github.com/distributed-system-analysis/smallfile.git "${BENCH_PREFIX}/smallfile"
  git -C "${BENCH_PREFIX}/smallfile" checkout -q "${SMALLFILE_COMMIT}"
  chmod +x "${BENCH_PREFIX}/smallfile/smallfile_cli.py"
fi

if [[ ! -x "${BENCH_BIN}/passthrough_ll" ]]; then
  log "Building libfuse passthrough_ll (${LIBFUSE_TAG})..."
  rm -rf "${BUILD_ROOT}/libfuse"
  git clone --depth 1 --branch "${LIBFUSE_TAG}" https://github.com/libfuse/libfuse.git "${BUILD_ROOT}/libfuse"
  (
    cd "${BUILD_ROOT}/libfuse"
    meson setup build -Dtests=false -Dutils=false -Dexamples=true >/dev/null
    ninja -C build example/passthrough_ll >/dev/null
    install -m 0755 build/example/passthrough_ll "${BENCH_BIN}/passthrough_ll"
  )
  rm -rf "${BUILD_ROOT}/libfuse"
fi

if [[ ! -x "${BENCH_PREFIX}/pjdfstest/pjdfstest" ]]; then
  log "Building pjd/pjdfstest..."
  rm -rf "${BENCH_PREFIX}/pjdfstest"
  git clone --depth 1 https://github.com/pjd/pjdfstest.git "${BENCH_PREFIX}/pjdfstest"
  (
    cd "${BENCH_PREFIX}/pjdfstest"
    autoreconf -ifs
    ./configure >/dev/null
    make -j"$(nproc)" >/dev/null
  )
  ln -sf "${BENCH_PREFIX}/pjdfstest/pjdfstest" "${BENCH_BIN}/pjdfstest"
  ln -sf "${BENCH_PREFIX}/pjdfstest/pjdfstest" /usr/local/bin/pjdfstest
fi

if [[ ! -f "${CORPUS_DIR}/manifest.sha256" ]]; then
  log "Generating deterministic offline workload corpus in ${CORPUS_DIR} (tar file size=${CORPUS_TAR_FILE_BYTES}B)..."
  CORPUS_DIR="${CORPUS_DIR}" CORPUS_TAR_FILE_BYTES="${CORPUS_TAR_FILE_BYTES}" python3 - <<'PY_CORPUS'
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

corpus_dir = Path(os.environ["CORPUS_DIR"])
tar_file_bytes = int(os.environ.get("CORPUS_TAR_FILE_BYTES", "30720"))
corpus_dir.mkdir(parents=True, exist_ok=True)

# 1. Deterministic offline Go repository (20 stdlib packages + unit tests)
go_work = corpus_dir / "go-repo"
go_bare = corpus_dir / "go-repo.git"
if go_work.exists():
    shutil.rmtree(go_work)
if go_bare.exists():
    shutil.rmtree(go_bare)

go_work.mkdir(parents=True)
(go_work / "go.mod").write_text("module bench.local/corpusapp\n\ngo 1.22\n", encoding="utf-8")

pkg_imports = []
pkg_calls = []
for i in range(1, 21):
    pkg = f"pkg{i:02d}"
    pdir = go_work / "internal" / pkg
    pdir.mkdir(parents=True)
    src = f"""package {pkg}

import (
\t"crypto/sha256"
\t"encoding/binary"
\t"fmt"
)

type Accumulator struct {{
\tSeed uint64
\tSum  uint64
}}

func Compute(n int) uint64 {{
\tvar acc Accumulator
\tacc.Seed = {i * 10007}
\tvar buf [8]byte
\tfor j := 0; j < n; j++ {{
\t\tbinary.LittleEndian.PutUint64(buf[:], acc.Seed+uint64(j*31+{i}))
\t\td := sha256.Sum256(buf[:])
\t\tacc.Sum ^= binary.LittleEndian.Uint64(d[:8])
\t}}
\treturn acc.Sum
}}

func Label(n int) string {{
\treturn fmt.Sprintf("{pkg}:%016x", Compute(n))
}}
"""
    test_src = f"""package {pkg}

import "testing"

func TestComputeDeterministic(t *testing.T) {{
\ta := Compute(64)
\tb := Compute(64)
\tif a != b {{
\t\tt.Fatalf("non-deterministic result: %x != %x", a, b)
\t}}
\tif Label(16) == "" {{
\t\tt.Fatal("empty label")
\t}}
}}
"""
    (pdir / f"{pkg}.go").write_text(src, encoding="utf-8")
    (pdir / f"{pkg}_test.go").write_text(test_src, encoding="utf-8")
    pkg_imports.append(f'\t"bench.local/corpusapp/internal/{pkg}"')
    pkg_calls.append(f'\tsum ^= {pkg}.Compute(32)')

cmd_dir = go_work / "cmd" / "corpusapp"
cmd_dir.mkdir(parents=True)
main_src = (
    'package main\n\nimport (\n\t"fmt"\n'
    + "\n".join(pkg_imports)
    + "\n)\n\nfunc main() {\n\tvar sum uint64\n"
    + "\n".join(pkg_calls)
    + '\n\tfmt.Printf("corpusapp:%016x\\n", sum)\n}\n'
)
(cmd_dir / "main.go").write_text(main_src, encoding="utf-8")

git_env = dict(os.environ)
git_env.update({
    "GIT_AUTHOR_NAME": "Bowl Bench",
    "GIT_AUTHOR_EMAIL": "bench@bowl.local",
    "GIT_COMMITTER_NAME": "Bowl Bench",
    "GIT_COMMITTER_EMAIL": "bench@bowl.local",
    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z",
    "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z",
})
subprocess.run(["git", "init", "-q", "-b", "main"], cwd=go_work, env=git_env, check=True)
subprocess.run(["git", "add", "."], cwd=go_work, env=git_env, check=True)
subprocess.run(["git", "commit", "-q", "-m", "Initial deterministic corpus"], cwd=go_work, env=git_env, check=True)
subprocess.run(["git", "clone", "--bare", "-q", str(go_work), str(go_bare)], env=git_env, check=True)
head_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=go_work, text=True).strip()

# 2. Deterministic 5,000-file tar archive with per-file SHA-256 manifest
tar_path = corpus_dir / "files_5k.tar"
files_manifest_path = corpus_dir / "files_5k.sha256"
num_dirs = 50
files_per_dir = 100
total_files = num_dirs * files_per_dir
total_bytes = 0
manifest_lines = []

# Pre-build a deterministic 64 KiB base pattern to slice quickly
base_block = bytearray()
counter = 0
while len(base_block) < max(tar_file_bytes + 4096, 65536):
    base_block.extend(hashlib.sha256(f"bowl-corpus-block-{counter}".encode()).digest())
    counter += 1
base_bytes = bytes(base_block)

with tarfile.open(tar_path, "w") as tf:
    for d in range(num_dirs):
        for f in range(files_per_dir):
            idx = d * files_per_dir + f
            rel = f"d{d:02d}/f{f:03d}.dat"
            header = f"BOWL_CORPUS_FILE:{idx:06d}\n".encode("ascii")
            offset = (idx * 37) % 2048
            payload_len = max(0, tar_file_bytes - len(header))
            content = header + base_bytes[offset:offset + payload_len]
            digest = hashlib.sha256(content).hexdigest()
            manifest_lines.append(f"{digest}  {rel}\n")
            info = tarfile.TarInfo(name=rel)
            info.size = len(content)
            info.mtime = 1767225600
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            tf.addfile(info, io.BytesIO(content))
            total_bytes += len(content)

files_manifest_path.write_text("".join(manifest_lines), encoding="utf-8")

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

tar_sha = sha256_file(tar_path)
files_manifest_sha = sha256_file(files_manifest_path)

(corpus_dir / "manifest.sha256").write_text(
    f"{tar_sha}  files_5k.tar\n{files_manifest_sha}  files_5k.sha256\n",
    encoding="utf-8",
)
(corpus_dir / "corpus_meta.json").write_text(
    json.dumps(
        {
            "go_repo_bare": str(go_bare),
            "go_repo_work": str(go_work),
            "go_repo_head": head_sha,
            "tar_archive": str(tar_path),
            "tar_sha256": tar_sha,
            "tar_file_count": total_files,
            "tar_total_payload_bytes": total_bytes,
            "files_sha256_manifest": str(files_manifest_path),
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY_CORPUS
fi

rm -rf "${BUILD_ROOT}"
log "Client bootstrap complete. Installed tools in ${BENCH_BIN} and corpus in ${CORPUS_DIR}."
