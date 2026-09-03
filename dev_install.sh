#!/usr/bin/env bash
# Reproducible local install for domainhunter.
#
# Why this exists:
#   setuptools >= 64 generates the editable marker file
#       __editable__.<name>-<version>.pth
#   Python 3.14's site.py calls lstat() and skips any .pth file with the
#   macOS UF_HIDDEN flag (or the Windows hidden bit). Both `pip install -e .`
#   and `uv pip install -e .` produce .pth files that carry UF_HIDDEN on
#   macOS, so the editable install silently fails and `import domainhunter`
#   raises ModuleNotFoundError.
#
# This script runs the editable install, clears UF_HIDDEN from every .pth
# file in site-packages, writes a companion `domainhunter.pth` that points
# at <repo>/src, and patches the generated `domainhunter` console script to
# re-strip UF_HIDDEN at every invocation. The on-invocation patch matters:
# we observed macOS Spotlight re-applying UF_HIDDEN to freshly created .pth
# files within a second of being cleared, so a single install-time fix is
# not durable.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PY="${VENV_PY:-$REPO_ROOT/.venv/bin/python}"
SITE_PACKAGES="$("$VENV_PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
PTH_NAME="domainhunter.pth"

if [ ! -x "$VENV_PY" ]; then
    echo "error: $VENV_PY is not executable; create the venv first (python3 -m venv .venv)" >&2
    exit 1
fi

if command -v uv >/dev/null 2>&1; then
    uv pip install --python "$VENV_PY" -e "$REPO_ROOT"
else
    "$VENV_PY" -m pip install -e "$REPO_ROOT"
fi

# Strip macOS UF_HIDDEN from every .pth file pip / uv just touched.
if [ "$(uname -s)" = "Darwin" ]; then
    for pth in "$SITE_PACKAGES"/*.pth; do
        [ -f "$pth" ] || continue
        chflags nohidden "$pth" 2>/dev/null || true
    done
fi

printf '%s\n' "$REPO_ROOT/src" > "$SITE_PACKAGES/$PTH_NAME"
if [ "$(uname -s)" = "Darwin" ]; then
    chflags nohidden "$SITE_PACKAGES/$PTH_NAME" 2>/dev/null || true
fi

# Patch the generated console-script entry point so the package is
# importable even if macOS re-applies UF_HIDDEN to .pth files after the
# install. The patch rewrites the shebang to call a small bash shim that
# clears UF_HIDDEN on every .pth in site-packages before exec'ing the real
# Python interpreter. We cannot do the chflags inside Python itself because
# site.py caches sys.path at startup, before any user code runs.
if [ "$(uname -s)" = "Darwin" ]; then
    ENTRY="$REPO_ROOT/.venv/bin/domainhunter"
    SHIM="$REPO_ROOT/.venv/bin/_domainhunter_shim.sh"
    if [ -f "$ENTRY" ]; then
        # 1. Write a tiny shim that chflags every .pth in purelib then invokes
        #    the package CLI. The shim is itself chmod +x.
        SIBLING_PY="$(dirname "$ENTRY")/python"
        cat > "$SHIM" <<EOF
#!/usr/bin/env bash
# domainhunter-install-fix: strip UF_HIDDEN before Python starts so site.py
# can read the .pth files. macOS Spotlight re-applies UF_HIDDEN within
# seconds, so this must run on every invocation, not just at install.
set -e
SITE_PACKAGES="\$("$SIBLING_PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
if [ -n "\$SITE_PACKAGES" ] && [ -d "\$SITE_PACKAGES" ]; then
    for pth in "\$SITE_PACKAGES"/*.pth; do
        [ -f "\$pth" ] || continue
        chflags nohidden "\$pth" 2>/dev/null || true
    done
fi
exec "$SIBLING_PY" -c 'from domainhunter.cli import main; raise SystemExit(main())' "\$@"
EOF
        chmod +x "$SHIM"
        # 2. Replace the domainhunter entry's shebang with the shim.
        #    The real Python interpreter is still passed via the env so the
        #    shim's `exec` reaches it without re-resolving PATH.
        python3 - "$ENTRY" "$SHIM" <<'PYEOF'
import sys, os, shlex
entry, shim = sys.argv[1], sys.argv[2]
src = open(entry).read()
prefix = "#!/usr/bin/env bash\n# domainhunter-install-fix\n"
if src.startswith(prefix):
    rest = src.split("\n", 3)[3]
else:
    rest = "".join(src.splitlines(keepends=True)[1:])
quoted_shim = shlex.quote(shim)
open(entry, "w").write(f"#!/usr/bin/env bash\n# domainhunter-install-fix\nexec {quoted_shim} \"$@\"\n" + rest)
os.chmod(entry, 0o755)
PYEOF
    fi
fi

"$VENV_PY" -c 'import domainhunter; print("ok:", domainhunter.__file__)'
