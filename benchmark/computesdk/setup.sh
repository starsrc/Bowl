#!/usr/bin/env bash
# Clone computesdk/benchmarks into $1 and register bowl-sandbox as a provider.
set -euo pipefail
dest=${1:?usage: setup.sh <dir>}
here=$(cd "$(dirname "$0")" && pwd)
git clone --depth 1 https://github.com/computesdk/benchmarks.git "$dest"
cp "$here/bowl.ts" "$dest/benchmarks/sandbox/bowl.ts"
cd "$dest"
python3 - <<'PY'
p = 'benchmarks/sandbox/providers.ts'
s = open(p).read()
s = s.replace("import type { ProviderConfig } from './types.js';",
              "import type { ProviderConfig } from './types.js';\nimport { bowl } from './bowl.js';", 1)
marker = 'export const providers: ProviderConfig[] = [\n'
entry = """  {
    name: 'bowl',
    requiredEnvVars: ['BOWL_SANDBOX_URL'],
    createCompute: () => bowl({
      url: process.env.BOWL_SANDBOX_URL!,
      apiKey: process.env.BOWL_SANDBOX_API_KEY,
      template: process.env.BOWL_TEMPLATE ?? 'node:22-slim',
    }),
  },
"""
assert marker in s
open(p, 'w').write(s.replace(marker, marker + entry, 1))
PY
pnpm install --frozen-lockfile --ignore-scripts
pnpm -r --filter './packages/**' run build
