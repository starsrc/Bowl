// ComputeSDK-shaped adapter over the bowl-sandbox HTTP API (bowl-sandbox serve).
// It covers what the sandbox benchmarks call: create, runCommand, destroy.

interface BowlOptions {
  /** Base URL of bowl-sandbox serve, e.g. http://127.0.0.1:7070 */
  url: string;
  apiKey?: string;
  /** Template every sandbox starts from. */
  template: string;
}

export function bowl({ url, apiKey, template }: BowlOptions) {
  const call = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(url.replace(/\/$/, '') + path, {
      method,
      headers: {
        'content-type': 'application/json',
        ...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`bowl-sandbox ${method} ${path}: ${res.status} ${text}`);
    return text ? JSON.parse(text) : {};
  };
  return {
    sandbox: {
      create: async (opts?: { template?: string; cpus?: number; memoryMiB?: number }) => {
        const info = await call('POST', '/sandboxes', {
          template: opts?.template ?? template,
          cpus: opts?.cpus,
          memory_mib: opts?.memoryMiB,
        });
        const id: string = info.sandbox_id;
        return {
          sandboxId: id,
          runCommand: async (cmd: string) => {
            const r = await call('POST', `/sandboxes/${id}/commands`, { cmd });
            return { exitCode: r.exit_code as number, stdout: r.stdout as string, stderr: r.stderr as string };
          },
          destroy: async () => {
            await call('DELETE', `/sandboxes/${id}`);
          },
        };
      },
    },
  };
}
