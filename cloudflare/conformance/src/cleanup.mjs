/**
 * Fixture project cleanup (persistent deployments).
 *
 * Every scenario creates its own fixture project. A local backend throws them away with its process, but a
 * deployed backend keeps them: their tenants keep the deterministic `<scenario>-<name>` ids that the next run
 * needs, and every project with an active release is baked into the serving Worker's catalog, whose startup
 * time grows with it. After a scenario's trace is complete, the CLI deletes the projects the run created
 * through the backend's ordinary dev delete path (`DELETE /dev/projects/{id}`, which also deletes the
 * project's runtime-created tenants).
 *
 * Cleanup never changes a scenario's outcome and is never part of the comparison. It is skipped, and
 * reported, while the backend reports an open deployment fence, so a staged publication is never orphaned.
 * `GONVEX_CONFORMANCE_KEEP_PROJECTS=1` keeps every project for debugging.
 */

/**
 * @param {{ name: string, deleteProject?: (project: string) => Promise<unknown>, openDeployment?: () => Promise<unknown> }} backend
 * @param {string[]} created project ids this run created on `backend`, in creation order
 * @param {{ keep?: boolean }} [options]
 * @returns {Promise<Array<{ project: string, outcome: 'deleted' | 'kept' | 'failed', reason?: string }>>}
 */
export async function cleanupProjects(backend, created, { keep = process.env.GONVEX_CONFORMANCE_KEEP_PROJECTS === '1' } = {}) {
  if (!created.length || typeof backend.deleteProject !== 'function') return [];
  if (keep) return created.map(project => ({ project, outcome: 'kept', reason: 'GONVEX_CONFORMANCE_KEEP_PROJECTS=1' }));
  let open = null;
  try { open = await backend.openDeployment?.() ?? null; }
  catch (error) { return created.map(project => ({ project, outcome: 'kept', reason: `deployment state unreadable: ${error?.message ?? error}` })); }
  if (open) return created.map(project => ({ project, outcome: 'kept', reason: `deployment ${open.id ?? 'unknown'} is still open` }));
  const results = [];
  for (const project of created) {
    try { await backend.deleteProject(project); results.push({ project, outcome: 'deleted' }); }
    catch (error) { results.push({ project, outcome: 'failed', reason: String(error?.message ?? error).slice(0, 300) }); }
  }
  return results;
}
