// SDK-only allowance; the separate Whagons UI guard uses a 200 MB tab cap.
export const budget = Object.freeze({ privateMB: 120, retainedGrowthMiB: 4 });

export function assertBudget(summary) {
  if (![summary.peakPrivateMB, summary.peakRssMB, summary.retainedGrowthMiB, summary.activeMs, summary.taskCpuSeconds].every(Number.isFinite)
    || summary.peakPrivateMB <= 0 || summary.peakRssMB <= 0 || summary.taskCpuSeconds < 0 || !Array.isArray(summary.errors)) throw new Error('Incomplete memory measurements');
  if (summary.tasks !== 10_000 || summary.workspaces !== 5 || summary.visits?.length !== 10
    || summary.visits.some((visit, index) => visit.workspace !== (index + 1) % 5 || !Number.isInteger(visit.steps) || visit.steps < 3 || visit.residentRows !== 200 || visit.metadataIds !== 2000 || visit.queryRows !== 200)
    || summary.activeMs < 20_000 || summary.activeMs > 25_000) throw new Error('Incomplete or overlong memory workload');
  if (summary.errors?.length) throw new Error(summary.errors.join('\n'));
  if (summary.peakPrivateMB > budget.privateMB || summary.retainedGrowthMiB > budget.retainedGrowthMiB) throw new Error('SDK memory budget exceeded');
}
