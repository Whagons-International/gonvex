export function selectReleaseVersion({ latestTag = "", packageVersions, requestedVersion = "" }) {
  if (!Array.isArray(packageVersions) || packageVersions.length === 0) {
    throw new Error("At least one package version is required to select a release version.");
  }

  for (const version of packageVersions) validateVersion(version);

  const highestPackageVersion = maxVersion(packageVersions);
  const tagVersion = latestTag ? latestTag.replace(/^v/, "") : "";
  if (tagVersion) validateVersion(tagVersion);

  const baselineVersion = maxVersion([highestPackageVersion, ...(tagVersion ? [tagVersion] : [])]);
  const version = requestedVersion || bumpPatch(baselineVersion);
  validateVersion(version);

  if (compareVersions(version, baselineVersion) <= 0) {
    throw new Error(
      `Release version ${version} must be greater than the current release baseline ${baselineVersion} ` +
        `(highest package ${highestPackageVersion}${tagVersion ? `, latest tag ${latestTag}` : ""}).`,
    );
  }

  return { baselineVersion, highestPackageVersion, version };
}

export function bumpPatch(version) {
  validateVersion(version);
  const parts = version.split("-", 1)[0].split(".").map(Number);
  if (version.includes("-")) return parts.join(".");
  parts[2] += 1;
  return parts.join(".");
}

export function compareVersions(a, b) {
  validateVersion(a.replace(/^v/, ""));
  validateVersion(b.replace(/^v/, ""));
  const [leftCore, leftPre] = a.replace(/^v/, "").split(/-(.*)/s);
  const [rightCore, rightPre] = b.replace(/^v/, "").split(/-(.*)/s);
  const left = leftCore.split(".").map(Number);
  const right = rightCore.split(".").map(Number);
  for (let index = 0; index < 3; index += 1) {
    if (left[index] !== right[index]) return left[index] - right[index];
  }
  if (leftPre === rightPre) return 0;
  if (leftPre === undefined) return 1;
  if (rightPre === undefined) return -1;
  const leftIds = leftPre.split(".");
  const rightIds = rightPre.split(".");
  for (let index = 0; index < Math.max(leftIds.length, rightIds.length); index += 1) {
    const l = leftIds[index], r = rightIds[index];
    if (l === r) continue;
    if (l === undefined) return -1;
    if (r === undefined) return 1;
    const ln = /^\d+$/.test(l), rn = /^\d+$/.test(r);
    if (ln && rn) return BigInt(l) < BigInt(r) ? -1 : 1;
    if (ln !== rn) return ln ? -1 : 1;
    return l < r ? -1 : 1;
  }
  return 0;
}

export function validateVersion(version) {
  if (!/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/.test(version)
      || version.split(/-(.*)/s)[1]?.split('.').some(id => /^0\d+$/.test(id))) {
    throw new Error(`Invalid semver version: ${version}`);
  }
}

function maxVersion(versions) {
  return [...versions].sort(compareVersions).at(-1);
}
