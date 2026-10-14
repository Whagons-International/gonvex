export function stats(values) {
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const sd = Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / (values.length - 1));
  // Student t, 29 df for 30 samples. Larger n uses the conservative same value.
  const t = values.length >= 30 ? 2.04523 : values.length >= 5 ? 2.77645 : 4.30265;
  const margin = t * sd / Math.sqrt(values.length);
  return { n: values.length, mean, ci95: [mean - margin, mean + margin], sd };
}
export function ratioCI(plain, compressed) {
  let seed = 417;
  const values = [];
  for (let trial = 0; trial < 10000; trial++) {
    let a = 0, b = 0;
    for (let i = 0; i < plain.length; i++) {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      const index = Math.floor(seed / 0x100000000 * plain.length);
      a += plain[index]; b += compressed[index];
    }
    values.push(100 * (b / a - 1));
  }
  values.sort((a, b) => a - b);
  return { mean: 100 * (stats(compressed).mean / stats(plain).mean - 1), ci95: [values[250], values[9750]], method: 'paired percentile bootstrap, 10000 resamples, seed 417' };
}
