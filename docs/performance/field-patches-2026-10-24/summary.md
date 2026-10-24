# Field patch measurements

Each trial processes ten successive updates, including the first complete dictionary fill. Updates alternate status values, increase database revisions, and wait for the previous update to commit before the benchmark sends an ACK for the next update. This flow control is benchmark-only. Values are milliseconds for the complete ten-update stream, not single-update latency. Network counts exclude client ACKs, benchmark trailers, TCP/IP and TLS.

| Workload | Storage | Format | Trials | p50 ms | p95 ms | p99 ms | WS bytes | Decoder ms | Sampled peak MiB | Post-GC MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small-update | memory | no-old | 100 | 4.38 | 5.63 | 8.31 | 14,098 | 0.140 | 39.8 | 34.2 |
| small-update | memory | wire-patch | 100 | 4.38 | 5.33 | 5.80 | 9,694 | 0.380 | 40.1 | 34.3 |
| small-update | indexeddb | no-old | 100 | 31.11 | 34.79 | 38.12 | 14,098 | 0.175 | 93.4 | 35.1 |
| small-update | indexeddb | wire-patch | 100 | 34.48 | 39.63 | 41.82 | 9,694 | 0.480 | 93.3 | 35.1 |
| large-update | memory | no-old | 100 | 13.03 | 17.15 | 22.32 | 183,108 | 0.345 | 90.3 | 34.2 |
| large-update | memory | wire-patch | 100 | 7.88 | 13.80 | 17.65 | 26,533 | 0.650 | 73.9 | 34.3 |
| large-update | indexeddb | no-old | 100 | 53.11 | 148.00 | 156.41 | 183,108 | 0.405 | 115.0 | 35.1 |
| large-update | indexeddb | wire-patch | 100 | 42.07 | 54.01 | 77.75 | 26,533 | 0.635 | 102.1 | 35.1 |
| batch-100 | memory | no-old | 30 | 41.92 | n/a | n/a | 261,040 | 2.015 | 87.6 | 34.7 |
| batch-100 | memory | wire-patch | 30 | 40.76 | n/a | n/a | 63,447 | 9.250 | 93.6 | 34.7 |
| batch-100 | indexeddb | no-old | 16 | 455.94 | n/a | n/a | 261,040 | 2.065 | 88.6 | 34.9 |
| batch-100 | indexeddb | wire-patch | 16 | 523.89 | n/a | n/a | 63,447 | 11.135 | 87.3 | 35.0 |
| batch-1000 | memory | no-old | 10 | 476.61 | n/a | n/a | 2,504,111 | 15.870 | 90.9 | 34.6 |
| batch-1000 | memory | wire-patch | 10 | 422.16 | n/a | n/a | 565,050 | 86.795 | 116.2 | 34.7 |
| batch-1000 | indexeddb | no-old | 3 | 8377.32 | n/a | n/a | 2,504,111 | 16.860 | 86.3 | 34.8 |
| batch-1000 | indexeddb | wire-patch | 3 | 12806.38 | n/a | n/a | 565,050 | 88.125 | 83.5 | 34.8 |

Decoder time includes native JSON.parse, patch reconstruction, cache checks and freezing. Native browser DEFLATE inflation is included in overall latency but is not separately observed. Sampled peak heap is a lower bound. GC and renderer CPU totals in the raw data include untimed seeding and cleanup.

The 1,000-row IndexedDB medians had only three trials. The first run measured 24,014.80 to 12,200.66 ms; the final full run measured 8,377.32 to 12,806.38 ms. This reversal is why no broad durable-storage or large-batch latency win is claimed. Some checks/builds overlapped the full runs. See the separate large-row repeat after those builds.
