# Browser transport and storage measurements

Synthetic single-client loopback trials on a shared workstation. SQL, authentication, SDK dispatch, digest verification and TLS/WAN are excluded. Native PMD decompression is included in latency; explicit decoder time cannot separate it from browser internals. The TCP proxy adds measurement overhead. Samples are sequential, not a saturation throughput test.

Small/100-row updates have 100 samples. The expensive 1,000-row durable cases have 30 samples; memory cases retained their original 100. Memory snapshots have eight samples; the expensive IndexedDB snapshots have three. They span runs, so only their medians are shown. p99 is omitted below 100 samples. Host load and GC/storage variability prevent treating these numbers as production latency estimates. The network byte totals are verified against actual TCP frames.

Storage: memory

| Workload | Variant | Codec/compression | WS bytes | p50 ms | p95 ms | p99 ms | Serial rows/s | Sampled peak MiB | Post-GC MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small-update | baseline | json/deflate1-1024 | 1,797 | 0.73 | 0.85 | 1.37 | 1347 | 37.0 | 32.4 |
| small-update | no-old | json/deflate1-1024 | 1,402 | 0.62 | 0.77 | 1.37 | 1572 | 36.9 | 32.4 |
| small-update | patch | json/deflate1-1024 | 758 | 0.58 | 0.71 | 1.30 | 1669 | 36.8 | 32.4 |
| small-update | baseline | msgpack/deflate1-1024 | 1,500 | 0.75 | 0.96 | 1.42 | 1307 | 37.1 | 32.4 |
| small-update | no-old | msgpack/deflate1-1024 | 1,163 | 0.59 | 0.72 | 1.11 | 1644 | 37.2 | 32.4 |
| small-update | baseline | json/zstd3 | 969 | 0.67 | 1.11 | 1.48 | 1388 | 37.4 | 32.5 |
| small-update | baseline | json/brotli4 | 902 | 1.36 | 1.91 | 2.09 | 713 | 118.5 | 32.5 |
| large-update | baseline | json/deflate1-1024 | 27,059 | 1.61 | 2.21 | 9.77 | 526 | 44.4 | 33.4 |
| large-update | no-old | json/deflate1-1024 | 18,292 | 1.21 | 1.81 | 1.96 | 772 | 43.3 | 33.4 |
| large-update | patch | json/deflate1-1024 | 758 | 0.62 | 1.22 | 1.50 | 1313 | 40.9 | 33.4 |
| large-update | baseline | msgpack/deflate1-1024 | 26,846 | 1.12 | 1.59 | 1.98 | 853 | 43.8 | 33.4 |
| large-update | no-old | msgpack/deflate1-1024 | 18,122 | 0.89 | 1.41 | 1.47 | 1039 | 42.4 | 33.4 |
| large-update | baseline | json/zstd3 | 12,842 | 1.09 | 1.58 | 1.92 | 833 | 46.5 | 33.4 |
| large-update | baseline | json/brotli4 | 12,826 | 2.28 | 3.73 | 7.50 | 382 | 137.6 | 33.4 |
| batch-100 | baseline | json/deflate1-1024 | 27,912 | 6.00 | 9.27 | 16.20 | 15666 | 56.3 | 33.6 |
| batch-100 | no-old | json/deflate1-1024 | 25,903 | 4.54 | 5.98 | 7.39 | 21596 | 60.1 | 33.7 |
| batch-100 | patch | json/deflate1-1024 | 1,472 | 2.81 | 4.46 | 5.10 | 33601 | 54.6 | 33.6 |
| batch-100 | baseline | msgpack/deflate1-1024 | 26,538 | 4.55 | 6.28 | 9.93 | 20645 | 67.7 | 33.6 |
| batch-100 | no-old | msgpack/deflate1-1024 | 24,763 | 3.30 | 5.66 | 9.33 | 27077 | 67.7 | 33.6 |
| batch-100 | baseline | json/zstd3 | 17,854 | 3.14 | 5.00 | 8.86 | 28934 | 62.6 | 33.6 |
| batch-100 | baseline | json/brotli4 | 17,779 | 6.48 | 8.18 | 16.60 | 14627 | 145.6 | 33.7 |
| batch-1000 | baseline | json/deflate1-1024 | 269,109 | 47.94 | 61.87 | 72.84 | 20088 | 108.1 | 34.4 |
| batch-1000 | no-old | json/deflate1-1024 | 248,778 | 37.48 | 49.02 | 54.79 | 26407 | 107.2 | 34.4 |
| batch-1000 | patch | json/deflate1-1024 | 9,077 | 20.80 | 25.89 | 31.25 | 47141 | 120.7 | 35.4 |
| batch-1000 | baseline | msgpack/deflate1-1024 | 256,268 | 36.53 | 48.73 | 56.33 | 26477 | 149.8 | 35.4 |
| batch-1000 | no-old | msgpack/deflate1-1024 | 238,382 | 28.73 | 41.26 | 48.26 | 33524 | 141.5 | 35.4 |
| batch-1000 | baseline | json/zstd3 | 168,509 | 28.17 | 36.08 | 41.01 | 34532 | 148.9 | 35.4 |
| batch-1000 | baseline | json/brotli4 | 167,918 | 40.41 | 48.96 | 51.80 | 24408 | 195.8 | 35.4 |
| snapshot-10000 | baseline | json/deflate1-1024 | 1,207,205 | 414.86 | n/a | n/a | 24865 | 73.5 | 35.5 |
| snapshot-10000 | paged | json/deflate1-1024 | 1,215,868 | 412.84 | n/a | n/a | 18556 | 93.7 | 35.5 |
| snapshot-10000 | canonical | json/deflate1-1024 | 1,259,806 | 182.56 | n/a | n/a | 53287 | 119.6 | 35.1 |
| snapshot-10000 | baseline | msgpack/deflate1-1024 | 1,157,815 | 186.49 | n/a | n/a | 52480 | 135.6 | 35.1 |
| snapshot-10000 | paged | msgpack/deflate1-1024 | 1,165,020 | 138.90 | n/a | n/a | 66192 | 147.7 | 35.1 |
| snapshot-10000 | baseline | json/zstd3 | 764,647 | 138.67 | n/a | n/a | 71198 | 145.0 | 35.2 |
| snapshot-10000 | baseline | json/brotli4 | 760,741 | 177.87 | n/a | n/a | 54939 | 125.5 | 35.2 |

Storage: indexeddb

| Workload | Variant | Codec/compression | WS bytes | p50 ms | p95 ms | p99 ms | Serial rows/s | Sampled peak MiB | Post-GC MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small-update | baseline | json/deflate1-1024 | 1,797 | 4.72 | 6.24 | 7.09 | 205 | 55.9 | 35.0 |
| small-update | no-old | json/deflate1-1024 | 1,402 | 4.81 | 6.03 | 7.57 | 204 | 56.0 | 35.0 |
| small-update | patch | json/deflate1-1024 | 758 | 4.79 | 7.48 | 9.28 | 198 | 56.2 | 35.0 |
| small-update | baseline | msgpack/deflate1-1024 | 1,500 | 4.40 | 9.85 | 12.26 | 197 | 56.2 | 35.0 |
| small-update | no-old | msgpack/deflate1-1024 | 1,163 | 4.61 | 6.48 | 7.88 | 207 | 56.3 | 35.0 |
| small-update | baseline | json/zstd3 | 969 | 4.70 | 6.82 | 8.08 | 203 | 56.2 | 35.1 |
| small-update | baseline | json/brotli4 | 902 | 5.23 | 9.00 | 11.67 | 178 | 121.7 | 35.0 |
| large-update | baseline | json/deflate1-1024 | 27,059 | 6.23 | 8.53 | 11.30 | 153 | 79.4 | 35.0 |
| large-update | no-old | json/deflate1-1024 | 18,292 | 5.63 | 6.90 | 10.50 | 172 | 78.0 | 35.0 |
| large-update | patch | json/deflate1-1024 | 758 | 4.94 | 6.75 | 7.99 | 193 | 77.6 | 35.0 |
| large-update | baseline | msgpack/deflate1-1024 | 26,846 | 5.60 | 7.37 | 8.94 | 172 | 83.6 | 35.1 |
| large-update | no-old | msgpack/deflate1-1024 | 18,122 | 5.43 | 7.80 | 8.88 | 173 | 82.2 | 35.1 |
| large-update | baseline | json/zstd3 | 12,842 | 6.48 | 10.78 | 12.43 | 147 | 81.4 | 35.1 |
| large-update | baseline | json/brotli4 | 12,826 | 8.47 | 10.72 | 11.67 | 117 | 123.4 | 35.0 |
| batch-100 | baseline | json/deflate1-1024 | 27,912 | 84.71 | 116.33 | 153.03 | 1134 | 86.6 | 35.0 |
| batch-100 | no-old | json/deflate1-1024 | 25,903 | 213.35 | 417.43 | 469.06 | 455 | 89.9 | 35.0 |
| batch-100 | patch | json/deflate1-1024 | 1,472 | 72.63 | 86.50 | 156.63 | 1304 | 89.6 | 35.1 |
| batch-100 | baseline | msgpack/deflate1-1024 | 26,538 | 79.61 | 106.76 | 203.34 | 1151 | 90.5 | 35.0 |
| batch-100 | no-old | msgpack/deflate1-1024 | 24,763 | 74.03 | 93.25 | 133.95 | 1280 | 89.5 | 35.0 |
| batch-100 | baseline | json/zstd3 | 17,854 | 72.64 | 102.58 | 145.55 | 1293 | 91.9 | 35.1 |
| batch-100 | baseline | json/brotli4 | 17,779 | 142.63 | 437.35 | 476.77 | 540 | 124.1 | 35.0 |
| batch-1000 | baseline | json/deflate1-1024 | 269,109 | 1035.28 | 1813.66 | n/a | 919 | 78.6 | 35.0 |
| batch-1000 | no-old | json/deflate1-1024 | 248,778 | 1281.99 | 6688.65 | n/a | 460 | 80.3 | 35.0 |
| batch-1000 | patch | json/deflate1-1024 | 9,077 | 2220.76 | 4762.41 | n/a | 384 | 78.7 | 35.0 |
| batch-1000 | baseline | msgpack/deflate1-1024 | 256,268 | 1068.83 | 1223.99 | n/a | 915 | 77.5 | 35.1 |
| batch-1000 | no-old | msgpack/deflate1-1024 | 238,382 | 1100.80 | 2306.88 | n/a | 717 | 102.6 | 35.1 |
| batch-1000 | baseline | json/zstd3 | 168,509 | 2333.23 | 7714.31 | n/a | 305 | 87.8 | 35.1 |
| batch-1000 | baseline | json/brotli4 | 167,918 | 1010.24 | 1854.05 | n/a | 945 | 79.6 | 35.1 |
| snapshot-10000 | baseline | json/deflate1-1024 | 1,207,205 | 49316.91 | n/a | n/a | 211 | 59.1 | 34.7 |
| snapshot-10000 | paged | json/deflate1-1024 | 1,215,868 | 41985.17 | n/a | n/a | 235 | 61.9 | 34.7 |
| snapshot-10000 | canonical | json/deflate1-1024 | 1,259,806 | 48648.01 | n/a | n/a | 233 | 80.0 | 34.7 |
| snapshot-10000 | baseline | msgpack/deflate1-1024 | 1,157,815 | 37908.49 | n/a | n/a | 263 | 63.0 | 34.7 |
| snapshot-10000 | paged | msgpack/deflate1-1024 | 1,165,020 | 41276.75 | n/a | n/a | 245 | 82.6 | 34.8 |
| snapshot-10000 | baseline | json/zstd3 | 764,647 | 39121.84 | n/a | n/a | 272 | 66.0 | 34.7 |
| snapshot-10000 | baseline | json/brotli4 | 760,741 | 40961.33 | n/a | n/a | 228 | 59.8 | 34.7 |

Peak heap is sampled at message boundaries and is a lower bound. Post-GC heap includes the retained fixture and benchmark code. Task/GC totals in browser.json include untimed seeding and database cleanup. Codec/compression costs interact with buffering, allocation and storage; isolated decoder speed is not an end-to-end ranking.
