# Real cloud path measurements (2026-09-05, from cluster04)

Anonymous HTTPS only. No credentials, no requester-pays buckets, nothing billable.

## TCP handshake RTT to cloud storage endpoints

| endpoint | region | RTT |
|---|---|---|
| s3.ap-northeast-2 | Seoul | 4.9 ms |
| s3.ap-northeast-1 | Tokyo | 30.1 ms |
| s3.us-west-2 | Oregon | 118.0 ms |
| s3.us-east-1 | N. Virginia | 171.7 ms |
| s3.eu-west-1 | Ireland | 261.4 ms |
| s3.sa-east-1 | Sao Paulo | 301.2 ms |
| storage.googleapis.com | GCS front end | 2.5 ms |

## Achieved goodput, public objects

| object | path | goodput |
|---|---|---|
| NYC TLC yellow_tripdata_2024-01.parquet (47.6 MiB) | CloudFront edge | 93.6 MB/s |
| gharchive 2024-01-01-0.json.gz (71.1 MiB) | CDN | 73.5 MB/s |
| GCS gcp-public-data-landsat index (100 MiB range) | US origin, direct | 28.5 MB/s |
| Ookla open data, s3.us-west-2 (100 MiB range) | Oregon origin, direct | 20.5 MB/s |

## How the emulated conditions map onto this

| emulated condition | measured goodput | closest real path |
|---|---|---|
| 10 GbE unshaped | 888 MB/s | same-datacenter; above anything wide-area |
| 1 Gbit + 10 ms | 118 MB/s | Seoul-region object store (RTT 4.9 ms) |
| 1 Gbit + 50 ms | 91.3 MB/s | CDN-fronted pull, measured 93.6 MB/s |
| 500 Mbit + 50 ms | 58.5 MB/s | between CDN and direct origin |
| 1 Gbit + 150 ms | 33.0 MB/s | direct US origin, measured 20.5-28.5 MB/s at 118-172 ms |
| 100 Mbit + 50 ms | 11.9 MB/s | constrained or shared link; below observed range |

The sweep brackets the measured range on both sides.

## The observation worth putting in the paper

The same object served through a CDN edge runs 3-4x faster than pulled from its
origin region (93.6 vs 20.5-28.5 MB/s), but **egress is billed per byte either
way**. So in production, exactly as the paper argues, the latency-optimal
replication threshold moves with the path while the egress-optimal one does
not. Our central claim is not an artefact of emulation; it is visible in the
public cloud with no account at all.
