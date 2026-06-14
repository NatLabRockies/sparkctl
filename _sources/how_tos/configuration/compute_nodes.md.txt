# How to Select Compute Nodes

Selecting the right compute nodes is critical for Spark cluster performance, especially for jobs
that perform large shuffles (joins, aggregations, sorting).

## Storage Speed Requirements

Spark shuffle operations write intermediate data to local storage. If this storage is slow,
shuffles become a bottleneck. The minimum requirement is approximately **500 MB/s** sequential
write speed. Drives that can deliver **2 GB/s** are ideal.

## Storage Options (Best to Worst)

| Storage Type | Speed | Capacity | Notes |
|--------------|-------|----------|-------|
| NVMe SSD | 2-7 GB/s | 1-10 TB | Best choice for shuffle-heavy workloads |
| RAM disk (`/dev/shm`) | 10+ GB/s | Limited by RAM | Excellent speed but limited capacity |
| HPC shared filesystem (Lustre, GPFS) | 500 MB/s - 2 GB/s | Unlimited | Shared bandwidth; slower than local SSD |
| Spinning disk (HDD) | 100-200 MB/s | Large | Too slow for shuffle-heavy workloads |

## Recommendations by Workload

### Shuffle-Heavy Workloads (Joins, Group-Bys)
- Prefer nodes with local NVMe storage
- Configure sparkctl to use the NVMe path for shuffle storage
- Allocate more nodes if shuffle data exceeds local SSD capacity

### Read-Heavy Workloads (Scans, Filters)
- Storage speed is less critical
- Focus on memory capacity and network bandwidth
- Shared filesystem storage is often acceptable

### Small Data / Prototyping
- RAM disk (`/dev/shm`) works well
- Many HPC compute nodes have `/dev/shm` available (typically half of RAM)
- Fastest option but limited by memory

## Configuring Shuffle Storage Location

Specify local storage for shuffle writes (for systems where sparkctl knows how to identify
local storage such as NLR's Kestrel cluster):

```console
$ sparkctl configure --local-storage
```

Or for RAM disk (or other custom location accessible on each compute node):

```console
$ sparkctl configure --spark-scratch /dev/shm
```

## Capacity Planning

If your shuffle data exceeds local storage capacity, you have two options:

1. **Add more nodes**: Distributes shuffle data across more local SSDs
2. **Use shared filesystem**: Configure `--spark-scratch` to point to Lustre/GPFS (slower but unlimited capacity)

## Checking Node Storage

On most HPCs, you can check available local storage during an interactive session:

```console
$ df -h /dev/nvme0
$ df -h /dev/shm
```

Consult your HPC documentation for node-specific storage configurations.

## Network Considerations

Shuffle operations also transfer data between nodes. Ensure your nodes have:
- High-bandwidth, low-latency interconnect
- Nodes in the same rack or network segment if possible
