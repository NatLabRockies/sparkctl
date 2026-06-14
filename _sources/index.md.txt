# sparkctl

This package implements configuration and orchestration of Spark clusters with standalone cluster
managers. This is useful in environments like HPCs where the infrastructure implemented by cloud
providers, such as AWS, is not available. It is particularly helpful when users want to deploy
Spark but do not have administrative control of the servers.

## Features
- Controls startup and shutdown of Spark processes on compute nodes in a Slurm compute node
  allocation.
- Customizes the Spark configuration automatically based on the resources in the given computing
  environment. This includes executors per compute node, memory per executor, and Spark SQL shuffle
  partitions.
- Provides helper commands to customize Spark features and configuration parameters, such as
  shuffle storage, dynamic allocation, the Spark history server, Spark Connect server, Apache
  Thrift server, and a Hive metastore.
- Provides integration with resource monitoring tools to report CPU, memory, disk, and network
  utilization.

```{eval-rst}
.. toctree::
    :maxdepth: 4
    :caption: Contents:
    :hidden:

    how_tos/index
    tutorials/index
    reference/index
    explanation/index
    faq
```

## How to use this guide
- Refer to [How Tos](#how-tos) for step-by-step instructions for configuring a Spark cluster.
- Refer to [Tutorials](#tutorials) for examples of running Spark clusters in specific
  environments.
- Refer to [Reference](#reference) for CLI reference material.
- Refer to [Explanation](#explanation) for descriptions and behaviors of the software.
- Refer to [FAQ](#sparkctl-faqs) for answers to common questions.

# Indices and tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
