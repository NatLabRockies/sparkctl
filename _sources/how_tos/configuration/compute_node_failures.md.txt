(compute-node-failures)=
# How to handle compute node failures
If your job uses multiple compute nodes, you may want to consider setting the flag `--no-kill` in
your `sbatch` command.

By default, if a compute node fails (kernel panic, hardware failure, etc.), Slurm will terminate
the job.

This behavior can be advantageous for a Spark cluster because it can tolerate worker node
failures in many cases. If a job is almost complete when a failure occurs, it may be able to retry
the failed tasks on a different node and complete successfully.

On the other hand, if the remaining nodes will not be able to complete the job, the default
behavior is better.

The more nodes you have, the more likely it is to be beneficial to set this option. If you allocate
two nodes, you probably don't want to set it. If you allocate eight nodes, it may be good to set
it.

Recommendation: only set this option in conjunction with {ref}`heterogeneous-slurm-jobs`.
The Spark cluster master created by these scripts is not redundant. If it fails, the job
will never complete. Hopefully, since it is doing far less work, it will be less likely to fail.
However, if you are using a shared node, you obviously cannot control what others are doing.
