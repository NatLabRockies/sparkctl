from sparkctl.compute_interface import ComputeInterface
from sparkctl.fake_compute import FakeCompute
from sparkctl.models import ComputeEnvironment, SparkConfig
from sparkctl.native_compute import NativeCompute
from sparkctl.slurm_compute import SlurmCompute


def make_compute_interface(config: SparkConfig) -> ComputeInterface:
    """Return a compute interface appropriate for teh current environment."""
    match config.compute.environment:
        case ComputeEnvironment.SLURM:
            return SlurmCompute(config)
        case ComputeEnvironment.NATIVE:
            return NativeCompute(config)
        case ComputeEnvironment.FAKE:
            return FakeCompute(config)
        case _:
            msg = f"{config.environment=} is not supported"
            raise NotImplementedError(msg)
