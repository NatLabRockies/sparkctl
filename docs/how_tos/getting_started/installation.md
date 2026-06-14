# Installation

1. Create a virtual environment with Python 3.11 or later. These examples create a virtual
   environment in your home directory.

   If you are running on an HPC, you may need to `module load python` first.

   ```{eval-rst}
   .. tabs::

      .. tab:: venv

         This uses the ``venv`` module in the standard library. You may prefer ``conda`` or
         ``mamba``.

         .. code-block:: console

            $ python -m venv ~/python-envs/sparkctl

      .. tab:: uv

         `uv <https://docs.astral.sh/uv/>`_ creates the environment quickly and can install the
         requested Python version for you.

         .. code-block:: console

            $ uv venv --python 3.11 ~/python-envs/sparkctl
   ```

2. Activate the virtual environment.

   ```console
   $ source ~/python-envs/sparkctl/bin/activate
   ```

   Whenever you are done using sparkctl, you can deactivate the environment by running `deactivate`.

3. Install the Python package `sparkctl`.

   If you will be using Spark Connect to run Spark jobs, the base installation is sufficient.

   ```{eval-rst}
   .. note:: This does not include `spark-submit` or `pyspark`.
   ```

   ```{eval-rst}
   .. tabs::

      .. tab:: pip

         .. code-block:: console

            $ pip install sparkctl

      .. tab:: uv

         .. code-block:: console

            $ uv pip install sparkctl
   ```

   If you will be running Spark jobs with `spark-submit` or `pyspark`, you will need to install
   the full `pyspark` package:

   ```{eval-rst}
   .. tabs::

      .. tab:: pip

         .. code-block:: console

            $ pip install "sparkctl[pyspark]"

      .. tab:: uv

         .. code-block:: console

            $ uv pip install "sparkctl[pyspark]"
   ```

   ```{eval-rst}
   .. tip::

      If you only need the ``sparkctl`` command-line tool (and not the Python API), you can install
      it as a standalone, isolated tool with uv. This does not require creating or activating a
      virtual environment:

      .. code-block:: console

         $ uv tool install sparkctl
   ```

4. Optional, install from the main branch (or substitute another branch or tag).

   ```{eval-rst}
   .. tabs::

      .. tab:: pip

         .. code-block:: console

            $ pip install git+https://github.com/NatLabRockies/sparkctl.git@main

      .. tab:: uv

         .. code-block:: console

            $ uv pip install git+https://github.com/NatLabRockies/sparkctl.git@main
   ```

5. Create a one-time sparkctl default configuration file. The parameters will vary based on your
   environment. If no one has deployed the required dependencies in your environment, please refer
   to {ref}`deploy-sparkctl`.

   ```bash
   $ sparkctl default-config \
       /datasets/images/apache_spark/spark-4.1.1-bin-hadoop3 \
       /datasets/images/apache_spark/jdk-21.0.7 \
       --compute-environment slurm
   ```
   ```console
   Wrote sparkctl settings to /Users/dthom/.sparkctl.toml
   ```
   Refer to `sparkctl default-config --help` for additional options.

   The paths to the Spark binaries will likely not change often. This file will also seed the default
   values for your `sparkctl configure` commands, and so you may want to manually edit those settings.
