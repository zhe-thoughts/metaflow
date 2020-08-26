import os
import sys
import platform
import re
import tarfile

from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.datastore import TransformableObject
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.metadata import MetaDatum

from metaflow import util
from metaflow import R

try:
    # python2
    from urlparse import urlparse
except:  # noqa E722
    # python3
    from urllib.parse import urlparse


class RayResourcesDecorator(StepDecorator):
    """
    Step decorator to specify the resources needed when executing this step.
    This decorator passes this information along to Batch when requesting resources
    to execute this step.
    This decorator is ignored if the execution of the step does not happen on Batch.
    To use, annotate your step as follows:
    ```
    @resources(cpu=32)
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1
    gpu : int
        Number of GPUs required for this step. Defaults to 0
    memory : int
        Memory size (in MB) required for this step. Defaults to 4000
    """
    name = 'rayresources'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4000',
    }

class RayDecorator(StepDecorator):
    """
    Step decorator to specify that this step should execute on Batch.
    This decorator indicates that your step should execute on Batch. Note that you can
    apply this decorator automatically to all steps using the ```--with batch``` argument
    when calling run. Step level decorators are overrides and will force a step to execute
    on Batch regardless of the ```--with``` specification.
    To use, annotate your step as follows:
    ```
    @batch
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1. If @resources is also
        present, the maximum value from all decorators is used
    gpu : int
        Number of GPUs required for this step. Defaults to 0. If @resources is also
        present, the maximum value from all decorators is used
    memory : int
        Memory size (in MB) required for this step. Defaults to 4000. If @resources is
        also present, the maximum value from all decorators is used
    image : string
        Image to use when launching on Batch. If not specified, a default image mapping to
        the current version of Python is used
    queue : string
        Queue to submit the job to. Defaults to the one determined by the environment variable
        METAFLOW_BATCH_JOB_QUEUE
    iam_role : string
        IAM role that Batch can use to access S3. Defaults to the one determined by the environment
        variable METAFLOW_ECS_S3_ACCESS_IAM_ROLE
    """
    name = 'ray'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4000',
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(RayhDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        self.logger = logger
        self.environment = environment
        self.step = step
        print('At Ray step!')
