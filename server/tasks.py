import subprocess

from redis import Redis
from rq import Queue
from rq.job import Job
from rq.decorators import job

REDIS_CONN = Redis(host='localhost', port='6379')
QUEUE = Queue(connection=REDIS_CONN)

def get_job_result(job_id: str):
    job = Job.fetch(job_id, connection=REDIS_CONN)
    return str(job.result)

@job(connection=REDIS_CONN, queue=QUEUE)
def dbt_run(models_path: str, vars_param: str) -> None:
    process = subprocess.run(
            ["dbt","--log-format", "json", "run","-m", models_path, "--vars", vars_param, "--project-dir", "../project/"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return [{"status_code_task":process.returncode},process.stdout.decode()]

