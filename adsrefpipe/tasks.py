from adsrefpipe import app as app_module
from kombu import Queue

import os

import adsrefpipe.perf_metrics as perf_metrics
import adsrefpipe.utils as utils

from adsputils import load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
app = app_module.ADSReferencePipelineCelery('reference-pipeline',
                                            proj_home=proj_home,
                                            local_config=globals().get('local_config', {}))

app.conf.CELERY_QUEUES = (
    Queue('task_process_reference', app.exchange, routing_key='task_process_reference'),
)

logger = app.logger


class FailedRequest(Exception):
    """
    Failed to connect to reference service.
    """
    pass


@app.task(queue='task_process_reference', max_retries=config['MAX_QUEUE_RETRIES'])
def task_process_reference(reference_task: dict) -> bool:
    """
    process a reference task by resolving references and updating the database

    :param reference_task: dictionary containing reference details and service url
    :return: True if processing is successful, False otherwise
    """
    reference_payload = reference_task.get('reference', {}) or {}
    # Older tests and some call sites provide a single-item reference list,
    # while the task instrumentation only needs one dict for metadata lookup.
    if isinstance(reference_payload, dict):
        reference_record = reference_payload
    elif isinstance(reference_payload, list) and reference_payload and isinstance(reference_payload[0], dict):
        reference_record = reference_payload[0]
    else:
        reference_record = {}
    event_extra = perf_metrics.build_event_extra(
        source_filename=reference_task.get('source_filename'),
        parser_name=reference_task.get('parser_name'),
        source_bibcode=reference_task.get('source_bibcode'),
        input_extension=reference_task.get('input_extension'),
        source_type=reference_task.get('source_type'),
        record_count=1,
    )
    record_id = reference_record.get('id')
    benchmark_context = perf_metrics.resolve_record_metrics_context(record_id, config=config) if record_id else {}
    benchmark_run_id = benchmark_context.get('run_id')
    benchmark_context_id = benchmark_context.get('context_id')
    benchmark_path = benchmark_context.get('path')
    try:
        with perf_metrics.timed_stage(
            stage='record_wall',
            run_id=benchmark_run_id,
            context_id=benchmark_context_id,
            record_id=record_id,
            extra=event_extra,
            path=benchmark_path,
        ):
            with perf_metrics.timed_stage(
                stage='resolver_http',
                run_id=benchmark_run_id,
                context_id=benchmark_context_id,
                record_id=record_id,
                extra=event_extra,
                path=benchmark_path,
            ):
                resolved = utils.post_request_resolved_reference(reference_payload, reference_task['resolver_service_url'])
            # if failed to connect to reference service, raise a exception to requeue, for max_retries times
            if not resolved:
                raise FailedRequest

            # TODO: remove comparing to classic before going to production
            classic_resolved_filename = reference_task['source_filename'].replace('sources', 'resolved') + '.result' if config['COMPARE_CLASSIC'] else None

            with perf_metrics.timed_stage(
                stage='post_resolved_db',
                run_id=benchmark_run_id,
                context_id=benchmark_context_id,
                record_id=record_id,
                extra=event_extra,
                path=benchmark_path,
            ):
                status = app.populate_tables_post_resolved(resolved, reference_task['source_bibcode'], classic_resolved_filename)
            if not status:
                return False

            return True

    except KeyError:
        return False

# dont know how to unittest this part
# this (app.start()) the only line that is not unittested
# and since i want all modules to be 100% covered,
# making this line not be considered part of coverage
if __name__ == '__main__':    # pragma: no cover
    app.start()
