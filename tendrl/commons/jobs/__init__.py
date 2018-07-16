import datetime
import threading
import time
import traceback


import etcd
from pytz import utc


from tendrl.commons.event import Event
from tendrl.commons.flows.exceptions import FlowExecutionFailedError
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.utils import alert_utils
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import log_utils as logger
from tendrl.commons.utils import time_utils


DEFAULT_JOB_TTL = 172800


class JobConsumerThread(threading.Thread):

    def __init__(self):
        super(JobConsumerThread, self).__init__()
        self.daemon = True
        self.name = "Job consumer thread"
        self._complete = threading.Event()

    def run(self):
        logger.log(
            "debug",
            NS.publisher_id,
            {"message": "%s running" % self.__class__.__name__}
        )
        while not self._complete.is_set():
            _job_sync_interval = 5
            NS.node_context = NS.node_context.load()
            NS.tendrl_context = NS.tendrl_context.load()
            if "tendrl/monitor" not in NS.node_context.tags:
                if NS.tendrl_context.integration_id is None or \
                        NS.node_context.fqdn is None:
                    time.sleep(_job_sync_interval)
                    continue
            if "tendrl/monitor" in NS.node_context.tags:
                _job_sync_interval = 3

            time.sleep(_job_sync_interval)
            try:
                jobs = NS.tendrl.objects.Job().load_all()
            except etcd.EtcdKeyNotFound:
                continue

            for job in jobs:
                # Check job not already locked by some agent
                if job.locked_by or job.job_id in [None, '']:
                    continue

                _job_thread = threading.Thread(
                    target=process_job, args=(job.job_id,)
                )
                _job_thread.daemon = True
                _job_thread.start()
                _job_thread.join()

    def stop(self):
        self._complete.set()


def process_job(jid):
    job = NS.tendrl.objects.Job(job_id=jid).load()
    if job.status in [None, ""]:
        job.status = "new"
        job.save()

    NS.node_context = NS.node_context.load()
    # Check job not already "finished", or "processing"
    try:
        if job.status in ["finished",
                          "processing",
                          "failed"]:
            return
    except etcd.EtcdKeyNotFound:
        pass

    try:
        _timeout = None
        _timeout = job.timeout
        if _timeout:
            _timeout = _timeout.lower()
    except etcd.EtcdKeyNotFound:
        pass

    # tendrl-node-agent tagged as tendrl/monitor will ensure
    # >10 min old "new" jobs are timed out and marked as
    # "failed" (the parent job of these jobs will also be
    # marked as "failed")
    if "tendrl/monitor" in NS.node_context.tags and \
        _timeout == "yes" and job.status == "new":
        _valid_until = job.valid_until

        if _valid_until:
            _now_epoch = (time_utils.now() -
                          datetime.datetime(1970, 1,
                                            1).replace(
                              tzinfo=utc)).total_seconds()
            if int(_now_epoch) >= int(_valid_until):
                # Job has "new" status since 10 minutes,
                # mark status as "failed" and Job.error =
                # "Timed out"
                try:
                    job = job.load()
                    if job.status == "new":
                        job.status = "failed"
                        job.save()
                except etcd.EtcdCompareFailed:
                    pass
                else:
                    job = NS.tendrl.objects.Job(job_id=jid).load()
                    if job.status == "new":
                        _msg = str("Timed-out (>10min as 'new')")
                        job.errors = _msg
                        job.save()
                        if job.payload.get('parent') is None:
                            integration_id = NS.tendrl_context.integration_id
                            alert_utils.alert_job_status(
                                "failed",
                                "Job timed out (job_id: %s)" % jid,
                                integration_id=integration_id or
                                job.payload['parameters'].get(
                                    'TendrlContext.integration_id'
                                ),
                                cluster_name=NS.tendrl_context.cluster_name or
                                job.payload['parameters'].get(
                                    'TendrlContext.cluster_name'
                                )
                            )
                    return
        else:
            _now_plus_10 = time_utils.now() + datetime.timedelta(minutes=10)
            _epoch_start = datetime.datetime(1970, 1, 1).replace(tzinfo=utc)

            _now_plus_10_epoch = (_now_plus_10 -
                                  _epoch_start).total_seconds()
            time.sleep(7)
            job = job.load()
            if job.status == "new":
                # To avoid  server and storage node do save same time
                job.valid_until = int(_now_plus_10_epoch)
                job.save()

    job = NS.tendrl.objects.Job(job_id=jid).load()
    if job.payload["type"] == NS.type and \
            job.status == "new":
        # Job routing
        # Flows created by tendrl-api use 'tags' from flow
        # definition to target jobs
        _tag_match = False
        if job.payload.get("tags", []):
            for flow_tag in job.payload['tags']:
                if flow_tag in NS.node_context.tags:
                    _tag_match = True

        if not _tag_match:
            _job_tags = ", ".join(job.payload.get("tags", []))
            _msg = "Node (%s)(type: %s)(tags: %s) will not " \
                   "process job-%s (tags: %s)" % \
                   (NS.node_context.node_id, NS.type,
                    NS.node_context.tags, jid,
                    _job_tags)
            logger.log(
                "debug",
                NS.publisher_id,
                {"message": _msg}
            )
            return

        try:
            try:
                job_status_key = "/queue/%s/status" % job.job_id
                etcd_utils.write(job_status_key,
                                 "processing",
                                 prevValue="new")
            except etcd.EtcdKeyNotFound:
                # if status watchable attribute not present
                # then it will be created when job save happens
                pass
            lock_info = dict(node_id=NS.node_context.node_id,
                             fqdn=NS.node_context.fqdn,
                             type=NS.type)
            job = job.load()
            job.locked_by = lock_info
            job.status = "processing"
            job.save(ttl=DEFAULT_JOB_TTL)
        except etcd.EtcdCompareFailed:
            # job is already being processed by some tendrl
            # agent
            return

        the_flow = None
        try:
            current_ns, flow_name, obj_name = \
                _extract_fqdn(job.payload['run'])

            if obj_name:
                runnable_flow = current_ns.ns.get_obj_flow(
                    obj_name, flow_name)
            else:
                runnable_flow = current_ns.ns.get_flow(flow_name)

            job = job.load()
            lock_info = dict(node_id=NS.node_context.node_id,
                             fqdn=NS.node_context.fqdn,
                             type=NS.type)
            if job.locked_by != lock_info:
                return

            the_flow = runnable_flow(parameters=job.payload[
                'parameters'], job_id=job.job_id)
            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Starting Job %s" %
                            job.job_id},
                job_id=job.job_id,
                flow_id=the_flow.parameters['flow_id']
            )

            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Running %s" %
                            job.payload['run'].split('.')[-1]},
                job_id=job.job_id,
                flow_id=the_flow.parameters['flow_id']
            )

            the_flow.run()

            try:
                job = job.load()
                job.status = "finished"
                job.save()
            except etcd.EtcdCompareFailed:
                # This should not happen!
                _msg = "Cannot mark job as 'finished', " \
                       "current job status invalid"
                raise FlowExecutionFailedError(_msg)

            logger.log(
                "info",
                NS.publisher_id,
                {"message": "Job (%s) for %s finished. "
                            % (
                                job.job_id,
                                job.payload['run'].split('.')[-1])},
                job_id=job.job_id,
                flow_id=the_flow.parameters['flow_id'],
            )
            if job.payload.get('parent') is None:
                alert_utils.alert_job_status(
                    "finished",
                    "%s (job ID: %s) completed successfully " % (
                        job.payload['run'].split('.')[-1],
                        job.job_id),
                    integration_id=NS.tendrl_context.integration_id or
                    job.payload['parameters'].get(
                        'TendrlContext.integration_id'
                    ),
                    cluster_name=NS.tendrl_context.cluster_name or
                    job.payload['parameters'].get(
                        'TendrlContext.cluster_name'
                    )
                )
        except (FlowExecutionFailedError,
                AtomExecutionFailedError,
                Exception) as e:
            _trace = str(traceback.format_exc(e))
            _msg = "Failure in Job %s Flow %s with error:" % \
                   (job.job_id, job.payload['run'])
            Event(
                ExceptionMessage(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={"message": _msg + _trace,
                             "exception": e
                             }
                )
            )
            if the_flow:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {"message": _msg + "\n" + _trace},
                    job_id=job.job_id,
                    flow_id=the_flow.parameters['flow_id']
                )
            else:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {"message": _msg + "\n" + _trace}
                )

            try:
                job = job.load()
                job.status = "failed"
                job.save()
            except etcd.EtcdCompareFailed:
                # This should not happen!
                _msg = "Cannot mark job as 'failed', current" \
                       "job status invalid"
                raise FlowExecutionFailedError(_msg)
            else:
                job = job.load()
                job.errors = _trace
                if job.payload.get('parent') is None:
                    alert_utils.alert_job_status(
                        "failed",
                        "Job failed (job_id: %s)" % job.job_id,
                        integration_id=NS.tendrl_context.integration_id or
                        job.payload['parameters'].get(
                            'TendrlContext.integration_id'
                        ),
                        cluster_name=NS.tendrl_context.cluster_name or
                        job.payload['parameters'].get(
                            'TendrlContext.cluster_name'
                        )
                    )
                job.save()


def _extract_fqdn(flow_fqdn):
    ns, flow_name = flow_fqdn.split(".flows.")
    obj_name = None

    # check if the flow is bound to any object
    try:
        ns, obj_name = ns.split(".objects.")
    except ValueError:
        pass

    ns_str = ns.split(".")[-1]
    if "integrations" in ns:
        return getattr(NS.integrations, ns_str), flow_name, obj_name
    else:
        return getattr(NS, ns_str), flow_name, obj_name
