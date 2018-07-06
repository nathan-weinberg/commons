import etcd

from tendrl.commons import objects
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import log_utils as logger


def check_resource(resource, node_id):
    res = etcd_utils.read("nodes/%s/%s" % (node_id, resource))
    if res.leaves is None:
        return False
    return True


class IsNodeTendrlManaged(objects.BaseAtom):

    def __init__(self, *args, **kwargs):
        super(IsNodeTendrlManaged, self).__init__(*args, **kwargs)

    def run(self):
        node_ids = self.parameters.get('Node[]')
        if not node_ids or len(node_ids) == 0:
            raise AtomExecutionFailedError("Node[] cannot be empty")

        resources = ["Os", "Memory", "Cpu", "Networks"]
        for node_id in node_ids:
            for resource in resources:
                try:
                    if not check_resource(node_id, resource):
                        logger.log(
                            "error",
                            NS.get("publisher_id", None),
                            {
                                "message": "Node %s doesn't have %s details "
                                "populated" % (
                                           NS.node_context.fqdn, resource)
                            },
                            job_id=self.parameters['job_id'],
                            flow_id=self.parameters['flow_id']
                        )
                        return False
                except etcd.EtcdKeyNotFound:
                    logger.log(
                        "error",
                        NS.get("publisher_id", None),
                        {
                            "message": "Node %s doesn't have %s details "
                                       "populated" % (
                                           NS.node_context.fqdn, resource)
                        },
                        job_id=self.parameters['job_id'],
                        flow_id=self.parameters['flow_id']
                    )
                    return False
        return True
