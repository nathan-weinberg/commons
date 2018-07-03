import etcd

from tendrl.commons import objects
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.utils import etcd_utils


def os_check(node_id):
    try:
        os_details = etcd_utils.read("nodes/%s/Os" % node_id)
        if os_details.leaves is None:
            raise AtomExecutionFailedError(
                "Node doesnt have OS details populated"
            )
    except etcd.EtcdKeyNotFound:
        raise AtomExecutionFailedError(
            "Node doesnt have OS details populated"
        )


def cpu_check(node_id):
    try:
        cpu_details = etcd_utils.read("nodes/%s/Cpu" % node_id)
        if cpu_details.leaves is None:
            raise AtomExecutionFailedError(
                "Node doesnt have CPU details populated"
            )
    except etcd.EtcdKeyNotFound:
        raise AtomExecutionFailedError(
            "Node doesnt have CPU details populated"
        )


def memory_check(node_id):
    try:
        memory_details = etcd_utils.read(
            "nodes/%s/Memory" % node_id
        )
        if memory_details.leaves is None:
            raise AtomExecutionFailedError(
                "Node doesnt have Memory details populated"
            )
    except etcd.EtcdKeyNotFound:
        raise AtomExecutionFailedError(
            "Node doesnt have Memory details populated"
        )


def network_check(node_id):
    try:
        networks = etcd_utils.read("nodes/%s/Networks" % node_id)
        if networks.leaves is None:
            raise AtomExecutionFailedError(
                "Node doesnt have network details populated"
            )
    except etcd.EtcdKeyNotFound:
        raise AtomExecutionFailedError(
            "Node doesnt have network details populated"
        )


class IsNodeTendrlManaged(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(IsNodeTendrlManaged, self).__init__(*args, **kwargs)

    def run(self):
        node_ids = self.parameters.get('Node[]')
        if not node_ids or len(node_ids) == 0:
            raise AtomExecutionFailedError("Node[] cannot be empty")

        for node_id in node_ids:

            # Check if node has the OS details populated
            os_check(node_id)

            # Check if node has the CPU details populated
            cpu_check(node_id)

            # Check if node has the Memory populated
            memory_check(node_id)

            # Check if node has networks details populated
            network_check(node_id)

        return True
