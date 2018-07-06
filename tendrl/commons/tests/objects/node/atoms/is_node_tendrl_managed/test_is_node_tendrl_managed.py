import __builtin__
import etcd
import maps
from mock import patch
import pytest

from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.objects.node.atoms.is_node_tendrl_managed import \
    IsNodeTendrlManaged
from tendrl.commons.utils import etcd_utils


def read(*args, **kwargs):
    raise etcd.EtcdKeyNotFound


@patch.object(etcd_utils, "read")
@patch.object(etcd, "Client")
def test_run(mock_client, mock_etcd_read):
    setattr(__builtin__, "NS", maps.NamedDict())
    setattr(NS, "_int", maps.NamedDict())
    setattr(NS, "node_context", maps.NamedDict())
    NS.node_context["fqdn"] = "test_fqdn"
    NS._int.etcd_kwargs = {
        'port': 1,
        'host': 2,
        'allow_reconnect': True}
    NS._int.client = etcd.Client(**NS._int.etcd_kwargs)

    intr_obj = IsNodeTendrlManaged()
    intr_obj.parameters = maps.NamedDict()
    mock_client.return_value = etcd.Client()

    # no node ids
    intr_obj.parameters["Node[]"] = []
    with pytest.raises(AtomExecutionFailedError):
        intr_obj.run()

    # with one node id
    intr_obj.parameters["Node[]"] = ["Test_node"]
    intr_obj.run()

    # leaves is None
    intr_obj.parameters['job_id'] = ['Test_job_id']
    intr_obj.parameters['flow_id'] = ['Test_flow_id']
    mock_etcd_read.return_value = maps.NamedDict(
        leaves=None,
        value='{"status": "UP",'
              '"pkey": "tendrl-node-test",'
              '"node_id": "test_node_id",'
              '"ipv4_addr": "test_ip",'
              '"tags": "[\\"my_tag\\"]",'
              '"sync_status": "done",'
              '"locked_by": "fd",'
              '"fqdn": "tendrl-node-test",'
              '"leaves: None",'
              '"last_sync": "date"}'
    )
    intr_obj.run()

    # raises etcd.EtcdKeyNotFound
    with patch.object(etcd_utils, "read", read):
        intr_obj.run()
