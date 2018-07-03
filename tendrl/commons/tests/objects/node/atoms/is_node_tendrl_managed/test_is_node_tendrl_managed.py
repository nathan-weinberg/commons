import __builtin__
import etcd
import maps
from mock import patch
import pytest


from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.objects.node.atoms.is_node_tendrl_managed import \
    IsNodeTendrlManaged, os_check, cpu_check, memory_check, network_check # noqa
from tendrl.commons.utils import etcd_utils


def read(*args, **kwargs):
    raise etcd.EtcdKeyNotFound


@patch.object(etcd, "Client")
def test_run(mock_client):
    setattr(__builtin__, "NS", maps.NamedDict())
    setattr(NS, "_int", maps.NamedDict())
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


@patch.object(etcd_utils, "read")
def test_os_check(mock_etcd_read):
    node_id = "Test_node"
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
    with pytest.raises(AtomExecutionFailedError):
        os_check(node_id)

    with pytest.raises(AtomExecutionFailedError):
        with patch.object(etcd_utils, "read", read):
            os_check(node_id)


@patch.object(etcd_utils, "read")
def test_cpu_check(mock_etcd_read):
    node_id = "Test_node"
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
    with pytest.raises(AtomExecutionFailedError):
        cpu_check(node_id)

    with pytest.raises(AtomExecutionFailedError):
        with patch.object(etcd_utils, "read", read):
            cpu_check(node_id)


@patch.object(etcd_utils, "read")
def test_memory_check(mock_etcd_read):
    node_id = "Test_node"
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
    with pytest.raises(AtomExecutionFailedError):
        memory_check(node_id)

    with pytest.raises(AtomExecutionFailedError):
        with patch.object(etcd_utils, "read", read):
            memory_check(node_id)


@patch.object(etcd_utils, "read")
def test_network_check(mock_etcd_read):
    node_id = "Test_node"
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
    with pytest.raises(AtomExecutionFailedError):
        network_check(node_id)

    with pytest.raises(AtomExecutionFailedError):
        with patch.object(etcd_utils, "read", read):
            network_check(node_id)
