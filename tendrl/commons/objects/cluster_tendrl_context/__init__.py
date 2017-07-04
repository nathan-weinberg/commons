from tendrl.commons import objects


class ClusterTendrlContext(objects.BaseObject):

    def __init__(
        self,
        int_id=None,
        cluster_id=None,
        cluster_name=None,
        sds_name=None,
        sds_version=None,
            *args, **kwargs):

        super(ClusterTendrlContext, self).__init__(*args, **kwargs)
        # integration_id is the Tendrl generated cluster UUID
        self.integration_id = int_id or NS.tendrl_context.integration_id
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.sds_name = sds_name
        self.sds_version = sds_version
        self.value = 'clusters/{0}/TendrlContext'

    def render(self):
        self.value = self.value.format(self.integration_id)
        return super(ClusterTendrlContext, self).render()
