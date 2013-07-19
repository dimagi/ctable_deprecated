from ctable.models import SqlExtractMapping


class CtableMappingFixture(object):
    """
    Base class for creating ctable mapping fixtures.

    Sub-classes should be created in a 'ctable_mappings' module in the app root
    and will get synced during syncdb.
    """
    name = None
    domains = []
    couch_view = ''
    schedule_active = False

    @property
    def columns(self):
        return []

    def create(self):
        if not self.name or not self.domains:
            raise Exception('Missing name or domains property')

        mapping = SqlExtractMapping()
        for domain in self.domains:
            existing = SqlExtractMapping.by_name(domain, self.name)
            if existing:
                mapping = existing
                break

        mapping.active = self.schedule_active
        mapping.auto_generated = True
        mapping.name = self.name
        mapping.domains = self.domains
        mapping.couch_view = self.couch_view
        mapping.columns = self.columns

        self.customize(mapping)
        mapping.save()

    def customize(self, mapping):
        pass
