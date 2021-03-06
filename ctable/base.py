import functools
from couchdbkit import ResourceNotFound
from .models import SqlExtractMapping, ColumnDef, KeyMatcher
from couchdbkit.ext.django.loading import get_db
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("ctable")

fluff_view = 'fluff/generic'


class CtableExtractor(object):
    def __init__(self, couch_db, backend):
        self.db = couch_db
        self.backend = backend

        from ctable.util import combine_rows
        self.combine_rows = combine_rows

    def extract(self, mapping, limit=None, date_range=None, status_callback=None):
        """
        Extract data from a CouchDb view into SQL
        """
        startkey, endkey = self.get_couch_keys(mapping, date_range=date_range)

        db = get_db(mapping.database) if mapping.database else self.db

        kwargs = mapping.couch_view_params
        if mapping.couch_group_level:
            kwargs['group_level'] = mapping.couch_group_level
        result = self.get_couch_rows(mapping.couch_view, startkey, endkey, db=db, limit=limit, **kwargs)

        total_rows = result.total_rows
        rows_with_value = 0
        if total_rows > 0:
            logger.info("Total rows: %d", total_rows)

            if status_callback:
                status_callback = functools.partial(status_callback, total_rows)

            rows = self.couch_rows_to_sql_rows(result, mapping, status_callback=status_callback)
            if limit:
                rows = list(rows)
                rows_with_value = len(rows)

            munged_rows = self.combine_rows(rows, mapping, chunksize=(limit or 250))
            self.write_rows_to_sql(munged_rows, mapping)

        return total_rows, rows_with_value

    def process_fluff_diff(self, diff, backend_name):
        """
        Given a Fluff diff, update the data in SQL to reflect the changes. This will
        query CouchDB in order to re-calculate all the grains that have changed.
        """
        mapping = self.get_fluff_extract_mapping(diff, backend_name)
        grains = self.get_fluff_grains(diff)
        couch_rows = self.recalculate_grains(grains, diff['database'])
        sql_rows = self.couch_rows_to_sql_rows(couch_rows, mapping)
        munged_rows = self.combine_rows(sql_rows, mapping)
        self.write_rows_to_sql(munged_rows, mapping)

    def get_couch_keys(self, extract_mapping, date_range=None):
        startkey = list(extract_mapping.couch_key_prefix)
        endkey = list(extract_mapping.couch_key_prefix)
        date_format = extract_mapping.couch_date_format
        if not date_range:
            date_range = extract_mapping.couch_date_range

        if date_range > 0 and date_format:
            end = datetime.utcnow()
            endkey += [end.strftime(date_format)]
            start = end - timedelta(days=date_range)
            startkey += [start.strftime(date_format)]
        endkey += [{}]
        return startkey, endkey

    def get_couch_rows(self, couch_view, startkey, endkey, db=None, **kwargs):
        db = db or self.db

        if 'reduce' not in kwargs:
            kwargs['reduce'] = True
        if 'group' not in kwargs and kwargs['reduce']:
            kwargs['group'] = True

        result = db.view(
            couch_view,
            startkey=startkey,
            endkey=endkey,
            **kwargs)
        return result

    def write_rows_to_sql(self, rows, extract_mapping):
        with self.backend:
            self.backend.write_rows(rows, extract_mapping)

    def couch_rows_to_sql_rows(self, couch_rows, mapping, status_callback=None):
        """
        Convert the list of rows from CouchDB into rows for insertion into SQL. To prevent getting
        empty rows in SQL (rows with no data columns) the following rules are applied to determine if a row
        should be return:

        * At least one 'non-key' column must match the row i.e. one column that has at least one matcher
        OR
        * All columns in the mapping must match the row.
        """
        num_cols = len(mapping.columns)
        count = 0
        for crow in couch_rows:
            sql_row = {}
            non_key_match = False
            for mc in mapping.columns:
                if mc.matches(crow['key'], crow['value']):
                    sql_row[mc.name] = mc.get_value(crow['key'], crow['value'])
                    non_key_match = non_key_match or not mc.is_key_column

            count += 1
            if status_callback and (count % 100) == 0:
                status_callback(count)

            if non_key_match or len(sql_row) == num_cols:
                yield sql_row

    def get_fluff_grains(self, diff):
        """
        Get the list of grains that have changed as a result of this Fluff diff.
        """
        groups = diff['group_values']
        for ind in diff['indicator_changes']:
            key_prefix = [diff['doc_type']] + groups + [ind['calculator'], ind['emitter']]
            if ind['emitter_type'] == 'null':
                yield key_prefix + [None]
            elif ind['emitter_type'] == 'date':
                for value in ind['values']:
                    yield key_prefix + [value['date'].isoformat()]

    def get_fluff_extract_mapping(self, diff, backend_name):
        """
        Get the SqlExtractMapping for the Fluff diff. This assumes the Fluff
        view is emitting data as follows:

            [doc_type, group1... groupN, calc_name, emitter_name, emitter_value] = 1
        """
        mapping_id = 'CtableFluffMapping_%s' % diff['doc_type']
        save = False
        try:
            mapping = SqlExtractMapping.get(mapping_id)
        except ResourceNotFound:
            save = True
            mapping = SqlExtractMapping(_id=mapping_id,
                                        backend=backend_name,
                                        database=diff['database'],
                                        domains=diff['domains'],
                                        name=diff['doc_type'],
                                        couch_view=fluff_view,
                                        active=False,
                                        auto_generated=True)

        for i, group in enumerate(diff['group_names']):
            if not any(x.name == group for x in mapping.columns):
                save = True
                type_map = diff.get('group_type_map') or {}
                mapping.columns.append(ColumnDef(name=group,
                                                 data_type=type_map.get(group, 'string'),
                                                 value_source='key',
                                                 value_index=1 + i))

        num_groups = len(diff['group_names'])
        if not any(x.name == 'date' for x in mapping.columns):
            save = True
            mapping.columns.append(ColumnDef(name='date',
                                             data_type='date',
                                             date_format="%Y-%m-%d",
                                             value_source='key',
                                             value_index=3 + num_groups))

        existing_columns = [x.name for x in mapping.columns]
        for indicator in diff['all_indicators']:
            calc_name = indicator['calculator']
            emitter_name = indicator['emitter']
            name = '{0}_{1}'.format(calc_name, emitter_name)
            if name not in existing_columns:
                save = True
                existing_columns.append(name)
                mapping.columns.append(ColumnDef(name=name,
                                                 data_type='integer',
                                                 value_source='value',
                                                 value_attribute=indicator['reduce_type'],
                                                 match_keys=[
                                                     KeyMatcher(index=1 + num_groups, value=calc_name),
                                                     KeyMatcher(index=2 + num_groups, value=emitter_name)
                                                 ]))

        if save:
            key_columns = [c for c in mapping.columns if c.is_key_column]
            non_key_columns = [c for c in mapping.columns if not c.is_key_column]
            mapping.columns = key_columns + sorted(non_key_columns, key=lambda c: c.name)

            mapping.save()

        return mapping

    def recalculate_grains(self, grains, database):
        """
        Query CouchDB to get the updated value for the grains.
        """
        result = []
        for grain in grains:
            result.extend(self.get_couch_rows(fluff_view, grain, grain + [{}], db=get_db(database)))
        return result

    def clear_all_data(self, mapping):
        with self.backend:
            self.backend.clear_all_data(mapping)
