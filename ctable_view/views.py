import json
from celery.result import AsyncResult
from couchdbkit import ResourceNotFound
from django.contrib.auth.decorators import user_passes_test, login_required
from django.views.decorators.http import require_POST
from corehq.apps.domain.decorators import require_superuser
from ctable.base import CtableExtractor
from ctable.util import get_test_extractor
from ctable.writer import InMemoryWriter
from dimagi.utils.web import json_response
from django.utils.translation import ugettext_noop as _

from django.core.urlresolvers import reverse
from django.http import Http404

from corehq.apps.users.models import Permissions
from corehq.apps.users.decorators import require_permission
from ctable.models import SqlExtractMapping
from django.shortcuts import render, redirect
from ctable.tasks import process_extract
from ctable.util import get_extractor

require_can_edit_sql_mappings = require_permission(Permissions.edit_data)


def _to_kwargs(req):
    # unicode can mix this up so have a little utility to do it
    # was seeing this only locally, not sure if python / django
    # version dependent
    return dict((str(k), v) for k, v in json.load(req).items())


@require_superuser
def view(request, domain, template='ctable/list_mappings.html'):
    mappings = SqlExtractMapping.by_domain(domain)
    return render(request, template, {
        'domain': domain,
        'mappings': mappings
    })


@require_superuser
def edit(request, domain, mapping_id, template='ctable/edit_mapping.html'):
    if request.method == 'POST':
            d = _to_kwargs(request)
            if domain not in d['domains']:
                d['domains'].append(domain)
            mapping = SqlExtractMapping.from_json(d)
            mapping.save()
            return json_response({'redirect': reverse('sql_mappings_list', kwargs={'domain': domain})})

    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
        except ResourceNotFound:
            raise Http404()
    else:
        mapping = SqlExtractMapping()

    return render(request, template, {
        'domain': domain,
        'mapping': mapping,
    })


@require_superuser
def test(request, domain, mapping_id, template='ctable/test_mapping.html'):
    if mapping_id:
        try:
            limit = int(request.GET.get('limit', 100))
            limit = min(limit, 1000)

            mapping = SqlExtractMapping.get(mapping_id)

            test_extractor = get_test_extractor()

            with test_extractor.writer:
                checks = test_extractor.writer.check_mapping(mapping)

            rows_processed, rows_with_value = test_extractor.extract(mapping, limit=limit)
            checks.update({
                'domain': domain,
                'mapping': mapping,
                'rows_processed': rows_processed,
                'rows_with_value': rows_with_value,
                'data': test_extractor.writer.data,
            })
            return render(request, template, checks)
        except ResourceNotFound:
            raise Http404()

    return redirect('sql_mappings_list', domain=domain)


@require_superuser
@require_POST
def clear_data(request, domain, mapping_id):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            get_extractor().drop_table(mapping.table_name)
            return json_response({'status': 'success', 'message': _('Data successfully cleared.')})
        except ResourceNotFound:
            return json_response({'status': 'error', 'message': _('Mapping not found.')})

    return json_response({'status': 'error', 'message': _('Mapping not found.')})


@require_superuser
def poll_state(request, domain, job_id=None):
    if not job_id:
        return redirect('sql_mappings_list', domain=domain)

    job = AsyncResult(job_id)
    data = job.result or job.state
    return json_response(data)


@require_superuser
def run(request, domain, mapping_id):
    limit = request.GET.get('limit', None)
    date_range = request.GET.get('date_range', None)
    if not limit or limit == 'undefined':
        limit = None
    elif limit:
        limit = int(limit)

    if not date_range or date_range == 'undefined':
        date_range = None
    elif date_range:
        date_range = int(date_range)
    job = process_extract.delay(mapping_id, limit=limit, date_range=date_range)
    return json_response({'redirect': reverse('sql_mappings_poll', kwargs={'domain': domain, 'job_id': job.id})})


@require_superuser
def toggle(request, domain, mapping_id):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            # don't activate 'fluff' mappings
            if not mapping.auto_generated:
                mapping.active = not mapping.active
                mapping.save()
        except ResourceNotFound:
            raise Http404()

    return redirect('sql_mappings_list', domain=domain)


@require_superuser
def delete(request, domain, mapping_id):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            mapping.delete()
        except ResourceNotFound:
            raise Http404()

    return redirect('sql_mappings_list', domain=domain)