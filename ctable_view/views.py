import json
from couchdbkit import ResourceNotFound
from ctable.base import CtableExtractor
from ctable.writer import TestWriter
from dimagi.utils.couch.bulk import CouchTransaction
from dimagi.utils.web import json_response

from django.contrib import messages
from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect, HttpResponseForbidden, Http404, HttpResponseBadRequest, HttpResponse
from django.views.decorators.http import require_POST
from django.utils.translation import ugettext as _

from corehq.apps.users.models import Permissions
from corehq.apps.users.decorators import require_permission
from ctable.models import SqlExtractMapping, KeyMatcher, ColumnDef
from django.shortcuts import render, redirect
from django.core.urlresolvers import reverse


require_can_edit_sql_mappings = require_permission(Permissions.edit_data)


def _to_kwargs(req):
    # unicode can mix this up so have a little utility to do it
    # was seeing this only locally, not sure if python / django
    # version dependent
    return dict((str(k), v) for k, v in json.load(req).items())


@require_can_edit_sql_mappings
def view(request, domain, template='ctable/list_mappings.html'):
    mappings = SqlExtractMapping.by_domain(domain)
    return render(request, template, {
        'domain': domain,
        'mappings': mappings
    })

@require_can_edit_sql_mappings
def edit(request, domain, mapping_id, template='ctable/edit_mapping.html'):
    if request.method == 'POST':
            d = _to_kwargs(request)
            if domain not in d['domains']:
                d['domains'].append(domain)
            mapping = SqlExtractMapping.from_json(d)
            mapping.save()
            return json_response({'redirect': reverse('sql_mappings_list', kwargs={'domain':domain})})

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


def test(request, domain, mapping_id, template='ctable/test_mapping.html'):
    if mapping_id:
        try:
            limit = int(request.GET.get('limit', 100))
            limit = min(limit, 1000)
            mapping = SqlExtractMapping.get(mapping_id)
            test_writer = TestWriter()
            extractor = CtableExtractor('', SqlExtractMapping.get_db(), writer=test_writer)
            rows_processed, rows_with_value = extractor.extract(mapping, limit=limit)
            return render(request, template, {
                'domain': domain,
                'mapping': mapping,
                'rows_processed': rows_processed,
                'rows_with_value': rows_with_value,
                'data': test_writer.data
            })
        except ResourceNotFound:
            raise Http404()

    return redirect('sql_mappings_list', domain=domain)


@require_can_edit_sql_mappings
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

@require_can_edit_sql_mappings
def delete(request, domain, mapping_id):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            mapping.delete()
        except ResourceNotFound:
            raise Http404()

    return redirect('sql_mappings_list', domain=domain)