import json
from celery.result import AsyncResult
from couchdbkit import ResourceNotFound
from django.views.decorators.http import require_POST
from corehq.apps.domain.decorators import require_superuser
from ctable.util import get_test_extractor, get_backend, backends
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
def view(request, domain=None, template='ctable/list_mappings.html'):
    if domain:
        mappings = SqlExtractMapping.by_domain(domain)
    else:
        mappings = SqlExtractMapping.all()

    return render(request, template, {
        'domain': domain,
        'mappings': mappings
    })


@require_superuser
def edit(request, mapping_id, domain=None, template='ctable/edit_mapping.html'):
    if request.method == 'POST':
            d = _to_kwargs(request)
            if domain and domain not in d['domains']:
                d['domains'].append(domain)

            mapping = SqlExtractMapping.from_json(d)
            if mapping.couch_key_prefix and mapping.couch_key_prefix[0] == '':
                mapping.couch_key_prefix = None

            # check for unique name
            for dom in mapping.domains:
                existing = SqlExtractMapping.by_name(dom, mapping.name)
                if existing and existing._id != mapping._id:
                    args = {'domain': dom}
                    return json_response({'error': _("Mapping with the same name exists "
                                                     "in the '%(domain)s' domain.") % args})

            mapping.save()

            kwargs = {'domain': domain} if domain else {}
            return json_response({'redirect': reverse('sql_mappings_list', kwargs=kwargs)})

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
        'backends': backends()
    })


@require_superuser
def test(request, mapping_id, domain=None, template='ctable/test_mapping.html'):
    if mapping_id:
        try:
            limit = request.GET.get('limit', 100)
            if not limit or limit == 'undefined':
                limit = None
            elif limit:
                limit = int(limit)

            limit = min(limit, 1000)

            mapping = SqlExtractMapping.get(mapping_id)
            test_extractor = get_test_extractor()

            backend = get_backend(mapping.backend)
            with backend:
                checks = backend.check_mapping(mapping)

            rows_processed, rows_with_value = test_extractor.extract(mapping, date_range=-1, limit=limit)
            checks.update({
                'domain': domain,
                'mapping': mapping,
                'rows_processed': rows_processed,
                'rows_with_value': rows_with_value,
                'data': test_extractor.backend.data,
            })
            return render(request, template, checks)
        except ResourceNotFound:
            raise Http404()

    kwargs = {'domain': domain} if domain else {}
    return redirect('sql_mappings_list', **kwargs)


@require_superuser
@require_POST
def clear_data(request, mapping_id, domain=None):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            get_extractor(mapping.backend).clear_all_data(mapping)
            return json_response({'status': 'success', 'message': _('Data successfully cleared.')})
        except ResourceNotFound:
            return json_response({'status': 'error', 'message': _('Mapping not found.')})

    return json_response({'status': 'error', 'message': _('Mapping not found.')})


@require_superuser
def poll_state(request, domain=None, job_id=None):
    if not job_id:
        kwargs = {'domain': domain} if domain else {}
        return redirect('sql_mappings_list', **kwargs)

    job = AsyncResult(job_id)
    data = job.result or job.state
    try:
        return json_response(data)
    except TypeError:
        return json_response(dict(error=str(data)))


@require_superuser
def run(request, mapping_id, domain=None):
    limit = request.GET.get('limit', None)
    date_range = request.GET.get('date_range', None)
    if not limit or limit == 'undefined':
        limit = None
    elif limit:
        limit = int(limit)

    if not date_range or date_range == 'undefined':
        date_range = -1
    elif date_range:
        date_range = int(date_range)

    job = process_extract.delay(mapping_id, limit=limit, date_range=date_range)

    kwargs = {'domain': domain} if domain else {}
    kwargs['job_id'] = job.id
    return json_response({'redirect': reverse('sql_mappings_poll', kwargs=kwargs)})

@require_superuser
def toggle(request, mapping_id, domain=None):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            # don't activate 'fluff' mappings
            if not mapping.auto_generated:
                mapping.active = not mapping.active
                mapping.save()
        except ResourceNotFound:
            raise Http404()

    kwargs = {'domain': domain} if domain else {}
    return redirect('sql_mappings_list', **kwargs)


@require_superuser
def delete(request, mapping_id, domain=None):
    if mapping_id:
        try:
            mapping = SqlExtractMapping.get(mapping_id)
            if domain:
                assert domain in mapping.domains
            assert mapping.doc_type == SqlExtractMapping._doc_type
            mapping.delete()
        except ResourceNotFound:
            raise Http404()

    kwargs = {'domain': domain} if domain else {}
    return redirect('sql_mappings_list', **kwargs)