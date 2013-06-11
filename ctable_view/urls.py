from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('ctable_view.views',
    url(r'^$', 'view', name='sql_mappings_list'),
    url(r'^edit/(?P<mapping_id>[\w-]+)?$', 'edit', name='sql_mappings_edit'),
    url(r'^test/(?P<mapping_id>[\w-]+)?$', 'test', name='sql_mappings_test'),
    url(r'^toggle_state/(?P<mapping_id>[\w-]+)?$', 'toggle', name='sql_mappings_toggle_state'),
    url(r'^delete/(?P<mapping_id>[\w-]+)?$', 'delete', name='sql_mappings_delete'),
)