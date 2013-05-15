function (doc) {
    if (doc.doc_type === 'SqlExtractMapping') {
        var status = doc.active ? 'active' : 'inactive';
        emit([status, doc.schedule_type, doc.schedule_day, doc.schedule_hour], null)
    }
}