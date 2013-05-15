function (doc) {
    if (doc.doc_type === 'SqlExtractMapping') {
        emit([doc.schedule_type, doc.schedule_day, doc.schedule_hour], null)
    }
}