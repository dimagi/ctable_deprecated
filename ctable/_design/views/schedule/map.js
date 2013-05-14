function (doc) {
    if (doc.doc_type === 'SqlExtractMapping') {
        emit([doc.schedule_type, doc.day_of_week_month, doc.hour], null)
    }
}