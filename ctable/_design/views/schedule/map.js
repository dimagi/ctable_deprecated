function (doc) {
    if (doc.base_doc === 'SqlExtractMapping') {
        emit([doc.schedule_type, doc.day_of_week_month, doc.hour], null)
    }
}