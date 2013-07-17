function (doc) {
    var i;
    if (doc.doc_type === 'SqlExtractMapping') {
        emit([null, doc.name], null);
        for (i = 0; i < doc.domains.length; i += 1) {
            emit([doc.domains[i], doc.name], null);
        }
    }
}