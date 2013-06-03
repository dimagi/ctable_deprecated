function (doc) {
    var i;
    if (doc.doc_type === 'SqlExtractMapping') {
        for (i = 0; i < doc.domains.length; i += 1) {
            emit([doc.domains[i], doc.name], null);
        }
    }
}