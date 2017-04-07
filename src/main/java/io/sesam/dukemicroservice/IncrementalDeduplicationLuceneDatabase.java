package io.sesam.dukemicroservice;

import java.util.Collection;

import no.priv.garshol.duke.Record;

public class IncrementalDeduplicationLuceneDatabase extends IncrementalLuceneDatabase {
    public Collection<Record> findCandidateMatches(Record record) {
        return findCandidateMatches(record, false);
    }
}