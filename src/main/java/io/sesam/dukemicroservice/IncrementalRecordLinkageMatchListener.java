package io.sesam.dukemicroservice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.priv.garshol.duke.Configuration;
import no.priv.garshol.duke.DukeException;
import no.priv.garshol.duke.Link;
import no.priv.garshol.duke.LinkDatabase;
import no.priv.garshol.duke.LinkKind;
import no.priv.garshol.duke.LinkStatus;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.matchers.AbstractMatchListener;
import no.priv.garshol.duke.matchers.PrintMatchListener;

public class IncrementalRecordLinkageMatchListener extends AbstractMatchListener {

    private final Logger logger;
    private ThreadLocal<Long> batchStartTimeTL = new ThreadLocal<>();
    private Configuration config;
    private final LinkDatabase linkdb;
    private ThreadLocal<Record> currentTL = new ThreadLocal<>();
    private ThreadLocal<Collection<Link>> curlinksTL = new ThreadLocal<>();

    public IncrementalRecordLinkageMatchListener(String deduplicationName, Configuration config, LinkDatabase linkdb) {
        this.config = config;
        this.linkdb = linkdb;
        this.logger = LoggerFactory.getLogger("recordLinkageMatchListener-" + deduplicationName);
    }

    public void batchReady(int size) {
        logger.info("batchReady(size={})", size);
        this.batchStartTimeTL.set(System.nanoTime());
    }

    public void batchDone() {
        // clearly, this is the end of the previous record
        if (currentTL.get() != null) {
            endRecord_();
        }
        currentTL.set(null);
        curlinksTL.set(null);
        linkdb.commit();

        long batchElapsedTime = System.nanoTime() - this.batchStartTimeTL.get();
        logger.info("batchDone() batchElapsedTime: {} seconds.", batchElapsedTime / 1e9);
    }

    // the only callbacks we get are matches(), matchesPerhaps(), and
    // noMatchFor(). from these, we need to work out when Duke starts
    // on a new record, and call startRecord_() and endRecord_()
    // accordingly.

    public void matches(Record r1, Record r2, double confidence) {
        Record current = currentTL.get();

        if (r1 != current) {
            // we've finished processing the previous record, so make the calls
            if (current != null)
                endRecord_();
            startRecord_(r1);
        }

        String id1 = getIdentity(r1);
        String id2 = getIdentity(r2);
        Collection<Link> curlinks = curlinksTL.get();
        curlinks.add(new Link(id1, id2, LinkStatus.INFERRED, LinkKind.SAME,
                              confidence));
    }

    public void matchesPerhaps(Record r1, Record r2, double confidence) {
        Record current = currentTL.get();
        if (r1 != current) {
            // we've finished processing the previous record, so make the calls
            if (current != null)
                endRecord_();
            startRecord_(r1);
        }

        String id1 = getIdentity(r1);
        String id2 = getIdentity(r2);
        curlinksTL.get().add(new Link(id1, id2, LinkStatus.INFERRED, LinkKind.MAYBESAME,
                              confidence));
    }

    public void noMatchFor(Record record) {
        // this is the only call we'll get for this record. it means the
        // previous record has ended, and this one has begun (and will end
        // with the next call, whatever it is)
        if (currentTL.get() != null)
            endRecord_();
        startRecord_(record);
        // next callback will trigger endRecord_()
    }

    // this method is called from the event methods
    public void startRecord_(Record r) {
        currentTL.set(r);
        curlinksTL.set(new ArrayList<>());
    }

    // this method is called from the event methods
    public void endRecord_() {
        // this is where we actually update the link database. basically,
        // all we need to do is to retract those links which weren't seen
        // this time around, and that can be done via assertLink, since it
        // can override existing links.

        Record current = currentTL.get();
        Collection<Link> curlinks = curlinksTL.get();;

        // get all the existing links
        Collection<Link> oldlinks = null;
        if (current != null) {
            oldlinks = linkdb.getAllLinksFor(getIdentity(current));
        }

        // build a hashmap so that we can look up corresponding old links from
        // new links
        if (oldlinks != null) {
            Map<String, Link> oldmap = new HashMap<>(oldlinks.size());
            for (Link l : oldlinks)
                oldmap.put(makeKey(l), l);

            // removing all the links we found this time around from the set of
            // old links. any links remaining after this will be stale, and need
            // to be retracted
            for (Link newl : new ArrayList<Link>(curlinks)) {
                String key = makeKey(newl);
                Link oldl = oldmap.get(key);
                if (oldl == null)
                    continue;

                if (oldl.overrides(newl))
                    // previous information overrides this link, so ignore
                    curlinks.remove(newl);
                else
                    // the link is out of date, but will be overwritten, so remove
                    oldmap.remove(key);
            }

            // all the inferred links left in oldmap are now old links we
            // didn't find on this pass. there is no longer any evidence
            // supporting them, and so we can retract them.
            for (Link oldl : oldmap.values())
                if (oldl.getStatus() == LinkStatus.INFERRED) {
                    oldl.retract(); // changes to retracted, updates timestamp
                    curlinks.add(oldl);
                }
        }

        // okay, now we write it all to the database
        for (Link l : curlinks)
            linkdb.assertLink(l);
    }

    private String getIdentity(Record r) {
        for (Property p : config.getIdentityProperties()) {
            Collection<String> vs = r.getValues(p.getName());
            if (vs == null)
                continue;
            for (String v : vs)
                return v;
        }
        throw new DukeException("No identity found in record [" +
                                        PrintMatchListener.toString(r) + "]");
    }

    private String makeKey(Link l) {
        return l.getID1() + "\t" + l.getID2();
    }
}
