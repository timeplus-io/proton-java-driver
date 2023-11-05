package com.timeplus.proton.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.timeplus.proton.client.logging.Logger;
import com.timeplus.proton.client.logging.LoggerFactory;

/**
 * This class maintains two immutable lists: preferred protocols and tags.
 * Usually it will be used in two scenarios: 1) find suitable
 * {@link ProtonClient} according to preferred protocol(s); and 2) pick
 * suitable {@link ProtonNode} to connect to.
 */
public class ProtonNodeSelector implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ProtonNodeSelector.class);

    public static final ProtonNodeSelector EMPTY = new ProtonNodeSelector(Collections.emptyList(),
            Collections.emptyList());

    public static ProtonNodeSelector of(ProtonProtocol protocol, ProtonProtocol... more) {
        List<ProtonProtocol> list = new LinkedList<>();

        if (protocol != null) {
            list.add(protocol);
        }

        if (more != null) {
            for (ProtonProtocol p : more) {
                if (p != null) {
                    list.add(p);
                }
            }
        }

        return of(list, null);
    }

    public static ProtonNodeSelector of(String tag, String... more) {
        List<String> list = new LinkedList<>();

        if (!ProtonChecker.isNullOrEmpty(tag)) {
            list.add(tag);
        }

        if (more != null) {
            for (String t : more) {
                if (!ProtonChecker.isNullOrEmpty(t)) {
                    list.add(t);
                }
            }
        }

        return of(null, list);
    }

    public static ProtonNodeSelector of(Collection<ProtonProtocol> protocols, Collection<String> tags) {
        return (protocols == null || protocols.isEmpty()) && (tags == null || tags.isEmpty()) ? EMPTY
                : new ProtonNodeSelector(protocols, tags);
    }

    private static final long serialVersionUID = 488571984297086418L;

    private final List<ProtonProtocol> protocols;
    private final Set<String> tags;

    protected ProtonNodeSelector(Collection<ProtonProtocol> protocols, Collection<String> tags) {
        if (protocols == null || protocols.isEmpty()) {
            this.protocols = Collections.emptyList();
        } else {
            List<ProtonProtocol> p = new ArrayList<>(protocols.size());
            for (ProtonProtocol protocol : protocols) {
                if (protocol == null) {
                    continue;
                } else if (protocol == ProtonProtocol.ANY) {
                    p.clear();
                    break;
                } else if (!p.contains(protocol)) {
                    p.add(protocol);
                }
            }

            this.protocols = p.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(p);
        }

        if (tags == null || tags.isEmpty()) {
            this.tags = Collections.emptySet();
        } else {
            Set<String> t = new HashSet<>();
            for (String tag : tags) {
                if (tag == null || tag.isEmpty()) {
                    continue;
                } else {
                    t.add(tag);
                }
            }

            this.tags = t.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(t);
        }
    }

    public List<ProtonProtocol> getPreferredProtocols() {
        return this.protocols;
    }

    public Set<String> getPreferredTags() {
        return this.tags;
    }

    /**
     * Test if the given client supports any of {@link #getPreferredProtocols()}.
     * It's always {@code false} if either the client is null or there's no
     * preferred protocol.
     *
     * @param client client to test
     * @return true if any of the preferred protocols is supported by the client
     */
    public boolean match(ProtonClient client) {
        boolean matched = false;

        if (client != null) {
            for (ProtonProtocol p : protocols) {
                log.debug("Checking [%s] against [%s]...", client, p);
                if (client.accept(p)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    public boolean match(ProtonNode node) {
        boolean matched = false;

        if (node != null) {
            matched = matchAnyOfPreferredProtocols(node.getProtocol()) && matchAllPreferredTags(node.getTags());
        }

        return matched;
    }

    public boolean matchAnyOfPreferredProtocols(ProtonProtocol protocol) {
        boolean matched = protocols.isEmpty() || protocol == ProtonProtocol.ANY;

        if (!matched && protocol != null) {
            for (ProtonProtocol p : protocols) {
                if (p == protocol) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    public boolean matchAllPreferredTags(Collection<String> tags) {
        boolean matched = true;

        if (tags != null && tags.size() > 0) {
            for (String t : tags) {
                if (t == null || t.isEmpty()) {
                    continue;
                }

                matched = matched && this.tags.contains(t);

                if (!matched) {
                    break;
                }
            }
        }

        return matched;
    }

    public boolean matchAnyOfPreferredTags(Collection<String> tags) {
        boolean matched = tags.isEmpty();

        if (tags != null && tags.size() > 0) {
            for (String t : tags) {
                if (t == null || t.isEmpty()) {
                    continue;
                }

                if (this.tags.contains(t)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    /*
     * public boolean matchAnyOfPreferredTags(String cluster,
     * List<ProtonProtocol> protocols, List<String> tags) { return
     * (ProtonChecker.isNullOrEmpty(cluster) || cluster.equals(this.cluster)) &&
     * supportAnyProtocol(protocols) && hasAllTags(tags); }
     */
}
