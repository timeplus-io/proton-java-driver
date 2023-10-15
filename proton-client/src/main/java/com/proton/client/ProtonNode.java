package com.proton.client;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.proton.client.config.ProtonClientOption;
import com.proton.client.config.ProtonDefaults;

/**
 * This class depicts a Proton server, essentially a combination of host,
 * port and protocol, for client to connect.
 */
public class ProtonNode implements Function<ProtonNodeSelector, ProtonNode>, Serializable {
    /**
     * Node status.
     */
    public enum Status {
        HEALTHY, UNHEALTHY, MANAGED, UNMANAGED
    }

    /**
     * Mutable and non-thread safe builder.
     */
    public static class Builder {
        protected String cluster;
        protected String host;
        protected Integer port;
        protected InetSocketAddress address;
        protected ProtonProtocol protocol;
        protected ProtonCredentials credentials;
        protected String database;
        // label is more expressive, but is slow for comparison
        protected Set<String> tags;
        protected Integer weight;

        protected TimeZone tz;
        protected ProtonVersion version;

        /**
         * Default constructor.
         */
        protected Builder() {
            this.tags = new HashSet<>(3);
        }

        protected String getCluster() {
            if (cluster == null) {
                cluster = (String) ProtonDefaults.CLUSTER.getEffectiveDefaultValue();
            }

            return cluster;
        }

        protected InetSocketAddress getAddress() {
            if (host == null) {
                host = (String) ProtonDefaults.HOST.getEffectiveDefaultValue();
            }

            if (port == null) {
                port = (Integer) ProtonDefaults.PORT.getEffectiveDefaultValue();
            }

            if (address == null) {
                address = InetSocketAddress.createUnresolved(host, port);
            }

            return address;
        }

        protected ProtonProtocol getProtocol() {
            if (protocol == null) {
                protocol = (ProtonProtocol) ProtonDefaults.PROTOCOL.getEffectiveDefaultValue();
            }
            return protocol;
        }

        protected ProtonCredentials getCredentials() {
            return credentials;
        }

        protected String getDatabase() {
            return database;
        }

        protected Set<String> getTags() {
            int size = tags == null ? 0 : tags.size();
            if (size == 0) {
                return Collections.emptySet();
            }

            Set<String> s = new HashSet<>(size);
            s.addAll(tags);
            return Collections.unmodifiableSet(s);
        }

        protected int getWeight() {
            if (weight == null) {
                weight = (Integer) ProtonDefaults.WEIGHT.getEffectiveDefaultValue();
            }

            return weight.intValue();
        }

        protected TimeZone getTimeZone() {
            return tz;
        }

        protected ProtonVersion getVersion() {
            return version;
        }

        /**
         * Sets cluster name.
         *
         * @param cluster cluster name, null means {@link ProtonDefaults#CLUSTER}
         * @return this builder
         */
        public Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * Sets host name.
         *
         * @param host host name, null means {@link ProtonDefaults#HOST}
         * @return this builder
         */
        public Builder host(String host) {
            if (!Objects.equals(this.host, host)) {
                this.address = null;
            }

            this.host = host;
            return this;
        }

        /**
         * Sets port number.
         *
         * @param port port number, null means {@link ProtonDefaults#PORT}
         * @return this builder
         */
        public Builder port(Integer port) {
            return port(null, port);
        }

        /**
         * Sets protocol used by the port.
         *
         * @param protocol protocol, null means {@link ProtonDefaults#PROTOCOL}
         * @return this builder
         */
        public Builder port(ProtonProtocol protocol) {
            return port(protocol, ProtonChecker.nonNull(protocol, "protocol").getDefaultPort());
        }

        /**
         * Sets protocol and port number.
         *
         * @param protocol protocol, null means {@link ProtonDefaults#PROTOCOL}
         * @param port     number, null means {@link ProtonDefaults#PORT}
         * @return this builder
         */
        public Builder port(ProtonProtocol protocol, Integer port) {
            if (!Objects.equals(this.port, port)) {
                this.address = null;
            }

            this.port = port;
            this.protocol = protocol;
            return this;
        }

        /**
         * Sets socket address.
         *
         * @param address socket address, null means {@link ProtonDefaults#HOST} and
         *                {@link ProtonDefaults#PORT}
         * @return this builder
         */
        public Builder address(InetSocketAddress address) {
            return address(null, address);
        }

        /**
         * Sets protocol and socket address.
         *
         * @param protocol protocol, null means {@link ProtonDefaults#PROTOCOL}
         * @param address  socket address, null means {@link ProtonDefaults#HOST}
         *                 and {@link ProtonDefaults#PORT}
         * @return this builder
         */
        public Builder address(ProtonProtocol protocol, InetSocketAddress address) {
            if (!Objects.equals(this.address, address)) {
                this.host = null;
                this.port = null;
            }

            this.address = address;
            this.protocol = protocol;
            return this;
        }

        /**
         * Sets database name.
         *
         * @param database database name, null means {@link ProtonDefaults#DATABASE}
         * @return this builder
         */
        public Builder database(String database) {
            this.database = database;
            return this;
        }

        /**
         * Sets credentials will be used when connecting to this node. This is optional
         * as client will use {@link ProtonConfig#getDefaultCredentials()} to
         * connect to all nodes.
         *
         * @param credentials credentials, null means
         *                    {@link ProtonConfig#getDefaultCredentials()}
         * @return this builder
         */
        public Builder credentials(ProtonCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        /**
         * Adds a tag for this node.
         *
         * @param tag tag for the node, null or duplicate tag will be ignored
         * @return this builder
         */
        public Builder addTag(String tag) {
            if (tag != null) {
                this.tags.add(tag);
            }

            return this;
        }

        /**
         * Removes a tag from this node.
         *
         * @param tag tag to be removed, null value will be ignored
         * @return this builder
         */
        public Builder removeTag(String tag) {
            if (tag != null) {
                this.tags.remove(tag);
            }

            return this;
        }

        /**
         * Sets all tags for this node. Use null or empty value to clear all existing
         * tags.
         *
         * @param tag  tag for the node, null will be ignored
         * @param more more tags for the node, null tag will be ignored
         * @return this builder
         */
        public Builder tags(String tag, String... more) {
            this.tags.clear();

            addTag(tag);

            if (more != null) {
                for (String t : more) {
                    if (t == null) {
                        continue;
                    }
                    this.tags.add(t);
                }
            }

            return this;
        }

        /**
         * Sets all tags for this node. Use null or empty value to clear all existing
         * tags.
         *
         * @param tags list of tags for the node, null tag will be ignored
         * @return this builder
         */
        public Builder tags(Collection<String> tags) {
            this.tags.clear();

            if (tags != null) {
                for (String t : tags) {
                    if (t == null) {
                        continue;
                    }
                    this.tags.add(t);
                }
            }

            return this;
        }

        /**
         * Sets weight of this node.
         *
         * @param weight weight of the node, null means
         *               {@link ProtonDefaults#WEIGHT}
         * @return this builder
         */
        public Builder weight(Integer weight) {
            this.weight = weight;
            return this;
        }

        /**
         * Sets time zone of this node.
         *
         * @param tz time zone ID, could be null
         * @return this builder
         */
        public Builder timeZone(String tz) {
            this.tz = tz != null ? TimeZone.getTimeZone(tz) : null;
            return this;
        }

        /**
         * Sets time zone of this node.
         *
         * @param tz time zone, could be null
         * @return this builder
         */
        public Builder timeZone(TimeZone tz) {
            this.tz = tz;
            return this;
        }

        /**
         * Sets vesion of this node.
         *
         * @param version version string, could be null
         * @return this builder
         */
        public Builder version(String version) {
            this.version = version != null ? ProtonVersion.of(version) : null;
            return this;
        }

        /**
         * Sets vesion of this node.
         *
         * @param version version, could be null
         * @return this builder
         */
        public Builder version(ProtonVersion version) {
            this.version = version;
            return this;
        }

        /**
         * Creates a new node.
         *
         * @return new node
         */
        public ProtonNode build() {
            return new ProtonNode(this);
        }
    }

    /**
     * Gets builder for creating a new node, same as {@code builder(null)}.
     *
     * @return builder for creating a new node
     */
    public static Builder builder() {
        return builder(null);
    }

    /**
     * Gets builder for creating a new node based on the given one.
     *
     * @param base template to start with
     * @return builder for creating a new node
     */
    public static Builder builder(ProtonNode base) {
        Builder b = new Builder();
        if (base != null) {
            b.cluster(base.getCluster()).host(base.getHost()).port(base.getProtocol(), base.getPort())
                    .credentials(base.getCredentials().orElse(null)).database(base.getDatabase().orElse(null))
                    .tags(base.getTags()).weight(base.getWeight()).timeZone(base.getTimeZone().orElse(null))
                    .version(base.getVersion().orElse(null));
        }
        return b;
    }

    /**
     * Creates a node object in one go. Short version of
     * {@code ProtonNode.builder().host(host).port(protocol, port).database(database).tags(tags).build()}.
     *
     * @param host     host name, null means {@link ProtonDefaults#HOST}
     * @param protocol protocol, null means {@link ProtonDefaults#PROTOCOL}
     * @param port     port number
     * @param database database name, null means {@link ProtonDefaults#DATABASE}
     * @param tags     tags for the node, null tag will be ignored
     * @return node object
     */
    public static ProtonNode of(String host, ProtonProtocol protocol, int port, String database,
            String... tags) {
        Builder builder = builder().host(host).port(protocol, port).database(database);
        if (tags != null && tags.length > 0) {
            builder.tags(null, tags);
        }
        return builder.build();
    }

    /**
     * Generated UID.
     */
    private static final long serialVersionUID = 8342604784121795372L;

    private final String cluster;
    private final ProtonProtocol protocol;
    private final InetSocketAddress address;
    private final ProtonCredentials credentials;
    private final String database;
    private final Set<String> tags;
    private final int weight;

    // extended attributes, better to use a map and offload to sub class?
    private final TimeZone tz;
    private final ProtonVersion version;
    // TODO: metrics

    private transient BiConsumer<ProtonNode, Status> manager;

    protected ProtonNode(Builder builder) {
        ProtonChecker.nonNull(builder, "builder");

        this.cluster = builder.getCluster();
        this.protocol = builder.getProtocol();
        this.address = builder.getAddress();
        this.credentials = builder.getCredentials();
        this.database = builder.getDatabase();
        this.tags = builder.getTags();
        this.weight = builder.getWeight();

        this.tz = builder.getTimeZone();
        this.version = builder.getVersion();

        this.manager = null;
    }

    /**
     * Gets socket address to connect to this node.
     *
     * @return socket address to connect to the node
     */
    public InetSocketAddress getAddress() {
        return this.address;
    }

    /**
     * Gets credentials for accessing this node. Use
     * {@link ProtonConfig#getDefaultCredentials()} if this is not present.
     *
     * @return credentials for accessing this node
     */
    public Optional<ProtonCredentials> getCredentials() {
        return Optional.ofNullable(credentials);
    }

    /**
     * Gets credentials for accessing this node. It first attempts to use
     * credentials tied to the node, and then use default credentials from the given
     * configuration.
     * 
     * @param config non-null configuration for retrieving default credentials
     * @return credentials for accessing this node
     */
    public ProtonCredentials getCredentials(ProtonConfig config) {
        return credentials != null ? credentials : ProtonChecker.nonNull(config, "config").getDefaultCredentials();
    }

    /**
     * Gets host of the node.
     *
     * @return host of the node
     */
    public String getHost() {
        return this.address.getHostString();
    }

    /**
     * Gets port of the node.
     *
     * @return port of the node
     */
    public int getPort() {
        return this.address.getPort();
    }

    /**
     * Gets database of the node.
     *
     * @return database of the node
     */
    public Optional<String> getDatabase() {
        return Optional.ofNullable(database);
    }

    /**
     * Gets database of the node. When {@link #hasPreferredDatabase()} is
     * {@code false}, it will use database from the given configuration.
     * 
     * @param config non-null configuration to get default database
     * @return database of the node
     */
    public String getDatabase(ProtonConfig config) {
        return !ProtonChecker.nonNull(config, "config").hasOption(ProtonClientOption.DATABASE)
                && hasPreferredDatabase() ? database : config.getDatabase();
    }

    /**
     * Gets all tags of the node.
     *
     * @return tags of the node
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Gets weight of the node.
     *
     * @return weight of the node
     */
    public int getWeight() {
        return this.weight;
    }

    /**
     * Gets time zone of the node.
     *
     * @return time zone of the node
     */
    public Optional<TimeZone> getTimeZone() {
        return Optional.ofNullable(tz);
    }

    /**
     * Gets time zone of the node. When not defined, it will use server time zone
     * from the given configuration.
     * 
     * @param config non-null configuration to get server time zone
     * @return time zone of the node
     */
    public TimeZone getTimeZone(ProtonConfig config) {
        return tz != null ? tz : ProtonChecker.nonNull(config, "config").getServerTimeZone();
    }

    /**
     * Gets version of the node.
     *
     * @return version of the node
     */
    public Optional<ProtonVersion> getVersion() {
        return Optional.ofNullable(version);
    }

    /**
     * Gets version of the node. When not defined, it will use server version from
     * the given configuration.
     * 
     * @param config non-null configuration to get server version
     * @return version of the node
     */
    public ProtonVersion getVersion(ProtonConfig config) {
        return version != null ? version : ProtonChecker.nonNull(config, "config").getServerVersion();
    }

    /**
     * Gets cluster name of the node.
     *
     * @return cluster name of node
     */
    public String getCluster() {
        return this.cluster;
    }

    /**
     * Gets protocol used by the node.
     *
     * @return protocol used by the node
     */
    public ProtonProtocol getProtocol() {
        return this.protocol;
    }

    /**
     * Checks if preferred database was specified or not. When preferred database
     * was not specified, {@link #getDatabase()} will return default database.
     *
     * @return true if preferred database was specified; false otherwise
     */
    public boolean hasPreferredDatabase() {
        return database != null && !database.isEmpty();
    }

    /**
     * Sets manager for this node.
     * 
     * @param manager function to manage status of the node
     */
    public synchronized void setManager(BiConsumer<ProtonNode, Status> manager) {
        if (this.manager != null && !this.manager.equals(manager)) {
            this.manager.accept(this, Status.UNMANAGED);
        }

        if (manager != null && !manager.equals(this.manager)) {
            manager.accept(this, Status.MANAGED);
            this.manager = manager;
        }
    }

    /**
     * Updates status of the node. This will only work when a manager function
     * exists via {@link #setManager(BiConsumer)}.
     * 
     * @param status node status
     */
    public synchronized void updateStatus(Status status) {
        if (this.manager != null) {
            this.manager.accept(this, status);
        }
    }

    @Override
    public ProtonNode apply(ProtonNodeSelector t) {
        if (t != null && t != ProtonNodeSelector.EMPTY
                && (!t.matchAnyOfPreferredProtocols(protocol) || !t.matchAllPreferredTags(tags))) {
            throw new IllegalArgumentException("No suitable node found");
        }

        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append(getClass().getSimpleName()).append('(');
        boolean hasCluster = cluster != null && !cluster.isEmpty();
        if (hasCluster) {
            builder.append("cluster=").append(cluster).append(", ");
        }
        builder.append("addr=").append(protocol.name().toLowerCase()).append(":").append(address).append(", db=")
                .append(database);
        if (tz != null) {
            builder.append(", tz=").append(tz.getID());
        }
        if (version != null) {
            builder.append(", ver=").append(version);
        }
        if (tags != null && !tags.isEmpty()) {
            builder.append(", tags=").append(tags);
        }
        if (hasCluster) {
            builder.append(", weight=").append(weight);
        }

        return builder.append(')').append('@').append(hashCode()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, cluster, credentials, database, protocol, tags, weight, tz, version);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        ProtonNode node = (ProtonNode) obj;
        return address.equals(node.address) && cluster.equals(node.cluster)
                && Objects.equals(credentials, node.credentials) && Objects.equals(database, node.database)
                && protocol == node.protocol && tags.equals(node.tags) && weight == node.weight
                && Objects.equals(tz, node.tz) && Objects.equals(version, node.version);
    }
}
