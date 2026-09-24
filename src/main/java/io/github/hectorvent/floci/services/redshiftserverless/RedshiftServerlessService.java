package io.github.hectorvent.floci.services.redshiftserverless;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.PaginatedResult;
import io.github.hectorvent.floci.core.common.Pagination;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.redshiftserverless.model.Namespace;
import io.github.hectorvent.floci.services.redshiftserverless.model.RedshiftServerlessSnapshot;
import io.github.hectorvent.floci.services.redshiftserverless.model.RedshiftServerlessWorkgroup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

@ApplicationScoped
public class RedshiftServerlessService implements Resettable {
    public static final String DEFAULT_DB_NAME = "dev";
    public static final String AWS_OWNED_KMS_KEY = "AWS_OWNED_KMS_KEY";

    private static final Pattern NAMESPACE_NAME = Pattern.compile("[a-z0-9-]+");
    private static final Pattern DB_NAME = Pattern.compile("[a-zA-Z][a-zA-Z_0-9+.@-]*");
    private static final Set<String> LOG_EXPORTS = Set.of("useractivitylog", "userlog", "connectionlog");

    /**
     * Amazon Redshift's SQL reserved words, which CreateNamespace rejects as namespace names even
     * though they satisfy the length and character rules. Held lowercase because a namespace name
     * is already constrained to lowercase letters, digits and hyphens, so a direct lookup suffices.
     * Transcribed from https://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html
     */
    private static final Set<String> RESERVED_WORDS = Set.of(
            "aes128", "aes256", "all", "allowoverwrite", "analyse", "analyze",
            "and", "any", "array", "as", "asc", "authorization",
            "az64", "backup", "between", "binary", "blanksasnull", "both",
            "bytedict", "bzip2", "case", "cast", "check", "collate",
            "column", "constraint", "create", "credentials", "cross", "current_date",
            "current_time", "current_timestamp", "current_user", "current_user_id", "default", "deferrable",
            "deflate", "defrag", "delta", "delta32k", "desc", "disable",
            "distinct", "do", "else", "emptyasnull", "enable", "encode",
            "encrypt", "encryption", "end", "except", "explicit", "false",
            "for", "foreign", "freeze", "from", "full", "globaldict256",
            "globaldict64k", "grant", "group", "gzip", "having", "identity",
            "ignore", "ilike", "in", "initially", "inner", "intersect",
            "interval", "into", "is", "isnull", "join", "leading",
            "left", "like", "limit", "localtime", "localtimestamp", "lun",
            "luns", "lzo", "lzop", "minus", "mostly16", "mostly32",
            "mostly8", "natural", "new", "not", "notnull", "null",
            "nulls", "off", "offline", "offset", "oid", "old",
            "on", "only", "open", "or", "order", "outer",
            "overlaps", "parallel", "partition", "percent", "permissions", "pivot",
            "placing", "primary", "raw", "readratio", "recover", "references",
            "rejectlog", "resort", "respect", "restore", "right", "select",
            "session_user", "similar", "snapshot", "some", "sysdate", "system",
            "table", "tag", "tdes", "text255", "text32k", "then",
            "timestamp", "to", "top", "trailing", "true", "truncatecolumns",
            "union", "unique", "unnest", "unpivot", "user", "using",
            "verbose", "wallet", "when", "where", "with", "without");

    private final AccountAwareStorageBackend<Namespace> namespaces;
    private final AccountAwareStorageBackend<RedshiftServerlessWorkgroup> workgroups;
    private final AccountAwareStorageBackend<RedshiftServerlessSnapshot> snapshots;
    private final RegionResolver regionResolver;

    @Inject
    public RedshiftServerlessService(StorageFactory storageFactory, RegionResolver regionResolver) {
        this.namespaces = storageFactory.create("redshiftserverless", "redshiftserverless-namespaces.json",
                new TypeReference<Map<String, Namespace>>() {});
        this.workgroups = storageFactory.create("redshiftserverless", "redshiftserverless-workgroups.json",
                new TypeReference<Map<String, RedshiftServerlessWorkgroup>>() {});
        this.snapshots = storageFactory.create("redshiftserverless", "redshiftserverless-snapshots.json",
                new TypeReference<Map<String, RedshiftServerlessSnapshot>>() {});
        this.regionResolver = regionResolver;
    }

    public synchronized Namespace createNamespace(String namespaceName, String adminUsername, String dbName,
                                                  String kmsKeyId, String defaultIamRoleArn, List<String> iamRoles,
                                                  List<String> logExports, Map<String, String> tags, String region) {
        validateNamespaceName(namespaceName);
        String key = storageKey(region, namespaceName);
        if (namespaces.get(key).isPresent()) {
            throw new AwsException("ConflictException",
                    "The namespace " + namespaceName + " already exists.", 409);
        }

        Namespace namespace = new Namespace();
        namespace.setNamespaceName(namespaceName);
        namespace.setNamespaceId(UUID.randomUUID().toString());
        namespace.setNamespaceArn(regionResolver.buildArn("redshift-serverless", region,
                "namespace/" + namespace.getNamespaceId()));
        namespace.setAdminUsername(adminUsername);
        namespace.setDbName(validateDbName(dbName));
        namespace.setKmsKeyId(kmsKeyId == null || kmsKeyId.isBlank() ? AWS_OWNED_KMS_KEY : kmsKeyId);
        namespace.setDefaultIamRoleArn(defaultIamRoleArn);
        namespace.setIamRoles(copyOf(iamRoles));
        namespace.setLogExports(validateLogExports(logExports));
        namespace.setStatus("AVAILABLE");
        namespace.setCreationDate(Instant.now());
        namespace.setTags(tags == null ? new LinkedHashMap<>() : new LinkedHashMap<>(tags));
        namespaces.put(key, namespace);
        return namespace;
    }

    public Namespace getNamespace(String namespaceName, String region) {
        validateNamespaceName(namespaceName);
        return namespaces.get(storageKey(region, namespaceName))
                .orElseThrow(() -> notFound(namespaceName));
    }

    public PaginatedResult<Namespace> listNamespaces(String region, Integer maxResults, String nextToken) {
        List<Namespace> all = namespaces.scan(key -> key.startsWith(region + "::"));
        return Pagination.paginate(all, Namespace::getNamespaceName, maxResults, nextToken,
                100, 100, "ValidationException");
    }

    /**
     * Builds the new state on a copy and stores that, rather than mutating the stored instance.
     * Reads do not take the monitor this method holds, so an in-place mutation lets a concurrent
     * GetNamespace or ListNamespaces observe a torn object: some fields updated, some not.
     */
    public synchronized Namespace updateNamespace(String namespaceName, String adminUsername, String kmsKeyId,
                                                  String defaultIamRoleArn, List<String> iamRoles,
                                                  List<String> logExports, String region) {
        String key = storageKey(region, namespaceName);
        Namespace updated = new Namespace(getNamespace(namespaceName, region));
        if (adminUsername != null) {
            updated.setAdminUsername(adminUsername);
        }
        if (kmsKeyId != null && !kmsKeyId.isBlank()) {
            updated.setKmsKeyId(kmsKeyId);
        }
        if (defaultIamRoleArn != null) {
            updated.setDefaultIamRoleArn(defaultIamRoleArn);
        }
        if (iamRoles != null) {
            updated.setIamRoles(copyOf(iamRoles));
        }
        if (logExports != null) {
            updated.setLogExports(validateLogExports(logExports));
        }
        namespaces.put(key, updated);
        return updated;
    }

    public synchronized Namespace deleteNamespace(String namespaceName, String region) {
        Namespace deleted = new Namespace(getNamespace(namespaceName, region));
        namespaces.delete(storageKey(region, namespaceName));
        deleted.setStatus("DELETING");
        return deleted;
    }

    public synchronized RedshiftServerlessWorkgroup createWorkgroup(RedshiftServerlessWorkgroup workgroup,
                                                                    String region) {
        if (workgroup.getWorkgroupName() == null || workgroup.getWorkgroupName().isBlank()) {
            throw validation("workgroupName is required.");
        }
        if (workgroup.getNamespaceName() == null || workgroup.getNamespaceName().isBlank()) {
            throw validation("namespaceName is required.");
        }
        // Real CreateWorkgroup requires the namespace to already exist.
        getNamespace(workgroup.getNamespaceName(), region);

        String key = storageKey(region, workgroup.getWorkgroupName());
        if (workgroups.get(key).isPresent()) {
            throw new AwsException("ConflictException",
                    "The workgroup " + workgroup.getWorkgroupName() + " already exists.", 409);
        }
        workgroup.setWorkgroupId(UUID.randomUUID().toString());
        workgroup.setRegion(region);
        workgroup.setAccountId(regionResolver.getAccountId());
        workgroup.setWorkgroupArn(regionResolver.buildArn("redshift-serverless", region,
                "workgroup/" + workgroup.getWorkgroupId()));
        workgroup.setStatus("AVAILABLE");
        workgroup.setEndpointAddress(workgroup.getWorkgroupName() + "." + regionResolver.getAccountId()
                + "." + region + ".redshift-serverless.amazonaws.com");
        workgroup.setEndpointPort(workgroup.getPort() != null ? workgroup.getPort() : 5439);
        workgroup.setCreationDate(Instant.now());
        workgroups.put(key, workgroup);
        return workgroup;
    }

    public RedshiftServerlessWorkgroup getWorkgroup(String workgroupName, String region) {
        return workgroups.get(storageKey(region, workgroupName))
                .orElseThrow(() -> new AwsException("ResourceNotFoundException",
                        "The workgroup " + workgroupName + " was not found.", 404));
    }

    public PaginatedResult<RedshiftServerlessWorkgroup> listWorkgroups(String region, Integer maxResults,
                                                                      String nextToken) {
        List<RedshiftServerlessWorkgroup> all = workgroups.scan(key -> key.startsWith(region + "::"));
        return Pagination.paginate(all, RedshiftServerlessWorkgroup::getWorkgroupName, maxResults, nextToken,
                100, 100, "ValidationException");
    }

    public synchronized RedshiftServerlessWorkgroup updateWorkgroup(String workgroupName,
                                                                    RedshiftServerlessWorkgroup patch,
                                                                    String region) {
        RedshiftServerlessWorkgroup existing = getWorkgroup(workgroupName, region);
        if (patch.getBaseCapacity() != null) {
            existing.setBaseCapacity(patch.getBaseCapacity());
        }
        if (patch.getMaxCapacity() != null) {
            existing.setMaxCapacity(patch.getMaxCapacity());
        }
        if (patch.getEnhancedVpcRouting() != null) {
            existing.setEnhancedVpcRouting(patch.getEnhancedVpcRouting());
        }
        if (patch.getPubliclyAccessible() != null) {
            existing.setPubliclyAccessible(patch.getPubliclyAccessible());
        }
        if (!patch.getSecurityGroupIds().isEmpty()) {
            existing.setSecurityGroupIds(patch.getSecurityGroupIds());
        }
        if (!patch.getSubnetIds().isEmpty()) {
            existing.setSubnetIds(patch.getSubnetIds());
        }
        if (patch.getPort() != null) {
            existing.setPort(patch.getPort());
            existing.setEndpointPort(patch.getPort());
        }
        workgroups.put(storageKey(region, workgroupName), existing);
        return existing;
    }

    public synchronized RedshiftServerlessWorkgroup deleteWorkgroup(String workgroupName, String region) {
        RedshiftServerlessWorkgroup deleted = getWorkgroup(workgroupName, region);
        workgroups.delete(storageKey(region, workgroupName));
        deleted.setStatus("DELETING");
        return deleted;
    }

    /**
     * Creates a snapshot directly in {@code region}. Real {@code CreateSnapshot} always creates
     * in the source namespace's own region; a snapshot that should appear as a cross-region copy
     * is seeded by calling this against the standby region directly.
     */
    public synchronized RedshiftServerlessSnapshot createSnapshot(String snapshotName, String namespaceName,
                                                                  String region) {
        if (snapshotName == null || snapshotName.isBlank()) {
            throw validation("snapshotName is required.");
        }
        Namespace namespace = getNamespace(namespaceName, region);
        String key = storageKey(region, snapshotName);
        if (snapshots.get(key).isPresent()) {
            throw new AwsException("ConflictException",
                    "The snapshot " + snapshotName + " already exists.", 409);
        }
        RedshiftServerlessSnapshot snapshot = new RedshiftServerlessSnapshot();
        snapshot.setSnapshotName(snapshotName);
        snapshot.setNamespaceName(namespaceName);
        snapshot.setNamespaceArn(namespace.getNamespaceArn());
        snapshot.setRegion(region);
        snapshot.setAccountId(regionResolver.getAccountId());
        snapshot.setOwnerAccount(regionResolver.getAccountId());
        snapshot.setSnapshotArn(regionResolver.buildArn("redshift-serverless", region, "snapshot/" + snapshotName));
        snapshot.setStatus("AVAILABLE");
        snapshot.setSnapshotCreateTime(Instant.now());
        snapshots.put(key, snapshot);
        return snapshot;
    }

    public RedshiftServerlessSnapshot getSnapshot(String snapshotName, String region) {
        return snapshots.get(storageKey(region, snapshotName))
                .orElseThrow(() -> new AwsException("ResourceNotFoundException",
                        "The snapshot " + snapshotName + " was not found.", 404));
    }

    public synchronized RedshiftServerlessSnapshot deleteSnapshot(String snapshotName, String region) {
        RedshiftServerlessSnapshot deleted = getSnapshot(snapshotName, region);
        snapshots.delete(storageKey(region, snapshotName));
        return deleted;
    }

    public PaginatedResult<RedshiftServerlessSnapshot> listSnapshots(String namespaceName, String region,
                                                                     Integer maxResults, String nextToken) {
        List<RedshiftServerlessSnapshot> all = snapshots.scan(key -> key.startsWith(region + "::")).stream()
                .filter(snapshot -> namespaceName == null || namespaceName.isBlank()
                        || namespaceName.equals(snapshot.getNamespaceName()))
                .toList();
        return Pagination.paginate(all, RedshiftServerlessSnapshot::getSnapshotName, maxResults, nextToken,
                100, 100, "ValidationException");
    }

    /**
     * Restores into the already-existing namespace and workgroup in {@code region} and never
     * creates either, mirroring real {@code RestoreFromSnapshot}.
     */
    public synchronized Namespace restoreFromSnapshot(String namespaceName, String workgroupName,
                                                      String snapshotName, String region) {
        getSnapshot(snapshotName, region);
        Namespace restored = new Namespace(getNamespace(namespaceName, region));
        getWorkgroup(workgroupName, region);
        restored.setStatus("AVAILABLE");
        namespaces.put(storageKey(region, namespaceName), restored);
        return restored;
    }

    public Map<String, String> listTagsForResource(String resourceArn, String region) {
        requireResourceArn(resourceArn);
        Optional<Namespace> namespace = namespaceByArn(resourceArn, region);
        if (namespace.isPresent()) {
            return new LinkedHashMap<>(namespace.get().getTags());
        }
        return new LinkedHashMap<>(workgroupByArn(resourceArn, region).getTags());
    }

    public synchronized Map<String, String> tagResource(String resourceArn, Map<String, String> tags, String region) {
        requireResourceArn(resourceArn);
        Optional<Namespace> namespace = namespaceByArn(resourceArn, region);
        if (namespace.isPresent()) {
            Namespace updated = new Namespace(namespace.get());
            if (tags != null) {
                updated.getTags().putAll(tags);
            }
            namespaces.put(storageKey(region, updated.getNamespaceName()), updated);
            return new LinkedHashMap<>(updated.getTags());
        }
        RedshiftServerlessWorkgroup workgroup = workgroupByArn(resourceArn, region);
        if (tags != null) {
            workgroup.getTags().putAll(tags);
        }
        workgroups.put(storageKey(region, workgroup.getWorkgroupName()), workgroup);
        return new LinkedHashMap<>(workgroup.getTags());
    }

    public synchronized Map<String, String> untagResource(String resourceArn, List<String> tagKeys, String region) {
        requireResourceArn(resourceArn);
        Optional<Namespace> namespace = namespaceByArn(resourceArn, region);
        RedshiftServerlessWorkgroup workgroup = namespace.isPresent() ? null : workgroupByArn(resourceArn, region);
        if (tagKeys == null) {
            throw validation("tagKeys is required.");
        }
        if (namespace.isPresent()) {
            Namespace updated = new Namespace(namespace.get());
            tagKeys.forEach(updated.getTags()::remove);
            namespaces.put(storageKey(region, updated.getNamespaceName()), updated);
            return new LinkedHashMap<>(updated.getTags());
        }
        tagKeys.forEach(workgroup.getTags()::remove);
        workgroups.put(storageKey(region, workgroup.getWorkgroupName()), workgroup);
        return new LinkedHashMap<>(workgroup.getTags());
    }

    /**
     * Redshift Serverless tags whatever the ARN names, so the lookup is by ARN rather than by
     * resource name. Namespaces and workgroups are taggable in Floci; any other Redshift
     * Serverless ARN resolves to nothing and is reported as absent rather than as an unsupported
     * resource type.
     */
    private Optional<Namespace> namespaceByArn(String resourceArn, String region) {
        return namespaces.scan(key -> key.startsWith(region + "::")).stream()
                .filter(namespace -> resourceArn.equals(namespace.getNamespaceArn()))
                .findFirst();
    }

    private RedshiftServerlessWorkgroup workgroupByArn(String resourceArn, String region) {
        return workgroups.scan(key -> key.startsWith(region + "::")).stream()
                .filter(workgroup -> resourceArn.equals(workgroup.getWorkgroupArn()))
                .findFirst()
                .orElseThrow(() -> new AwsException("ResourceNotFoundException",
                        "The resource " + resourceArn + " was not found.", 404));
    }

    private static void requireResourceArn(String resourceArn) {
        if (resourceArn == null || resourceArn.isBlank()) {
            throw validation("resourceArn is required.");
        }
    }

    @Override
    public void clear() {
        namespaces.clear();
        workgroups.clear();
        snapshots.clear();
    }

    private static void validateNamespaceName(String namespaceName) {
        if (namespaceName == null || namespaceName.length() < 3 || namespaceName.length() > 64
                || !NAMESPACE_NAME.matcher(namespaceName).matches()) {
            throw validation("namespaceName must be 3-64 characters of lowercase letters, numbers, and hyphens.");
        }
        if (RESERVED_WORDS.contains(namespaceName)) {
            throw validation("namespaceName must not be an Amazon Redshift reserved word.");
        }
    }

    private static String validateDbName(String dbName) {
        if (dbName == null || dbName.isBlank()) {
            return DEFAULT_DB_NAME;
        }
        if (dbName.length() > 127 || !DB_NAME.matcher(dbName).matches()) {
            throw validation("dbName must start with a letter and be at most 127 characters.");
        }
        return dbName;
    }

    private static List<String> validateLogExports(List<String> logExports) {
        List<String> validated = copyOf(logExports);
        for (String logExport : validated) {
            if (!LOG_EXPORTS.contains(logExport)) {
                throw validation("logExports must contain only useractivitylog, userlog, and connectionlog.");
            }
        }
        return validated;
    }

    private static List<String> copyOf(List<String> values) {
        return values == null ? new ArrayList<>() : new ArrayList<>(values);
    }

    private static String storageKey(String region, String name) {
        return region + "::" + name;
    }

    private static AwsException notFound(String namespaceName) {
        return new AwsException("ResourceNotFoundException",
                "The namespace " + namespaceName + " was not found.", 404);
    }

    private static AwsException validation(String message) {
        return new AwsException("ValidationException", message, 400);
    }
}
