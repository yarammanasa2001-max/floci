package io.github.hectorvent.floci.services.redshiftserverless;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.core.common.AwsErrorResponse;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.JsonErrorResponseUtils;
import io.github.hectorvent.floci.core.common.PaginatedResult;
import io.github.hectorvent.floci.services.redshiftserverless.model.Namespace;
import io.github.hectorvent.floci.services.redshiftserverless.model.RedshiftServerlessSnapshot;
import io.github.hectorvent.floci.services.redshiftserverless.model.RedshiftServerlessWorkgroup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Redshift Serverless JSON 1.1 handler. Dispatched from
 * {@link io.github.hectorvent.floci.core.common.AwsJson11Controller}
 * under the {@code RedshiftServerless.} target prefix.
 */
@ApplicationScoped
public class RedshiftServerlessJsonHandler {

    private static final Logger LOG = Logger.getLogger(RedshiftServerlessJsonHandler.class);

    /**
     * {@code Namespace.creationDate}, {@code Workgroup.creationDate} and
     * {@code Snapshot.snapshotCreateTime} carry {@code TimestampFormatTrait(ISO_8601)}, which
     * overrides the epoch-seconds default that awsJson1.1 would otherwise apply. Do not
     * generalise: roughly half the timestamp members in this model carry no format trait and
     * use the epoch default, so check each member's trait before emitting it. Emitting a number here is accepted by the CLI, because
     * botocore coerces it, but strict SDKs reject the response outright.
     */
    private static final DateTimeFormatter CREATION_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final RedshiftServerlessService service;
    private final ObjectMapper objectMapper;

    @Inject
    public RedshiftServerlessJsonHandler(RedshiftServerlessService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    public Response handle(String action, JsonNode request, String region) {
        LOG.debugv("Redshift Serverless action: {0}", action);
        try {
            return switch (action) {
                case "CreateNamespace" -> handleCreateNamespace(request, region);
                case "GetNamespace" -> handleGetNamespace(request, region);
                case "ListNamespaces" -> handleListNamespaces(request, region);
                case "UpdateNamespace" -> handleUpdateNamespace(request, region);
                case "DeleteNamespace" -> handleDeleteNamespace(request, region);
                case "CreateWorkgroup" -> handleCreateWorkgroup(request, region);
                case "GetWorkgroup" -> handleGetWorkgroup(request, region);
                case "ListWorkgroups" -> handleListWorkgroups(request, region);
                case "UpdateWorkgroup" -> handleUpdateWorkgroup(request, region);
                case "DeleteWorkgroup" -> handleDeleteWorkgroup(request, region);
                case "CreateSnapshot" -> handleCreateSnapshot(request, region);
                case "GetSnapshot" -> handleGetSnapshot(request, region);
                case "ListSnapshots" -> handleListSnapshots(request, region);
                case "DeleteSnapshot" -> handleDeleteSnapshot(request, region);
                case "RestoreFromSnapshot" -> handleRestoreFromSnapshot(request, region);
                case "ListTagsForResource" -> handleListTagsForResource(request, region);
                case "TagResource" -> handleTagResource(request, region);
                case "UntagResource" -> handleUntagResource(request, region);
                default -> Response.status(400)
                        .entity(new AwsErrorResponse("UnknownOperationException",
                                "Operation " + action + " is not supported."))
                        .build();
            };
        } catch (AwsException e) {
            return JsonErrorResponseUtils.createErrorResponse(e);
        } catch (Exception e) {
            LOG.errorf(e, "Redshift Serverless error processing action %s", action);
            return JsonErrorResponseUtils.createErrorResponse(e);
        }
    }

    private Response handleCreateNamespace(JsonNode request, String region) {
        Namespace namespace = service.createNamespace(
                text(request, "namespaceName"),
                text(request, "adminUsername"),
                text(request, "dbName"),
                text(request, "kmsKeyId"),
                text(request, "defaultIamRoleArn"),
                parseStringList(request.path("iamRoles"), "iamRoles"),
                parseStringList(request.path("logExports"), "logExports"),
                parseTagList(request.path("tags"), "tags"),
                region);
        return namespaceResponse(namespace);
    }

    private Response handleGetNamespace(JsonNode request, String region) {
        return namespaceResponse(service.getNamespace(text(request, "namespaceName"), region));
    }

    private Response handleListNamespaces(JsonNode request, String region) {
        PaginatedResult<Namespace> page = service.listNamespaces(
                region, parseMaxResults(request), text(request, "nextToken"));
        ObjectNode response = objectMapper.createObjectNode();
        ArrayNode items = response.putArray("namespaces");
        page.items().forEach(namespace -> items.add(namespaceNode(namespace)));
        if (page.nextToken() != null) {
            response.put("nextToken", page.nextToken());
        }
        return Response.ok(response).build();
    }

    private Response handleUpdateNamespace(JsonNode request, String region) {
        Namespace namespace = service.updateNamespace(
                text(request, "namespaceName"),
                text(request, "adminUsername"),
                text(request, "kmsKeyId"),
                text(request, "defaultIamRoleArn"),
                parseStringList(request.path("iamRoles"), "iamRoles"),
                parseStringList(request.path("logExports"), "logExports"),
                region);
        return namespaceResponse(namespace);
    }

    private Response handleDeleteNamespace(JsonNode request, String region) {
        return namespaceResponse(service.deleteNamespace(text(request, "namespaceName"), region));
    }

    private Response handleCreateWorkgroup(JsonNode request, String region) {
        RedshiftServerlessWorkgroup workgroup = parseWorkgroup(request);
        workgroup.setWorkgroupName(text(request, "workgroupName"));
        workgroup.setNamespaceName(text(request, "namespaceName"));
        Map<String, String> tags = parseTagList(request.path("tags"), "tags");
        if (tags != null) {
            workgroup.setTags(tags);
        }
        return workgroupResponse(service.createWorkgroup(workgroup, region));
    }

    private Response handleGetWorkgroup(JsonNode request, String region) {
        return workgroupResponse(service.getWorkgroup(text(request, "workgroupName"), region));
    }

    private Response handleListWorkgroups(JsonNode request, String region) {
        PaginatedResult<RedshiftServerlessWorkgroup> page = service.listWorkgroups(
                region, parseMaxResults(request), text(request, "nextToken"));
        ObjectNode response = objectMapper.createObjectNode();
        ArrayNode items = response.putArray("workgroups");
        page.items().forEach(workgroup -> items.add(workgroupNode(workgroup)));
        if (page.nextToken() != null) {
            response.put("nextToken", page.nextToken());
        }
        return Response.ok(response).build();
    }

    private Response handleUpdateWorkgroup(JsonNode request, String region) {
        return workgroupResponse(service.updateWorkgroup(
                text(request, "workgroupName"), parseWorkgroup(request), region));
    }

    private Response handleDeleteWorkgroup(JsonNode request, String region) {
        return workgroupResponse(service.deleteWorkgroup(text(request, "workgroupName"), region));
    }

    /**
     * Only the members present in the request are set, so the same parse serves CreateWorkgroup
     * and the partial-update semantics of UpdateWorkgroup.
     */
    private RedshiftServerlessWorkgroup parseWorkgroup(JsonNode request) {
        RedshiftServerlessWorkgroup workgroup = new RedshiftServerlessWorkgroup();
        workgroup.setBaseCapacity(intOrNull(request, "baseCapacity"));
        workgroup.setMaxCapacity(intOrNull(request, "maxCapacity"));
        workgroup.setEnhancedVpcRouting(boolOrNull(request, "enhancedVpcRouting"));
        workgroup.setPubliclyAccessible(boolOrNull(request, "publiclyAccessible"));
        List<String> securityGroupIds = parseStringList(request.path("securityGroupIds"), "securityGroupIds");
        if (securityGroupIds != null) {
            workgroup.setSecurityGroupIds(securityGroupIds);
        }
        List<String> subnetIds = parseStringList(request.path("subnetIds"), "subnetIds");
        if (subnetIds != null) {
            workgroup.setSubnetIds(subnetIds);
        }
        workgroup.setPort(intOrNull(request, "port"));
        return workgroup;
    }

    private Response handleCreateSnapshot(JsonNode request, String region) {
        return snapshotResponse(service.createSnapshot(
                text(request, "snapshotName"), text(request, "namespaceName"), region));
    }

    /**
     * GetSnapshot also accepts {@code snapshotArn}. Floci's snapshot ARNs end in the snapshot
     * name, so the name is recovered from the ARN rather than tracked in a second index.
     */
    private Response handleGetSnapshot(JsonNode request, String region) {
        String snapshotName = text(request, "snapshotName");
        if (snapshotName == null) {
            String arn = text(request, "snapshotArn");
            snapshotName = arn != null && arn.contains("/") ? arn.substring(arn.lastIndexOf('/') + 1) : null;
        }
        return snapshotResponse(service.getSnapshot(snapshotName, region));
    }

    private Response handleListSnapshots(JsonNode request, String region) {
        PaginatedResult<RedshiftServerlessSnapshot> page = service.listSnapshots(
                text(request, "namespaceName"), region, parseMaxResults(request), text(request, "nextToken"));
        ObjectNode response = objectMapper.createObjectNode();
        ArrayNode items = response.putArray("snapshots");
        page.items().forEach(snapshot -> items.add(snapshotNode(snapshot)));
        if (page.nextToken() != null) {
            response.put("nextToken", page.nextToken());
        }
        return Response.ok(response).build();
    }

    private Response handleDeleteSnapshot(JsonNode request, String region) {
        return snapshotResponse(service.deleteSnapshot(text(request, "snapshotName"), region));
    }

    private Response handleRestoreFromSnapshot(JsonNode request, String region) {
        String snapshotName = text(request, "snapshotName");
        Namespace namespace = service.restoreFromSnapshot(
                text(request, "namespaceName"), text(request, "workgroupName"), snapshotName, region);
        ObjectNode response = objectMapper.createObjectNode();
        response.set("namespace", namespaceNode(namespace));
        response.put("ownerAccount", service.getSnapshot(snapshotName, region).getOwnerAccount());
        response.put("snapshotName", snapshotName);
        return Response.ok(response).build();
    }

    private Response handleListTagsForResource(JsonNode request, String region) {
        Map<String, String> tags = service.listTagsForResource(text(request, "resourceArn"), region);
        ObjectNode response = objectMapper.createObjectNode();
        response.set("tags", tagListNode(tags));
        return Response.ok(response).build();
    }

    private Response handleTagResource(JsonNode request, String region) {
        service.tagResource(text(request, "resourceArn"), parseTagList(request.path("tags"), "tags"), region);
        return Response.ok(objectMapper.createObjectNode()).build();
    }

    private Response handleUntagResource(JsonNode request, String region) {
        service.untagResource(text(request, "resourceArn"), parseStringList(request.path("tagKeys"), "tagKeys"), region);
        return Response.ok(objectMapper.createObjectNode()).build();
    }

    private ArrayNode tagListNode(Map<String, String> tags) {
        ArrayNode node = objectMapper.createArrayNode();
        tags.forEach((key, value) -> {
            ObjectNode tag = node.addObject();
            tag.put("key", key);
            tag.put("value", value);
        });
        return node;
    }

    private Response namespaceResponse(Namespace namespace) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("namespace", namespaceNode(namespace));
        return Response.ok(response).build();
    }

    /**
     * {@code adminUserPassword} is deliberately absent: AWS never returns it on any namespace
     * operation, and the Terraform provider treats a returned value as a permanent diff.
     */
    private ObjectNode namespaceNode(Namespace namespace) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("namespaceName", namespace.getNamespaceName());
        node.put("namespaceId", namespace.getNamespaceId());
        node.put("namespaceArn", namespace.getNamespaceArn());
        node.put("dbName", namespace.getDbName());
        node.put("kmsKeyId", namespace.getKmsKeyId());
        node.put("status", namespace.getStatus());
        if (namespace.getAdminUsername() != null) {
            node.put("adminUsername", namespace.getAdminUsername());
        }
        if (namespace.getDefaultIamRoleArn() != null) {
            node.put("defaultIamRoleArn", namespace.getDefaultIamRoleArn());
        }
        putTimestamp(node, "creationDate", namespace.getCreationDate());
        ArrayNode iamRoles = node.putArray("iamRoles");
        namespace.getIamRoles().forEach(iamRoles::add);
        ArrayNode logExports = node.putArray("logExports");
        namespace.getLogExports().forEach(logExports::add);
        return node;
    }

    private Response workgroupResponse(RedshiftServerlessWorkgroup workgroup) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("workgroup", workgroupNode(workgroup));
        return Response.ok(response).build();
    }

    private ObjectNode workgroupNode(RedshiftServerlessWorkgroup workgroup) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("workgroupId", workgroup.getWorkgroupId());
        node.put("workgroupName", workgroup.getWorkgroupName());
        node.put("workgroupArn", workgroup.getWorkgroupArn());
        node.put("namespaceName", workgroup.getNamespaceName());
        if (workgroup.getBaseCapacity() != null) {
            node.put("baseCapacity", workgroup.getBaseCapacity());
        }
        if (workgroup.getMaxCapacity() != null) {
            node.put("maxCapacity", workgroup.getMaxCapacity());
        }
        node.put("enhancedVpcRouting", Boolean.TRUE.equals(workgroup.getEnhancedVpcRouting()));
        node.put("publiclyAccessible", Boolean.TRUE.equals(workgroup.getPubliclyAccessible()));
        ArrayNode securityGroupIds = node.putArray("securityGroupIds");
        workgroup.getSecurityGroupIds().forEach(securityGroupIds::add);
        ArrayNode subnetIds = node.putArray("subnetIds");
        workgroup.getSubnetIds().forEach(subnetIds::add);
        if (workgroup.getPort() != null) {
            node.put("port", workgroup.getPort());
        }
        node.put("status", workgroup.getStatus());
        if (workgroup.getEndpointAddress() != null) {
            ObjectNode endpoint = node.putObject("endpoint");
            endpoint.put("address", workgroup.getEndpointAddress());
            endpoint.put("port", workgroup.getEndpointPort() != null ? workgroup.getEndpointPort() : 5439);
            endpoint.putArray("vpcEndpoints");
        }
        putTimestamp(node, "creationDate", workgroup.getCreationDate());
        return node;
    }

    private Response snapshotResponse(RedshiftServerlessSnapshot snapshot) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("snapshot", snapshotNode(snapshot));
        return Response.ok(response).build();
    }

    private ObjectNode snapshotNode(RedshiftServerlessSnapshot snapshot) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("snapshotName", snapshot.getSnapshotName());
        node.put("snapshotArn", snapshot.getSnapshotArn());
        node.put("namespaceName", snapshot.getNamespaceName());
        node.put("namespaceArn", snapshot.getNamespaceArn());
        node.put("ownerAccount", snapshot.getOwnerAccount());
        node.put("status", snapshot.getStatus());
        putTimestamp(node, "snapshotCreateTime", snapshot.getSnapshotCreateTime());
        return node;
    }

    private static void putTimestamp(ObjectNode node, String field, Instant instant) {
        if (instant != null) {
            node.put(field, CREATION_DATE_FORMAT.format(instant));
        }
    }

    private Integer intOrNull(JsonNode request, String field) {
        JsonNode node = request.path(field);
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isInt()) {
            throw validation(field + " must be an integer.");
        }
        return node.asInt();
    }

    private Boolean boolOrNull(JsonNode request, String field) {
        JsonNode node = request.path(field);
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isBoolean()) {
            throw validation(field + " must be a boolean.");
        }
        return node.asBoolean();
    }

    private Integer parseMaxResults(JsonNode request) {
        JsonNode node = request.path("maxResults");
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isNumber()) {
            throw validation("maxResults must be an integer.");
        }
        return node.asInt();
    }

    /**
     * Absent means absent and wrong means wrong: only a missing or null member returns null, so
     * that callers can distinguish "omitted, keep the stored value" from "supplied". Treating a
     * present member of the wrong type as absent would let a malformed UpdateNamespace silently
     * keep the old roles instead of reporting the request as invalid.
     */
    private List<String> parseStringList(JsonNode node, String field) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isArray()) {
            throw validation(field + " must be an array of strings.");
        }
        List<String> list = new ArrayList<>();
        for (JsonNode element : node) {
            if (!element.isTextual()) {
                throw validation(field + " must contain only strings.");
            }
            list.add(element.textValue());
        }
        return list;
    }

    private Map<String, String> parseTagList(JsonNode tagsNode, String field) {
        if (tagsNode == null || tagsNode.isMissingNode() || tagsNode.isNull()) {
            return null;
        }
        if (!tagsNode.isArray()) {
            throw validation(field + " must be an array of tags.");
        }
        Map<String, String> tags = new LinkedHashMap<>();
        for (JsonNode tag : tagsNode) {
            if (!tag.isObject()) {
                throw validation(field + " must contain only tag objects.");
            }
            String key = tag.path("key").asText(null);
            if (key != null) {
                tags.put(key, tag.path("value").asText(null));
            }
        }
        return tags;
    }

    private static AwsException validation(String message) {
        return new AwsException("ValidationException", message, 400);
    }

    private static String text(JsonNode request, String field) {
        JsonNode node = request == null ? null : request.get(field);
        return node != null && node.isTextual() ? node.textValue() : null;
    }
}
