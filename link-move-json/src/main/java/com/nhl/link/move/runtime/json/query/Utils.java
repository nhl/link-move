package com.nhl.link.move.runtime.json.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.util.List;

public class Utils {

    public static boolean isValueNode(List<JsonNode> wrappedNode) {
        return wrappedNode != null && wrappedNode.size() == 1 && wrappedNode.get(0).isValueNode();
    }

    public static boolean isValueMissing(JsonNode node) {
        return node == null || node.getNodeType() == JsonNodeType.MISSING || node.getNodeType() == JsonNodeType.NULL;
    }

    public static JsonNode unwrapValueNode(List<JsonNode> wrappedNode) {
        if (wrappedNode == null || wrappedNode.isEmpty()) {
            return null;
        }
        if (wrappedNode.size() > 1) {
            throw new RuntimeException("Unexpected number of results (must be 0 or 1): " + wrappedNode.size());
        }
        JsonNode valueNode = wrappedNode.get(0);
        if (!valueNode.isValueNode()) {
            throw new RuntimeException("Expected value node as result, but received: " + valueNode.getNodeType().name());
        }
        return valueNode;
    }

    /**
     * @return For strings, number and booleans - returns value of java.lang.Comparable#compareTo
     * @throws RuntimeException for other types
     */
    public static int compare(JsonNode node1, JsonNode node2) {

        if (hasComparableType(node1) && hasComparableType(node2) && ofEqualTypes(node1, node2)) {
            switch (node1.getNodeType()) {
                case STRING: {
                    return node1.textValue().compareTo(node2.textValue());
                }
                case BOOLEAN: {
                    Boolean left = node1.asBoolean(), right = node2.asBoolean();
                    return left.compareTo(right);
                }
                case NUMBER: {
                    if (node1.isIntegralNumber()) {
                        Integer left = node1.asInt(), right = node2.asInt();
                        return left.compareTo(right);
                    } else if (node1.isFloatingPointNumber()) {
                        Double left = node1.asDouble(), right = node2.asDouble();
                        return left.compareTo(right);
                    }
                    // fall through
                }
                default: {
                    // fall through
                }
            }
        }
        throw new RuntimeException("Unsupported comparable type: " + node1.getNodeType().name());
    }

    private static boolean ofEqualTypes(JsonNode... nodes) {

        if (nodes == null || nodes.length == 0) {
            return false;
        }
        JsonNodeType type = nodes[0].getNodeType();
        for (int i = 1; i < nodes.length; i++) {
            if (nodes[i] == null || nodes[i].getNodeType() != type) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasComparableType(JsonNode node) {
        return hasType(node, JsonNodeType.NUMBER, JsonNodeType.BOOLEAN, JsonNodeType.STRING);
    }

    private static boolean hasType(JsonNode node, JsonNodeType... types) {

        if (node == null) {
            return false;
        }
        for (JsonNodeType type : types) {
            if (node.getNodeType() == type) {
                return true;
            }
        }
        return false;
    }
}
