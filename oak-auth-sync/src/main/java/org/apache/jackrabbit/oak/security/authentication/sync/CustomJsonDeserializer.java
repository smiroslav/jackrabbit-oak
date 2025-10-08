package org.apache.jackrabbit.oak.security.authentication.sync;

/*
 * ADOBE CONFIDENTIAL
 *
 * Copyright (c) 2024 Adobe
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe and its suppliers, if any. The intellectual
 * and technical concepts contained herein are proprietary to Adobe
 * and its suppliers and are protected by all applicable intellectual
 * property laws, including trade secret and copyright laws.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe.
 */

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import javax.jcr.Value;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Custom deserializer that parses JSOP/JSON and uses UserManager to add/edit users and their properties.
 */
public class CustomJsonDeserializer {
    private static final Set<String> IGNORED_PROPS = new HashSet<>();
    static {
        IGNORED_PROPS.add("jcr:primaryType");
        IGNORED_PROPS.add("jcr:createdBy");
        IGNORED_PROPS.add("jcr:created");
        IGNORED_PROPS.add("jcr:uuid");
        IGNORED_PROPS.add("rep:principalName");
    }

    private final UserManager userManager;
    private final ValueFactory valueFactory;

    public CustomJsonDeserializer(@NotNull UserManager userManager, @NotNull ValueFactory valueFactory) {
        this.userManager = userManager;
        this.valueFactory = valueFactory;
    }

    /**
     * Apply the JSOP string, creating or editing users as described.
     */
    public void applyJsop(String jsop) throws RepositoryException {
        if (jsop == null || jsop.trim().isEmpty()) return;
        JsopTokenizer r = new JsopTokenizer(jsop);
        while (true) {
            int op = r.read();
            if (op == JsopReader.END) break;
            String path = r.readString();
            switch (op) {
                case '+':
                    r.read(':');
                    if (r.matches('{')) {
                        // Only process if this is a user node
                        processAdd(path, r);
                    }
                    break;
                case '^':
                    r.read(':');
                    String propName = PathUtils.getName(path);
                    String parentPath = PathUtils.getParentPath(path);
                    processEdit(parentPath, propName, r);
                    break;
                // ignore '-' (removal) and others
                default:
                    skipValue(r);
            }
        }
    }

    private void processAdd(String path, JsopTokenizer r) throws RepositoryException {
        // Walk down the JSON object tree to find user nodes
        walkAndCreateUser(path, r);
    }

    private void walkAndCreateUser(String path, JsopTokenizer r) throws RepositoryException {
        // Read the object at this level
        Map<String, Object> props = new HashMap<>();
        while (!r.matches('}')) {
            String key = r.readString();
            r.read(':');
            if (r.matches('{')) {
                // Recursively process child node
                walkAndCreateUser(PathUtils.concat(path, key), r);
            } else {
                Object value = readSimpleValue(r);
                props.put(key, value);
            }
            r.matches(',');
        }
        // Is this a user or group node?
        if ("nam:rep:User".equals(props.get("jcr:primaryType")) && props.containsKey("rep:authorizableId")) {
            String userId = String.valueOf(props.get("rep:authorizableId"));
            String password = props.containsKey("rep:password") ? String.valueOf(props.get("rep:password")) : null;
            User user = userManager.createUser(userId, password);
            // Set all other properties except ignored
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                String k = entry.getKey();
                if (IGNORED_PROPS.contains(k) || k.equals("rep:authorizableId") || k.equals("rep:password")) continue;
                setUserProperty(user, k, entry.getValue());
            }
        } else if ("nam:rep:Group".equals(props.get("jcr:primaryType")) && props.containsKey("rep:authorizableId")) {
            String groupId = String.valueOf(props.get("rep:authorizableId"));
            org.apache.jackrabbit.api.security.user.Group group = userManager.createGroup(groupId);
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                String k = entry.getKey();
                if (IGNORED_PROPS.contains(k) || k.equals("rep:authorizableId")) continue;
                setUserProperty(group, k, entry.getValue());
            }
        }
    }

    private Map<String, Object> readObject(JsopTokenizer r) {
        Map<String, Object> map = new HashMap<>();
        while (!r.matches('}')) {
            String key = r.readString();
            r.read(':');
            if (r.matches('{')) {
                map.put(key, readObject(r));
            } else {
                map.put(key, readSimpleValue(r));
            }
            r.matches(',');
        }
        return map;
    }

    private Object readSimpleValue(JsopTokenizer r) {
        if (r.matches(JsopReader.STRING)) return r.getToken();
        if (r.matches(JsopReader.NUMBER)) return r.getToken();
        if (r.matches(JsopReader.TRUE)) return Boolean.TRUE;
        if (r.matches(JsopReader.FALSE)) return Boolean.FALSE;
        if (r.matches(JsopReader.NULL)) return null;
        return null;
    }

    private void setUserProperty(Authorizable authorizable, String key, Object value) throws RepositoryException {
        if (value == null) return;
        Value jcrValue;
        if (value instanceof Boolean) {
            jcrValue = valueFactory.createValue((Boolean) value);
        } else if (value instanceof Long) {
            jcrValue = valueFactory.createValue((Long) value);
        } else if (value instanceof Double) {
            jcrValue = valueFactory.createValue((Double) value);
        } else {
            jcrValue = valueFactory.createValue(value.toString());
        }
        authorizable.setProperty(key, jcrValue);
    }

    private void setUserSubtree(Authorizable authorizable, String prefix, Map<String, Object> subtree) throws RepositoryException {
        for (Map.Entry<String, Object> entry : subtree.entrySet()) {
            String k = prefix + "/" + entry.getKey();
            Object v = entry.getValue();
            if (v instanceof Map) {
                setUserSubtree(authorizable, k, (Map<String, Object>) v);
            } else {
                setUserProperty(authorizable, k, v);
            }
        }
    }

    private void processEdit(String parentPath, String propName, JsopTokenizer r) throws RepositoryException {
        // Edit properties for user or group nodes
        Authorizable auth = findUserByPath(parentPath);
        if (auth != null && (auth instanceof User || auth instanceof org.apache.jackrabbit.api.security.user.Group)) {
            if (r.matches(JsopReader.NULL)) {
                auth.removeProperty(propName);
            } else if (r.matches('[')) {
                // Special handling for rep:members on groups
                if ("rep:members".equals(propName) && auth instanceof org.apache.jackrabbit.api.security.user.Group) {
                    org.apache.jackrabbit.api.security.user.Group group = (org.apache.jackrabbit.api.security.user.Group) auth;
                    while (!r.matches(']')) {
                        if (r.matches(JsopReader.STRING)) {
                            String memberValue = r.getToken();
                            if (memberValue.startsWith("+")) {
                                String memberId = memberValue.substring(1);
                                Authorizable member = userManager.getAuthorizable(memberId);
                                if (member != null) {
                                    group.addMember(member);
                                }
                            } else if (memberValue.startsWith("-")) {
                                String memberId = memberValue.substring(1);
                                Authorizable member = userManager.getAuthorizable(memberId);
                                if (member != null) {
                                    group.removeMember(member);
                                }
                            }
                        } else {
                            r.read(); // skip unexpected token
                        }
                        r.matches(',');
                    }
                } else {
                    skipArray(r); // Array property, not supported in this simple version
                }
            } else {
                Object value = readSimpleValue(r);
                setUserProperty(auth, propName, value); // setUserProperty works for both User and Group
            }
        } else {
            skipValue(r);
        }
    }

    private Authorizable findUserByPath(String path) throws RepositoryException {
        // Path is like /rep:security/rep:authorizables/rep:users/t/te/testUser
        java.util.List<String> elements = new java.util.ArrayList<>();
        for (String el : PathUtils.elements(path)) {
            elements.add(el);
        }
        if (elements.size() > 0) {
            String userId = elements.get(elements.size() - 1);
            return userManager.getAuthorizable(userId);
        }
        return null;
    }

    private void skipValue(JsopTokenizer r) {
        // Skip over the next value (object, array, or primitive)
        if (r.matches('{')) {
            int depth = 1;
            while (depth > 0) {
                if (r.matches('{')) depth++;
                else if (r.matches('}')) depth--;
                else r.read();
            }
        } else if (r.matches('[')) {
            int depth = 1;
            while (depth > 0) {
                if (r.matches('[')) depth++;
                else if (r.matches(']')) depth--;
                else r.read();
            }
        } else {
            r.read();
        }
    }

    private void skipArray(JsopTokenizer r) {
        int depth = 1;
        while (depth > 0) {
            if (r.matches('[')) depth++;
            else if (r.matches(']')) depth--;
            else r.read();
        }
    }

    // Helper to convert a Map<String,Object> to a JsopTokenizer for recursion
    private JsopTokenizer toJsopTokenizer(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append('"').append(entry.getKey()).append('"').append(":");
            Object v = entry.getValue();
            if (v instanceof Map) {
                sb.append(toJsopString((Map<String, Object>) v));
            } else if (v instanceof String) {
                sb.append('"').append(v).append('"');
            } else if (v == null) {
                sb.append("null");
            } else {
                sb.append(v.toString());
            }
        }
        sb.append("}");
        return new JsopTokenizer(sb.toString());
    }

    private String toJsopString(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append('"').append(entry.getKey()).append('"').append(":");
            Object v = entry.getValue();
            if (v instanceof Map) {
                sb.append(toJsopString((Map<String, Object>) v));
            } else if (v instanceof String) {
                sb.append('"').append(v).append('"');
            } else if (v == null) {
                sb.append("null");
            } else {
                sb.append(v.toString());
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
