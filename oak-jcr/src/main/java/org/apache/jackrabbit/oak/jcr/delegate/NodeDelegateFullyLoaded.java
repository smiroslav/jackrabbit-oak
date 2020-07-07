package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

public class NodeDelegateFullyLoaded extends NodeDelegate{
    public NodeDelegateFullyLoaded(SessionDelegate sessionDelegate, Tree tree) {
        super(sessionDelegate, tree);
    }

    @Override
    public NodeDelegate getChild(String relPath) throws RepositoryException {
        if (relPath.isEmpty()) {
            return this;
        }
        Tree tree = getTree(relPath);
        return tree == null || !tree.exists() ? null : new NodeDelegateFullyLoaded(sessionDelegate, tree);
    }

    @Nullable
    public NodeDelegate addChild(String name, String typeName)
            throws RepositoryException {
        Tree tree = getTree();
        if (tree.hasChild(name)) {
            return null;
        }

        Tree typeRoot = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
        Tree child = TreeUtil.addChild(tree, name, typeName, typeRoot, sessionDelegate.getAuthInfo().getUserID());
        return new NodeDelegateFullyLoaded(sessionDelegate, child);
    }

    private Tree getTree(String relPath) throws RepositoryException {
        if (PathUtils.isAbsolute(relPath)) {
            throw new RepositoryException("Not a relative path: " + relPath);
        }

        return TreeUtil.getTree(this.getTree(), relPath);
    }
}
