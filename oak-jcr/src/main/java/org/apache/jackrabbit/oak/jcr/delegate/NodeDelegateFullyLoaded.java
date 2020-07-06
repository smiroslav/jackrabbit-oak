package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;

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

    private Tree getTree(String relPath) throws RepositoryException {
        if (PathUtils.isAbsolute(relPath)) {
            throw new RepositoryException("Not a relative path: " + relPath);
        }

        return TreeUtil.getTree(this.getTree(), relPath);
    }
}
