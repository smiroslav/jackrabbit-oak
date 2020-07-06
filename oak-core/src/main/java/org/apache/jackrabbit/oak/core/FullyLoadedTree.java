package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractMutableTree;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.TreeNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class FullyLoadedTree extends AbstractMutableTree {

    private MutableTree mutableTree;
    private TreeNode subtree;
    private FullyLoadedTree parent;
    private String name;
    public FullyLoadedTree(Tree tree) {
        this.mutableTree = (MutableTree) tree;
        this.subtree = this.mutableTree.getNodeState().loadSubtree();
        this.name = tree.getName();
    }

    public FullyLoadedTree( String name, TreeNode treeNode) {
        //this.mutableTree = (MutableTree) tree;
        this.subtree = treeNode;
        this.name = name;
    }

    @Override
    protected @NotNull Iterable<String> getChildNames() {
        return subtree.getChildren().keySet();
    }

    @Override
    public String toString() {
        return "FullyLoadedTree";
    }

    @Override
    public boolean isRoot() {
        return getParentOrNull() == null;
    }

    @Override
    public @NotNull Status getStatus() {
        return super.getStatus();
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public @NotNull AbstractTree getParent() {
        return parent != null? parent : mutableTree.getParent();
    }

    //
    @Override
    public @NotNull Tree getChild(@NotNull String name) throws IllegalArgumentException {
        TreeNode childNode = subtree.getChildren().get(name);

        return new FullyLoadedTree(name, childNode);
    }

    @Override
    public @Nullable PropertyState getProperty(@NotNull String name) {
        //return super.getProperty(name);
        return this.subtree.getProperties().get(name);
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return super.hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return super.getPropertyCount();
    }

    @Override
    public @Nullable Status getPropertyStatus(@NotNull String name) {
        return super.getPropertyStatus(name);
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return super.getProperties();
    }

    @Override
    public boolean hasChild(@NotNull String name) {
        return super.hasChild(name);
    }

    @Override
    public long getChildrenCount(long max) {
        return super.getChildrenCount(max);
    }

    @Override
    public @NotNull Iterable<Tree> getChildren() {
        return super.getChildren();
    }

    @Override
    protected @NotNull AbstractTree createChild(@NotNull String name) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected @Nullable AbstractTree getParentOrNull() {
        return parent != null? parent : mutableTree.getParentOrNull();
    }

    @Override
    protected @NotNull NodeBuilder getNodeBuilder() {
        return mutableTree.getNodeBuilder();
    }

    @Override
    public @NotNull String getName() {
        return name;
    }
}
