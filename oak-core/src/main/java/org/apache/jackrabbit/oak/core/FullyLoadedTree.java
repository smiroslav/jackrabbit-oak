package org.apache.jackrabbit.oak.core;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractMutableTree;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.TreeNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;

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

    FullyLoadedTree( String name, TreeNode treeNode, FullyLoadedTree parent, MutableTree mutableTree) {
        //this.mutableTree = (MutableTree) tree;
        this.subtree = treeNode;
        this.name = name;
        this.parent = parent;
        this.mutableTree = mutableTree;
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
        return subtree != null;
    }

    @Override
    public @NotNull AbstractTree getParent() {
        return parent != null? parent : mutableTree.getParent();
    }

    //
    @Override
    public @NotNull Tree getChild(@NotNull String name) throws IllegalArgumentException {
        TreeNode childNode = subtree.getChildren().get(name);

        return new FullyLoadedTree(name, childNode, this, (MutableTree) mutableTree.getChild(name));
    }

    @Override
    public @Nullable PropertyState getProperty(@NotNull String name) {
        return this.subtree.getProperties().get(name);
    }

    @Override
    public void setProperty(@NotNull PropertyState property) {
        super.setProperty(property);
        this.subtree.getProperties().put(property.getName(), property);
    }

    @Override
    protected @NotNull AbstractTree createChild(@NotNull String name) throws IllegalArgumentException {
        TreeNode childTreeNode = new TreeNode(name, subtree.getPath() + "/" + name);
        subtree.getChildren().put(name, childTreeNode);
        return new FullyLoadedTree(name, childTreeNode, this, (MutableTree) mutableTree.getChild(name));
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return subtree.getProperties().containsKey(name);
    }

    @Override
    public boolean hasChild(@NotNull String name) {
        return subtree.getChildren().containsKey(name);
    }


    public <T> void setProperty(@NotNull String name, @NotNull T value, @NotNull Type<T> type)
            throws IllegalArgumentException {
        super.setProperty(name, value, type);
        subtree.getProperties().put(name, PropertyStates.createProperty(name, checkNotNull(value), checkNotNull(type)));
    }

    @Override
    public long getPropertyCount() {
        return size(getProperties());
    }

    @Override
    public @Nullable Status getPropertyStatus(@NotNull String name) {
        return super.getPropertyStatus(name);
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        return filter(subtree.getProperties().values(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState propertyState) {
                        return !isHidden(propertyState.getName());
                    }
                });
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
