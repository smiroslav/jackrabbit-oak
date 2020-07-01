package org.apache.jackrabbit.oak.store.remote;

import java.util.Objects;

public class Test {

    String a;
    long b;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Test test = (Test) o;
        return b == test.b &&
                Objects.equals(a, test.a);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }
}
