package org.apache.jackrabbit.oak.segment;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

public class SegmentNodeStateTest {

    public static void main(String[] args) throws Exception {
        System.out.println(VM.current().details());
        System.out.println(ClassLayout.parseClass(SegmentNodeState.class).toPrintable());

    }

    public static class A {
        boolean f;
    }
}
