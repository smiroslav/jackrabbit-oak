/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.remote.queue;

import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class SegmentWriteQueueTest {

    private static final byte[] EMPTY_DATA = new byte[0];

    private SegmentWriteQueue queue;

    private SegmentWriteQueue queueBlocked;

    @After
    public void shutdown() throws IOException {
        if (queue != null) {
            queue.close();
        }

        if (queueBlocked != null) {
            queueBlocked.close();
        }
    }

    @Test
    @Ignore("OAK-9086")
    public void testThreadInterruptedWhileAddigToQueue() throws InterruptedException, NoSuchFieldException {

        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);


        BlockingDeque<SegmentWriteAction> queue = Mockito.mock(BlockingDeque.class);

        queueBlocked = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        FieldSetter.setField(queueBlocked, queueBlocked.getClass().getDeclaredField("queue"), queue);
        Mockito.when(queue.offer(any(SegmentWriteAction.class), anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());

        try {
            queueBlocked.addToQueue(tarEntry(0), EMPTY_DATA, 0, 0);
            fail("IOException should have been thrown");
        } catch (IOException e) {
            assertEquals(e.getCause().getClass(), InterruptedException.class);
        }

        semaphore.release(Integer.MAX_VALUE);

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        Thread flusher = new Thread(() -> {
            try {
                queueBlocked.flush();
                flushFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        flusher.start();

        Thread.sleep(1000);

        assertEquals("Flush thread should have been completed till now", Thread.State.TERMINATED, flusher.getState());
        assertTrue("Segment queue is empty", flushFinished.get());
    }

    @Test
    public void testQueue() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 10; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        for (int i = 0; i < 10; i++) {
            assertNotNull("Segments should be available for read", queue.read(uuid(i)));
        }
        assertFalse("Queue shouldn't be empty", queue.isEmpty());

        semaphore.release(Integer.MAX_VALUE);
        while (!queue.isEmpty()) {
            Thread.sleep(10);
        }

        assertEquals("There should be 10 segments consumed",10, added.size());
        for (int i = 0; i < 10; i++) {
            assertTrue("Missing consumed segment", added.contains(uuid(i)));
        }
    }

    @Test(timeout = 1000)
    public void testFlush() throws IOException, InterruptedException {
        Set<UUID> added = Collections.synchronizedSet(new HashSet<>());
        Semaphore semaphore = new Semaphore(0);
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
            }
            added.add(new UUID(tarEntry.getMsb(), tarEntry.getLsb()));
        });

        for (int i = 0; i < 3; i++) {
            queue.addToQueue(tarEntry(i), EMPTY_DATA, 0, 0);
        }

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        Set<UUID> addedAfterFlush = new HashSet<>();
        new Thread(() -> {
            try {
                queue.flush();
                flushFinished.set(true);
                addedAfterFlush.addAll(added);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();

        Thread.sleep(100);
        assertFalse("Flush should be blocked", flushFinished.get());

        AtomicBoolean addFinished = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
                addFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();

        Thread.sleep(100);
        assertFalse("Adding segments should be blocked until the flush is finished", addFinished.get());

        semaphore.release(Integer.MAX_VALUE);

        while (!addFinished.get()) {
            Thread.sleep(10);
        }
        assertTrue("Flush should be finished once the ", flushFinished.get());
        assertTrue("Adding segments should be blocked until the flush is finished", addFinished.get());

        for (int i = 0; i < 3; i++) {
            assertTrue(addedAfterFlush.contains(uuid(i)));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testClose() throws IOException, InterruptedException {
        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {});
        queue.close();
        queue.addToQueue(tarEntry(10), EMPTY_DATA, 0, 0);
    }

    @Test
    public void testRuntimeExceptionInSegmentConsumer() throws InterruptedException, NoSuchFieldException, IOException {

        queue = new SegmentWriteQueue((tarEntry, data, offset, size) -> {

            //simulate runtime exception that can happen while writing to the remote repository
            throw new RuntimeException();
        });

        queue.addToQueue(tarEntry(0), EMPTY_DATA, 0, 0);

        AtomicBoolean flushFinished = new AtomicBoolean(false);
        Thread flusher = new Thread(() -> {
            try {
                queue.flush();
                flushFinished.set(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        flusher.start();

        Thread.sleep(1000);

        assertFalse("Segment queue should not be empty", flushFinished.get());

        //Provide new instance for segment consumer that does not throw runtime exception while writing to remote repository
        FieldSetter.setField(
                queue,
                queue.getClass().getDeclaredField("writer"),
                (SegmentWriteQueue.SegmentConsumer) (indexEntry, data, offset, size) -> {/*empty consumer*/});

        Thread.sleep(1000);

        assertTrue("Segment queue should be empty", flushFinished.get());
    }

    private static RemoteSegmentArchiveEntry tarEntry(long i) {
        return new RemoteSegmentArchiveEntry(0, i, 0, 0, 0, 0, false);
    }

    private static UUID uuid(long i) {
        return new UUID(0, i);
    }

}
