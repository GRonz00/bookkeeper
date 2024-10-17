package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class WriteCacheTest {
/*
    @RunWith(Parameterized.class)
    public static class TestGet{
        private final long ledgerId;
        private final long entryId;
        private final ByteBuf expected;
        private final boolean expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {-1L,-1L,null,true},
                    {0L,0L,null,false},//cerco di prendere da uno non scritto forse separa 0 da non valido
                    {1L,1L,Unpooled.wrappedBuffer("value".getBytes()),false}
            });
        }
        public TestGet(long ledgerId, long entryId, ByteBuf expected, boolean expectedException){
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.expected = expected;
            this.expectedException = expectedException;
        }
        @Test
        public void testGet() {

            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 9,16);

                if(expected!=null){
                    writeCache.put(ledgerId, entryId, Unpooled.wrappedBuffer("value".getBytes()));
                }
                // Recupera il valore con la chiave specifica
                ByteBuf result = writeCache.get(ledgerId, entryId);

                // Verifica che il valore sia corretto
                //assertNotNull(result);
                Assert.assertEquals(expected,result);

            }
            catch (Exception e){
                if (!expectedException){
                    Assert.fail();
                }
            }
    }
    }
    @RunWith(Parameterized.class)
    public static class TestPut{
        private final long ledgerId;
        private final long entryId;
        private final ByteBuf entry;
        private final boolean expected;
        private final boolean expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            String nonValid = "nonValid";
            for(int i = 0; i<4;i++ ){
                nonValid = nonValid.concat("n");
            }
            return Arrays.asList(new Object[][]{

                    {-1L,-1L,null,false,true},
                    {0L,0L,Unpooled.wrappedBuffer("valuevaluevalue".getBytes()),false,false},//superata lasize


                    {1L,1L,Unpooled.wrappedBuffer("value".getBytes()),true,false},


                    {1L,1L,Unpooled.wrappedBuffer("".getBytes()),true,false},//stringa vuota
                    //aggiunto dopo jacoco
                    {1L,1L,Unpooled.wrappedBuffer("valuevalue".getBytes()),false,false},//segment-offset<size


                    {2L,1L,Unpooled.wrappedBuffer("value".getBytes()),true,false},//provo a scrivere un entry prima dell ultimw
                    //{3L,9L,Unpooled.wrappedBuffer("value".getBytes()),true,false}//provo a scrivere un entry con id che già c'e
            });
        }
        public TestPut(long ledgerId, long entryId, ByteBuf entry, boolean expected, boolean expectedException){
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.entry = entry;
            this.expected = expected;
            this.expectedException = expectedException;
        }
        @Test
        public void testPut() {

            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 15,8);
                if(ledgerId == 2L){
                    writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 1024,8);
                    writeCache.put(ledgerId, 2L, entry);
                }
                if(ledgerId == 3L){
                    writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 1024,8);
                    //writeCache.put(ledgerId, 8L, entry);
                    writeCache.put(ledgerId, 1L, entry);
                    writeCache.put(ledgerId,2L,Unpooled.wrappedBuffer("value3".getBytes()));

                }
                Assert.assertEquals(expected,writeCache.put(ledgerId, entryId, entry));
                if(expected){
                    ByteBuf result = writeCache.get(ledgerId, entryId);
                    Assert.assertEquals(entry,result);

                }


            } catch (Exception e) {
                if (!expectedException){
                    Assert.fail();
                }

            }
        }


    }

    @RunWith(Parameterized.class)
    public static class TestGetLastEntry{
            private final long ledgerId;
            private final ByteBuf expected;
            private final boolean expectedException;

            @Parameterized.Parameters
            public static Collection<Object[]> getParameters() {
                return Arrays.asList(new Object[][]{
                        {-1L,null,true},
                        {0L,null, false},//non ci è stato scritto
                        {1L,Unpooled.wrappedBuffer("value".getBytes()), false},
                });
            }
            public TestGetLastEntry(long ledgerId, ByteBuf expected, boolean expectedException){
                this.ledgerId = ledgerId;
                this.expected = expected;
                this.expectedException = expectedException;
            }
            @Test
            public void testGetLastEntry() {

                try {
                    WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 9,16);
                    writeCache.put(1L, 1L, Unpooled.wrappedBuffer("value".getBytes()));
                    ByteBuf result = writeCache.getLastEntry(ledgerId);
                    Assert.assertEquals(expected,result);


                } catch (Exception e) {
                    if (!expectedException){
                        Assert.fail();
                    }
                    //System.out.print("eccezzione lanciata");

                }
            }

    }
    @RunWith(Parameterized.class)
    public static class TestDeleteLedger {
        private final long ledgerId;
        private final boolean expected;
        private final boolean expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {-1L, false, true},
                    {0L, true, false},
                    {1L, true, false},
            });
        }

        public TestDeleteLedger(long ledgerId, boolean expected, boolean expectedException) {
            this.ledgerId = ledgerId;
            this.expected = expected;
            this.expectedException = expectedException;
        }

        @Test
        public void testGetLastEntry() {

            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 9, 16);
                writeCache.put(ledgerId, 1L, Unpooled.wrappedBuffer("value".getBytes()));
                writeCache.deleteLedger(ledgerId);
            } catch (Exception e) {
                if (!expectedException) {
                    Assert.fail();
                }
                System.out.print("eccezzione lanciata");

            }
        }
    }
    @RunWith(Parameterized.class)
    public static class TestHasEntry{
        private final long ledgerId;
        private final long entryId;
        private final boolean expected;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {-1L,-1L,false},
                    {0L,0L,false},// non c'era entry
                    {1L,1L,true},//c'era lentry

            });
        }
        public TestHasEntry(long ledgerId, long entryId, boolean expected){
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.expected = expected;
        }
        @Test
        public void testHasEntry() {

            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 9,16);


                writeCache.put(1L, 1L, Unpooled.wrappedBuffer("value".getBytes()));

                boolean result = writeCache.hasEntry(ledgerId, entryId);
                Assert.assertEquals(expected,result);

            }
            catch (Exception e){
            }
        }
    }

 */
}
