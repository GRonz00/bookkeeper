package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.Getter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Enclosed.class)
public class WriteCacheTest {

    @RunWith(Parameterized.class)
    public static class TestPut{
        private final long ledgerId;
        private final long entryId;
        private final ByteBuf entry;
        private final boolean expected;
        private final boolean expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {-1L,-1L,null,false,true},
                    {0L,0L,Unpooled.wrappedBuffer("valuevaluevalue".getBytes()),false,false},
                    {1L,1L,Unpooled.wrappedBuffer("value".getBytes()),true,false},
                    {1L,1L,Unpooled.wrappedBuffer("".getBytes()),true,false},

                    //Pit
                    {1L,1L,Unpooled.wrappedBuffer("valuevalue".getBytes()),false,false},//segment-offset<size
                    {2L,1L,Unpooled.wrappedBuffer("value".getBytes()),true,false},//provo a scrivere un entry prima dell ultima
                    {3L,9L,Unpooled.wrappedBuffer("value".getBytes()),true,false},//provo a scrivere un entry con id che già c'e
                    {4L,1L,Unpooled.wrappedBuffer("value".getBytes()),true,false},//provo a scrivere un entry con id che già c'e


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
                    writeCache.put(ledgerId, 8L, entry);
                    writeCache.put(ledgerId, 1L, entry);
                    writeCache.put(ledgerId,2L,Unpooled.wrappedBuffer("value3".getBytes()));

                }
                if(ledgerId == 4L){
                    writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 5,8);
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



    @Getter
    public static class Pair{
        private final long ledgerId;
        private final long entryId;

        public Pair(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return ledgerId == pair.ledgerId && entryId == pair.entryId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ledgerId, entryId);
        }
    }
    @RunWith(Parameterized.class)
    public static class TestForEach{
        private final Map<Pair,ByteBuf> cacheValues;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Map<Pair, ByteBuf> map1 = new HashMap<>();
            Map<Pair, ByteBuf> map2 = new HashMap<>();
            map2.put(new Pair(1L, 1L), Unpooled.copiedBuffer("value1".getBytes()));
            Map<Pair, ByteBuf> map3 = new HashMap<>();
            map3.put(new Pair(1L, 1L), Unpooled.copiedBuffer("value1".getBytes()));
            map3.put(new Pair(2L, 2L), Unpooled.copiedBuffer("value2".getBytes()));



            return Arrays.asList(new Object[][]{
                    {map1},
                    {map2},
                    {map3}
            });
        }
        public TestForEach( Map<Pair,ByteBuf>  map){
            this.cacheValues = map;
        }
        @Test
        public void testForEach() {
            AtomicInteger counter = new AtomicInteger();
            AtomicInteger counter2 = new AtomicInteger();

            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 2048,128);
                for(Map.Entry<Pair,ByteBuf> entry: cacheValues.entrySet()){
                    writeCache.put(entry.getKey().ledgerId,entry.getKey().entryId, entry.getValue());
                }
                writeCache.forEach((ledgerId, entryId, entry) -> {
                    counter.getAndIncrement();
                    Assert.assertTrue(cacheValues.containsKey(new Pair(ledgerId,entryId)));//controllo che foreach prende un valore precedentemente aggiunto nella cache
                    Assert.assertTrue(ByteBufUtil.equals(cacheValues.get(new Pair(ledgerId, entryId)), entry));//controllo che sia presa l'entry effettiva inserita a ledgerId, entryId

                });
                Assert.assertEquals(cacheValues.size(),counter.get());//controllo che non ci siano più o meno entry

                //jacoco
                writeCache.deleteLedger(1L);
                //pit
                writeCache.put(6L,6L,Unpooled.copiedBuffer("v6".getBytes()));
                writeCache.put(7L,7L,Unpooled.copiedBuffer("v7".getBytes()));
                writeCache.put(8L,8L,Unpooled.copiedBuffer("v8".getBytes()));
                writeCache.forEach((ledgerId, entryId, entry) -> {
                    counter2.getAndIncrement();
                    //pit
                    if(counter2.get()==3 && cacheValues.size()>3)Assert.assertTrue(ByteBufUtil.equals(cacheValues.get(new Pair(ledgerId, entryId)), Unpooled.copiedBuffer("v4".getBytes())));//controllo che siano lette in ordine di ledgerId e entryId
                });


            } catch (Exception e) {
                Assert.fail();

            }
        }
    }

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
                    {0L,0L,null,false},//cerco di prendere da uno non scritto
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
                    writeCache.put(1, 1, Unpooled.wrappedBuffer("value".getBytes()));
                }
                // Recupera il valore con la chiave specifica
                ByteBuf result = writeCache.get(ledgerId, entryId);

                // Verifica che il valore sia corretto
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
        public void testDeleteLedger() {
            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 9, 16);
                writeCache.put(ledgerId, 1L, Unpooled.wrappedBuffer("value".getBytes()));
                writeCache.deleteLedger(ledgerId);
            } catch (Exception e) {
                if (!expectedException) {
                    Assert.fail();
                }
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
                System.out.print("eccezzione lanciata");
            }
        }
    }


}
