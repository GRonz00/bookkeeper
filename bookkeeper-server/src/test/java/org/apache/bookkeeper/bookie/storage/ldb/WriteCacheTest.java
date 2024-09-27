package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.experimental.runners.Enclosed;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.internal.matchers.text.ValuePrinter.print;

@RunWith(Enclosed.class)
public class WriteCacheTest {

    @RunWith(Parameterized.class)
    public static class TestGet{
        private final long ledgerId;
        private final long entryId;
        private final ByteBuf expected;
        private final Class<? extends Exception> expectedException;

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    {1L,1L,Unpooled.wrappedBuffer("testValue".getBytes()),null},
                    {-1L,-1L,null,Exception.class}
            });
        }
        public TestGet(long ledgerId, long entryId, ByteBuf expected, Class<? extends Exception> expectedException){
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.expected = expected;
            this.expectedException = expectedException;
        }
        @Test
        public void testGet() {

            try {
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 100 * 1024 * 1024); // 100MB
                ByteBuf value = Unpooled.wrappedBuffer("testValue".getBytes());
                if(expected!=null){
                    writeCache.put(ledgerId, entryId, value);
                }
                // Recupera il valore con la chiave specifica
                ByteBuf result = writeCache.get(ledgerId, entryId);

                // Verifica che il valore sia corretto
                //assertNotNull(result);
                if(expectedException != null){
                    Assert.assertEquals(value.compareTo(result), 0);
                }
                else{

                }
            }
            catch (Exception e){
                System.out.print("lanciata eccezione");

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
            for(int i = 0; i<=5;i++ ){
                nonValid = nonValid.concat("nonValid");
            }
            print(nonValid.getBytes().length);
            return Arrays.asList(new Object[][]{
                    {-1L,-1L,null,false,true},
                    {0L,0L,Unpooled.wrappedBuffer(nonValid.getBytes()),false,false},
                    {1L,1L,Unpooled.wrappedBuffer("value".getBytes()),true,false},
                    {1L,1L,Unpooled.wrappedBuffer("".getBytes()),true,false}
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
                WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 9,16);
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
}
