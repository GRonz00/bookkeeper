package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.eclipse.jetty.util.StringUtil.getBytes;

@RunWith(Enclosed.class)
public class DigestManagerTest {


    @RunWith(Parameterized.class)
    public static class TestInstantiate {
        private final Class<?> expected;
        private final long ledgerId;
        private final byte[] passwrd;
        private final DigestType digestType;
        private final ByteBufAllocator allocator;
        private final boolean useV2Protocol;
        private final Class<? extends Exception> expectedException;
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][]{
                    //{null, -1, getBytes("password"), DigestType.CRC32, PooledByteBufAllocator.DEFAULT, false,Exception.class}, //non viene effettuato nessun controlllo se è un ledger valido
                    {CRC32CDigestManager.class, 1, getBytes(""), DigestType.CRC32C, PooledByteBufAllocator.DEFAULT, true, null},
                    {MacDigestManager.class, 1, getBytes("password"), DigestType.HMAC, PooledByteBufAllocator.DEFAULT, false, null},
                    {null, 1, null, DigestType.HMAC, PooledByteBufAllocator.DEFAULT, false, Exception.class},
                    {CRC32DigestManager.class, 1, getBytes("password"), DigestType.CRC32, PooledByteBufAllocator.DEFAULT, true, null},
                    //{null, 1, getBytes("password"), DigestType.HMAC, null, true, Exception.class}, //non viene effetuato nessun controllo sul bufferAlloccator
                    {DummyDigestManager.class, 1, getBytes("password"), DigestType.DUMMY, PooledByteBufAllocator.DEFAULT, true, null},
            });
        }

        public TestInstantiate(Class<?> expected, long ledgerId, byte[] passwrd, DigestType digestType, ByteBufAllocator allocator, boolean useV2Protocol, Class<? extends Exception> expectedException) {

            this.expected = expected;
            this.ledgerId = ledgerId;
            this.passwrd = passwrd;
            this.digestType = digestType;
            this.allocator = allocator;
            this.useV2Protocol = useV2Protocol;
            this.expectedException = expectedException;
        }
        @Test
        public void initiateTest() {
            try {
                DigestManager result = DigestManager.instantiate(ledgerId, passwrd, digestType, allocator, useV2Protocol);
                if (expectedException == null) {
                    Assert.assertTrue(expected.isInstance(result));
                } else {
                    Assert.fail("L'eccezione attesa non è stata lanciata.");
                }
            } catch (Exception e) {
                if (expectedException == null) {
                    Assert.fail("Il test ha lanciato un'eccezione: " + e.getMessage());
                } else if (!expectedException.isInstance(e)) {
                    Assert.fail("L'eccezione lanciata non era dell'aspettato tipo: " + expectedException.getName());
                }
            }
        }

    }






    }

