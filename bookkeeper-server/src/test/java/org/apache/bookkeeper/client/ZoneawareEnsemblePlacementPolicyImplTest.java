package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetAddress;
import java.util.*;

import static org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

@RunWith(Enclosed.class)
public class ZoneawareEnsemblePlacementPolicyImplTest {

    @RunWith(Parameterized.class)
    public static class NewEnsembleTest {
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            Set<BookieId> emptyList = Collections.emptySet();
            BookieSocketAddress addr1,addr2,addr3;
            addr1 = new BookieSocketAddress("127.0.0.2", 3181);
            addr2 = new BookieSocketAddress("127.0.0.3", 3181);
            addr3 = new BookieSocketAddress("127.0.0.4", 3181);
            Set<BookieId> valid1 = new HashSet<BookieId>();
            valid1.add(addr1.toBookieId());
            valid1.add(addr2.toBookieId());
            valid1.add(addr3.toBookieId());
            List<String> initial1 = Collections.singletonList("validId");
            // expected, exception, ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies, weighted, initialBookies
            return Arrays.asList(
                    new Object[][]{

                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 0, null, emptyList, false, initial1},


                    }
            );
        }

        private ZoneawareEnsemblePlacementPolicy policy;
        private final int ensembleSize, writeQuorumSize, ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;
        private final  EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> expected;
        private final boolean expectedException;
        private final boolean weighted;
        private final List<String> initial;



        public NewEnsembleTest(EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> expected, boolean expectedException,
                               int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                               Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies,
                               boolean weighted, List<String> initial) {
            this.expected = expected;
            this.expectedException = expectedException;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.customMetadata = customMetadata;
            this.excludeBookies = excludeBookies;
            this.weighted = weighted;
            this.initial = initial;
        }


        static void updateMyUpgradeDomain(String zoneAndUD) throws Exception {
            StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), zoneAndUD);
            StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), zoneAndUD);
            StaticDNSResolver.addNodeToRack("127.0.0.1", zoneAndUD);
            StaticDNSResolver.addNodeToRack("localhost", zoneAndUD);
        }



        @Test
        public void newEnsemble() {
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
            clientConfiguration.setZkRetryBackoffMaxRetries(0);
            clientConfiguration.setEnsemblePlacementPolicy(ZoneawareEnsemblePlacementPolicyImpl.class);
            clientConfiguration.setDiskWeightBasedPlacementEnabled(this.weighted);
            //clientConfiguration.setEnforceStrictZoneawarePlacement(false);
            StaticDNSResolver.reset();
            BookieSocketAddress addr1;
            BookieSocketAddress addr2, addr3, addr4, addr5, addr6, addr7, addr8, addr9;
            try{
                StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(),
                        NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
            clientConfiguration.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
            addr1 = new BookieSocketAddress("127.0.0.2", 3181);
            addr2 = new BookieSocketAddress("127.0.0.3", 3181);
            addr3 = new BookieSocketAddress("127.0.0.4", 3181);
            addr4 = new BookieSocketAddress("127.0.0.5", 3181);
            addr5 = new BookieSocketAddress("127.0.0.6", 3181);
            addr6 = new BookieSocketAddress("127.0.0.7", 3181);
            addr7 = new BookieSocketAddress("127.0.0.8", 3181);
            addr8 = new BookieSocketAddress("127.0.0.9", 3181);
            addr9 = new BookieSocketAddress("127.0.0.10", 3181);
            // update dns mapping
            StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1" + "/ud1");
            StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
            StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
            StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1" + "/ud1");
            StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2" + "/ud1");
            StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud1");
            StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud1");
            StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2" + "/ud1");
            StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3" + "/ud1");
                Set<BookieId> validBookies = new HashSet<>();
                validBookies.add(addr1.toBookieId());
                validBookies.add(addr2.toBookieId());
                validBookies.add(addr3.toBookieId());
                /*
                validBookies.add(addr4.toBookieId());
                validBookies.add(addr5.toBookieId());
                validBookies.add(addr6.toBookieId());
                validBookies.add(addr7.toBookieId());
                validBookies.add(addr8.toBookieId());
                validBookies.add(addr9.toBookieId());

                 */


                ZoneawareEnsemblePlacementPolicyImpl z =  new ZoneawareEnsemblePlacementPolicyImpl();
                updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                z.initialize(clientConfiguration, Optional.<DNSToSwitchMapping> empty(), null, DISABLE_ALL,
                        NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

                z.onClusterChanged(validBookies, new HashSet<>());
                EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> result = z.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
                Set<BookieId> newEnsembleSet = new HashSet<BookieId>(result.getResult());
                Assert.assertTrue( newEnsembleSet.containsAll(expected.getResult()));
                //Assert.assertTrue(true);
                Assert.assertEquals(expected.getAdheringToPolicy(), result.getAdheringToPolicy());
            }catch (Exception e){
                if (!expectedException){
                    Assert.fail();
                }

            }


        }
    }



//public EnsemblePlacementPolicy initialize​(ClientConfiguration conf, java.util.Optional<DNSToSwitchMapping> optionalDnsResolver, io.netty.util.HashedWheelTimer timer, FeatureProvider featureProvider, StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver)
    /*
    @RunWith(Parameterized.class)
    public static class TestInitialize{
        private final ClientConfiguration conf;
        private final java.util.Optional<DNSToSwitchMapping> optionalDnsResolver;
        private final io.netty.util.HashedWheelTimer timer;
        private final FeatureProvider featureProvider;
        private final StatsLogger statsLogger;
        private final BookieAddressResolver bookieAddressResolver;

    }
    */





}
