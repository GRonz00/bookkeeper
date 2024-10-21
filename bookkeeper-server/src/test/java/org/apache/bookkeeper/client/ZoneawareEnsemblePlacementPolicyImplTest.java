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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
            BookieSocketAddress addr1,addr2,addr3;
            addr1 = new BookieSocketAddress("127.0.0.2", 3181);
            addr2 = new BookieSocketAddress("127.0.0.3", 3181);
            addr3 = new BookieSocketAddress("127.0.0.4", 3181);
            Map<String, byte[]> emptyMap = new HashMap<>();
            Map<String, byte[]> validMap = new HashMap<>();
            validMap.put("string", new byte[]{1});
            Set<BookieId> emptyList = Collections.emptySet();
            Set<BookieId> validList = Collections.singleton(addr1.toBookieId());
            Set<BookieId> invalidList = Collections.singleton(BookieId.parse("invalidId"));
            Set<BookieId> mixList = new HashSet<>();
            mixList.add(addr1.toBookieId());
            mixList.add(BookieId.parse("invalidId"));
            Set<BookieId> valid1 = new HashSet<BookieId>(Arrays.asList(
                    addr1.toBookieId(),
                    addr2.toBookieId(),
                    addr3.toBookieId()
            ));
            BookieSocketAddress addr4, addr5;
            addr4 = new BookieSocketAddress("127.0.0.5", 3181);
            addr5 = new BookieSocketAddress("127.0.0.6", 3181);
            Set<BookieId> valid2 = new HashSet<BookieId>(Arrays.asList(
                    addr3.toBookieId(),
                    addr2.toBookieId(),
                    addr4.toBookieId()
            ));
            Set<BookieId> initial1 = new HashSet<>(Arrays.asList(
                    addr1.toBookieId(),
                    addr2.toBookieId(),
                    addr3.toBookieId()
            ));
            Set<BookieId> initial2 = new HashSet<>(Arrays.asList(
                    addr1.toBookieId(),
                    addr2.toBookieId(),
                    addr3.toBookieId(),
                    addr4.toBookieId(),
                    addr5.toBookieId()  //cover addDefaultDomain
            ));

            // expected, exception, ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies, weighted, initialBookies
            return Arrays.asList(
                    new Object[][]{

                            //{PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 0, null, emptyList, true, initial1 enforceStrictZoneawarePlacement},
                            {null,true, 1, 0, -1, null, null, true, initial1, true},
                            {null,true, 1, 0, 0, emptyMap, emptyList, true, initial1, true},
                            {null,true, 1, 0, 1, validMap, validList, true, initial1, true},
                            {null,true, 1, 1, 0, null, invalidList, true, initial1, true},
                            {null,true, 1, 1, 1, emptyMap, mixList, true, initial1, true},
                            {null,true, 1, 1, 2, validMap, null, true, initial1, true},
                            {null,true, 1, 2, 1, null, emptyList, true, initial1, true},
                            {null,true, 1, 2, 2, emptyMap, validList, true, initial1, true},
                            {null,true, 1, 2, 3, validMap, invalidList, true, initial1, true},
                            {null,true, 2, 1, 0, null, mixList, true, initial1, true},
                            {null,true, 2, 1, 1, emptyMap, null, true, initial1, true},
                            {null,true, 2, 1, 2, validMap, emptyList, true, initial1, true},
                            {null,true, 2, 2, 1, null, validList, true, initial1, true},
                            {null,true, 2, 2, 2, emptyMap, invalidList, true, initial1, true},
                            {null,true, 2, 2, 3, validMap, mixList, true, initial1, true},
                            {null,true, 2, 3, 2, null, null, true, initial1, true},
                            {null,true, 2, 3, 3, emptyMap, emptyList, true, initial1, true},
                            {null,true, 2, 3, 4, validMap, validList, true, initial1, true},
                            {null,true, 3, 2, 1, null, invalidList, true, initial1, true},
                            {null,true, 3, 2, 2, emptyMap, mixList, true, initial1, true},
                            {null,true, 3, 2, 3, validMap, null, true, initial1, true},
                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 2, null, emptyList, true, initial1, true},
                            {null,true, 3, 3, 3, emptyMap, validList, true, initial1, true},
                            {null,true, 3, 3, 4, validMap, invalidList, true, initial1, true},
                            {null,true, 3, 4, 3, null, mixList, true, initial1, true},
                            {null,true, 3, 4, 4, emptyMap, null, true, initial1, true},
                            {null,true, 3, 4, 5, validMap, emptyList, true, initial1, true},
                            //Jacoco improvement
                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 3, emptyMap, emptyList, false, initial1, true},//non pesato
                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 3, emptyMap, emptyList, true, initial1, false},//random con abbastanza booki
                            {null,true, 3, 3, 3, emptyMap, validList, true, initial1, false},//random senza abbastanza bookie
                            {PlacementResult.of(valid2, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 2, null, validList, true, initial2, true}, //cover add default domain
                    }
            );
        }

        private final int ensembleSize, writeQuorumSize, ackQuorumSize;
        private final Map<String, byte[]> customMetadata;
        private final Set<BookieId> excludeBookies;
        private final  EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> expected;
        private final boolean expectedException;
        private final boolean weighted;
        private final Set<BookieId> initial;
        private final boolean enforceStrictZoneawarePlacement;
        private ZoneawareEnsemblePlacementPolicyImpl zep;

        static void updateMyUpgradeDomain(String zoneAndUD) throws Exception {
            StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), zoneAndUD);
            StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), zoneAndUD);
            StaticDNSResolver.addNodeToRack("127.0.0.1", zoneAndUD);
            StaticDNSResolver.addNodeToRack("localhost", zoneAndUD);
        }

        @Before
        public void setUp() {
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
            clientConfiguration.setZkRetryBackoffMaxRetries(0);
            clientConfiguration.setEnsemblePlacementPolicy(ZoneawareEnsemblePlacementPolicyImpl.class);
            clientConfiguration.setDiskWeightBasedPlacementEnabled(this.weighted);
            clientConfiguration.setEnforceStrictZoneawarePlacement(this.enforceStrictZoneawarePlacement);
            StaticDNSResolver.reset();
            BookieSocketAddress addr1;
            BookieSocketAddress addr2, addr3, addr4, addr5, addr6, addr7, addr8, addr9;
            try {
                StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(),
                        NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                clientConfiguration.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
                addr1 = new BookieSocketAddress("127.0.0.2", 3181);
                addr2 = new BookieSocketAddress("127.0.0.3", 3181);
                addr3 = new BookieSocketAddress("127.0.0.4", 3181);
                StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1" + "/ud1");
                StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
                StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");

                addr4 = new BookieSocketAddress("127.0.0.5", 3181);
                StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone2" + "/ud2");

                addr5 = new BookieSocketAddress("127.0.0.6", 3181);
                StaticDNSResolver.addNodeToRack(addr5.getHostName(),"/defZone/defDom");
                StaticDNSResolver.addNodeToRack("notBookie","/defZone/defDom");
                /*
                addr6 = new BookieSocketAddress("127.0.0.7", 3181);
                addr7 = new BookieSocketAddress("127.0.0.8", 3181);
                addr8 = new BookieSocketAddress("127.0.0.9", 3181);
                addr9 = new BookieSocketAddress("127.0.0.10", 3181);
                StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud1");
                StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud1");
                StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2" + "/ud1");
                StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3" + "/ud1");

                 */
                zep =  new ZoneawareEnsemblePlacementPolicyImpl();
                updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
                zep.initialize(clientConfiguration, Optional.<DNSToSwitchMapping> empty(), null, DISABLE_ALL,
                        NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
                zep.withDefaultFaultDomain("/defZone/defDom");
                zep.onClusterChanged(initial, new HashSet<>());
            } catch (Exception e) {
                if (!expectedException) {
                    Assert.fail();
                }
            }
        }

        @After
        public void tearDown() {
            zep.uninitalize();
        }
        public NewEnsembleTest(EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> expected, boolean expectedException,
                               int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                               Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies,
                               boolean weighted, Set<BookieId> initial, boolean enforceStrictZoneawarePlacement) {
            this.expected = expected;
            this.expectedException = expectedException;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.customMetadata = customMetadata;
            this.excludeBookies = excludeBookies;
            this.weighted = weighted;
            this.initial = initial;
            this.enforceStrictZoneawarePlacement = enforceStrictZoneawarePlacement;
        }






        @Test
        public void newEnsemble() {
            try{

                EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> result = zep.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
                Set<BookieId> newEnsembleSet = new HashSet<BookieId>(result.getResult());
                //Assert.assertTrue( newEnsembleSet.containsAll(expected.getResult()));
                //Assert.assertTrue(true);
                Assert.assertEquals(expected.getAdheringToPolicy(), result.getAdheringToPolicy());
                if(expectedException){
                    Assert.fail();
                }


            }catch (Exception e){
                if(!expectedException){
                    Assert.fail();
                }
            }


        }
    }









}
