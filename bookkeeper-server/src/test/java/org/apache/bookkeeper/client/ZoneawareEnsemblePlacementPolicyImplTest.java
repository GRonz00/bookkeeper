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
import org.checkerframework.checker.units.qual.A;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
            BookieSocketAddress addr4, addr5, addr6;
            addr4 = new BookieSocketAddress("127.0.0.5", 3181);
            addr5 = new BookieSocketAddress("127.0.0.6", 3181);
            addr6 = new BookieSocketAddress("127.0.0.7", 3181);
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
            Set<BookieId> initial3 = new HashSet<>(Arrays.asList(
                    addr1.toBookieId(),
                    addr2.toBookieId(),
                    addr6.toBookieId()
            ));
            Set<BookieId> initial4 = new HashSet<>(Collections.singletonList(
                    addr1.toBookieId()
            ));

            // expected, exception, ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies, weighted, initialBookies
            return Arrays.asList(
                    new Object[][]{



                            //{PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 0, null, emptyList, true, initial1 enforceStrictZoneawarePlacement},
                            {null,true, 0, 1, 0, null, null, true, initial1, true,2},
                            {null,true, 0, 1, 1, emptyMap, emptyList, true, initial1, true,2},
                            {null,true, 0, 1, 2, validMap, validList, true, initial1, true,2},
                            {null,true, 1, 1, 0, null, invalidList, true, initial1, true,2},
                            {null,true, 1, 1, 1, emptyMap, mixList, true, initial1, true,2},
                            {null,true, 1, 1, 2, validMap, null, true, initial1, true,2},
                            {null,true, 2, 1, 0, null, emptyList, true, initial1, true,2},
                            {null,true, 2, 1, 1, emptyMap, validList, true, initial1, true,2},
                            {null,true, 2, 1, 2, validMap, invalidList, true, initial1, true,2},
                            {null,true, 1, 2, 1, null, mixList, true, initial1, true,2},
                            {null,true, 1, 2, 2, emptyMap, null, true, initial1, true,2},
                            {null,true, 1, 2, 3, validMap, emptyList, true, initial1, true,2},
                            {null,true, 2, 2, 1, null, validList, true, initial1, true,2},
                            {null,true, 2, 2, 2, emptyMap, invalidList, true, initial1, true,2},
                            {null,true, 2, 2, 3, validMap, mixList, true, initial1, true,2},
                            {null,true, 3, 2, 1, null, null, true, initial1, true,2},
                            {null,true, 3, 2, 2, emptyMap, emptyList, true, initial1, true,2},
                            {null,true, 3, 2, 3, validMap, validList, true, initial1, true,2},
                            {null,true, 2, 3, 2, null, invalidList, true, initial1, true,2},
                            {null,true, 2, 3, 3, emptyMap, mixList, true, initial1, true,2},
                            {null,true, 2, 3, 4, validMap, null, true, initial1, true,2},
                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 2, null, emptyList, true, initial1, true,2},
                            {null,true, 3, 3, 3, emptyMap, validList, true, initial1, true,2},
                            //{null,true, 3, 3, 4, validMap, invalidList, true, initial1, true,2}, non viene effettuato il controllo sul ack quorum
                            {null,true, 4, 3, 2, null, mixList, true, initial1, true,2},
                            {null,true, 4, 3, 3, emptyMap, null, true, initial1, true,2},
                            {null,true, 4, 3, 4, validMap, emptyList, true, initial1, true,2},

                            //Jacoco improvement
                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 3, emptyMap, emptyList, false, initial1, true,2},//non pesato
                            {PlacementResult.of(valid1, PlacementPolicyAdherence.MEETS_STRICT),false, 3, 3, 3, emptyMap, emptyList, true, initial1, false,2},//random con abbastanza booki
                            {null,true, 3, 3, 3, emptyMap, validList, true, initial1, false,2},//random senza abbastanza bookie
                            {PlacementResult.of(valid2, PlacementPolicyAdherence.MEETS_SOFT),false, 3, 3, 2, null, validList, true, initial2, true,2}, //cover add default domain

                            {PlacementResult.of(new HashSet<>(Collections.singletonList(
                                    addr1.toBookieId()
                            )), PlacementPolicyAdherence.MEETS_STRICT),false, 1, 1,0, null, emptyList, true, initial4, true,0},// miglioria badua

                            //pit improvement
                            {null,true, 3,3, 0, emptyMap, emptyList, true, initial3, true,2},//pit kill secondo if enforceStrictZoneawarePlacement
                            {null,true, 3,2, 0, emptyMap, emptyList, true, initial1, true,2},//pit kill primo if enforceStrictZoneawarePlacement



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
        private final int minZone;
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
            clientConfiguration.setMinNumZonesPerWriteQuorum(this.minZone); //miglioria badua
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
                StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
                StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
                StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");

                addr4 = new BookieSocketAddress("127.0.0.5", 3181);
                StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone2" + "/ud2");

                addr5 = new BookieSocketAddress("127.0.0.6", 3181);
                StaticDNSResolver.addNodeToRack(addr5.getHostName(),"/defZone/defDom");
                addr6 = new BookieSocketAddress("127.0.0.7", 3181);
                StaticDNSResolver.addNodeToRack(addr6.getHostName(),"/zone1/ud1");
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
                               boolean weighted, Set<BookieId> initial, boolean enforceStrictZoneawarePlacement, int minZone) {
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
            this.minZone = minZone;
        }
        @Test
        public void newEnsemble() {
            try{

                EnsemblePlacementPolicy.PlacementResult<java.util.List<BookieId>> result = zep.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
                Set<BookieId> newEnsembleSet = new HashSet<BookieId>(result.getResult());
                if(expectedException){
                    Assert.fail();
                }
                Assert.assertTrue( newEnsembleSet.containsAll(expected.getResult()));
                Assert.assertEquals(expected.getAdheringToPolicy(), result.getAdheringToPolicy());
                //Pit
                Field rwLockField = TopologyAwareEnsemblePlacementPolicy.class.getDeclaredField("rwLock");
                rwLockField.setAccessible(true);
                Assert.assertEquals(0,zep.rwLock.getReadLockCount());

            }catch (Exception e){
                if(!expectedException){
                    Assert.fail();
                }
            }


        }
    }




@RunWith(Parameterized.class)
public static class IsEnsembleAdheringToPlacementPolicyTest{
    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        BookieSocketAddress addr1, addr2, addr3, addr4, addr5, addr6, addr7, addr8, addr9, addr10,addr11,addr12,addr13,addr14,addr15,addr16,addr17,addr18;
        addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        addr12 = new BookieSocketAddress("127.0.0.13", 3181);
        addr13 = new BookieSocketAddress("127.0.0.14", 3181);
        addr14 = new BookieSocketAddress("127.0.0.15", 3181);
        addr15 = new BookieSocketAddress("127.0.0.16", 3181);
        addr16 = new BookieSocketAddress("127.0.0.17", 3181);
        addr17 = new BookieSocketAddress("127.0.0.18", 3181);
        addr18 = new BookieSocketAddress("127.0.0.19", 3181);
        List<BookieId> strict_min = Arrays.asList(addr1.toBookieId(), addr2.toBookieId());
        List<BookieId> strict_eq = Arrays.asList(addr1.toBookieId(), addr2.toBookieId(),addr3.toBookieId());
        List<BookieId> strict_mag = Arrays.asList(addr1.toBookieId(), addr2.toBookieId(),addr3.toBookieId(),addr4.toBookieId());
        List<BookieId> strict_mod = Arrays.asList(addr1.toBookieId(), addr2.toBookieId(),addr3.toBookieId(),addr4.toBookieId(),addr5.toBookieId(),addr6.toBookieId());
        List<BookieId> sameZon_min = Arrays.asList(addr1.toBookieId(), addr7.toBookieId());
        List<BookieId> sameZon_eq = Arrays.asList(addr1.toBookieId(), addr7.toBookieId(),addr8.toBookieId());
        List<BookieId> sameZon_mag = Arrays.asList(addr1.toBookieId(), addr7.toBookieId(),addr8.toBookieId(),addr9.toBookieId());
        List<BookieId> sameZon_mod = Arrays.asList(addr1.toBookieId(), addr7.toBookieId(),addr8.toBookieId(),addr9.toBookieId(),addr10.toBookieId(),addr11.toBookieId(),addr12.toBookieId());
        List<BookieId> diffUd_min = Arrays.asList(addr1.toBookieId(), addr13.toBookieId());
        List<BookieId> diffUD_eq = Arrays.asList(addr1.toBookieId(), addr13.toBookieId(),addr14.toBookieId());
        List<BookieId> diffUD_mag = Arrays.asList(addr1.toBookieId(), addr13.toBookieId(),addr14.toBookieId(),addr15.toBookieId());
        List<BookieId> diffUD_mod = Arrays.asList(addr1.toBookieId(), addr13.toBookieId(),addr14.toBookieId(),addr15.toBookieId(),addr16.toBookieId(),addr17.toBookieId(),addr18.toBookieId());

        List<BookieId> undLoc = Arrays.asList(addr1.toBookieId(), addr2.toBookieId(),addr18.toBookieId());
        List<BookieId> diffUd_min_minUD = Arrays.asList(addr1.toBookieId(),addr7.toBookieId(), addr15.toBookieId());//num zone maggiore del minimo ma in una zona ci sono due con stesso ud
        List<BookieId> diffUd_min_minUD_des = Arrays.asList(addr1.toBookieId(), addr7.toBookieId(),addr3.toBookieId(),addr4.toBookieId(),addr5.toBookieId(),addr6.toBookieId());//num zone maggiore delle desiderate ma in una zona ci sono due con stesso ud
        List<BookieId> n_zone_mim_minzone =  Arrays.asList(addr1.toBookieId(),addr11.toBookieId(), addr12.toBookieId());


        return Arrays.asList(
                new Object[][]{
                        {strict_min,3,2,PlacementPolicyAdherence.FAIL,false,3},
                        {strict_eq,3,3,PlacementPolicyAdherence.MEETS_STRICT,false,3},
                        {strict_mag,3,4,PlacementPolicyAdherence.FAIL,false,3},
                        {strict_mod,3,2,PlacementPolicyAdherence.MEETS_STRICT,false,3},
                        {sameZon_min,3,3,PlacementPolicyAdherence.FAIL,false,3},
                        //{sameZon_eq,3,4,PlacementPolicyAdherence.FAIL,false,3},
                        {sameZon_mag,3,2,PlacementPolicyAdherence.FAIL,false,3},
                        {sameZon_mod,3,3,PlacementPolicyAdherence.FAIL,false,3},
                        {diffUd_min,3,4,PlacementPolicyAdherence.FAIL,false,3},
                        {diffUD_eq,3,2,PlacementPolicyAdherence.MEETS_SOFT,false,3},
                        {diffUD_mag,3,3,PlacementPolicyAdherence.FAIL,false,3},
                        //{diffUD_mod,3,4,PlacementPolicyAdherence.FAIL,false, 3}
                        {strict_min,2,2,PlacementPolicyAdherence.FAIL,false,3},
                        {sameZon_min,2,3,PlacementPolicyAdherence.FAIL,false,3},
                        {diffUd_min,2,4,PlacementPolicyAdherence.FAIL,false,3},
                        {new ArrayList<>(),3,2,PlacementPolicyAdherence.FAIL,false,3},
                        {null,3,3,PlacementPolicyAdherence.FAIL,false,3},

                        //jacoco
                        {undLoc,3,3,PlacementPolicyAdherence.FAIL,false,3},
                        {diffUd_min_minUD,3,3,PlacementPolicyAdherence.FAIL,false,3},
                        {n_zone_mim_minzone,3,3,PlacementPolicyAdherence.FAIL,false,3},
                        {diffUd_min_minUD_des,3,3,PlacementPolicyAdherence.FAIL,false,2},
                        //<du var="placementPolicyAdherence" def="867" use="947" target="893" covered="0"/> //impossibile diverso if appena la policy cambia a if return
                        //<du var="placementPolicyAdherence" def="948" use="947" target="948" covered="0"/> stesso motivo
                        //<du var="i" def="893" use="893" target="953" covered="0"/> la taglia della lista dovrebbe essere 0 ma viene fatto precedentemente il controllo che sia diverso da zero
                        //<du var="j" def="896" use="896" target="922" covered="0"/> bisognerebbe mettere writeQuorumSize=0 ma per farlo bisognerebbe configurare minNumZonesPerWriteQuorum come negativo cosa non permessa
                        //tutte le altre sono debug
                        //pit condizione di boundary cambiata non da errori perchè ad ogni iterazione del for si pulisce tutto e si utilizza il mod ensemble size, quindi viene solo effettuato 2 volte il controllo sul primo elemento
                        //il clear non è necessario perche è una mappa di supporto per l'altra, quando non esiste la riga per l'altra della seconda viene
                        //istanziata a 1, se invece esiste viene aggiornato il valore. Quindi se la prima mappa viene pulità non cambiera il risultato


                });
    }
    private final EnsemblePlacementPolicy.PlacementPolicyAdherence expected;
    private final boolean expectedException;
    private final List<BookieId> ensambleList;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private ZoneawareEnsemblePlacementPolicyImpl zep;
    private final int desiredNumZonesPerWriteQuorum;
    public IsEnsembleAdheringToPlacementPolicyTest(List<BookieId> ensambleList,int writeQuorumSize, int ackQuorumSize, EnsemblePlacementPolicy.PlacementPolicyAdherence expected, boolean expectedException, int desiredNumZonesPerWriteQuorum){
        this.ensambleList = ensambleList;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.expected = expected;
        this.expectedException = expectedException;
        this.desiredNumZonesPerWriteQuorum = desiredNumZonesPerWriteQuorum;
    }
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
        clientConfiguration.setDesiredNumZonesPerWriteQuorum(desiredNumZonesPerWriteQuorum);
        clientConfiguration.setEnsemblePlacementPolicy(ZoneawareEnsemblePlacementPolicyImpl.class);
        clientConfiguration.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        StaticDNSResolver.reset();
        BookieSocketAddress addr1, addr2, addr3, addr4, addr5, addr6, addr7, addr8, addr9, addr10,addr11,addr12,addr13,addr14,addr15,addr16,addr17,addr18;
        try {
            addr1 = new BookieSocketAddress("127.0.0.2", 3181);
            addr2 = new BookieSocketAddress("127.0.0.3", 3181);
            addr3 = new BookieSocketAddress("127.0.0.4", 3181);
            addr4 = new BookieSocketAddress("127.0.0.5", 3181);
            addr5 = new BookieSocketAddress("127.0.0.6", 3181);
            addr6 = new BookieSocketAddress("127.0.0.7", 3181);
            addr7 = new BookieSocketAddress("127.0.0.8", 3181);
            addr8 = new BookieSocketAddress("127.0.0.9", 3181);
            addr9 = new BookieSocketAddress("127.0.0.10", 3181);
            addr10 = new BookieSocketAddress("127.0.0.11", 3181);
            addr11 = new BookieSocketAddress("127.0.0.12", 3181);
            addr12 = new BookieSocketAddress("127.0.0.13", 3181);
            addr13 = new BookieSocketAddress("127.0.0.14", 3181);
            addr14 = new BookieSocketAddress("127.0.0.15", 3181);
            addr15 = new BookieSocketAddress("127.0.0.16", 3181);
            addr16 = new BookieSocketAddress("127.0.0.17", 3181);
            addr17 = new BookieSocketAddress("127.0.0.18", 3181);
            addr18 = new BookieSocketAddress("127.0.0.19", 3181);
            StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
            StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
            StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
            StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");
            StaticDNSResolver.addNodeToRack(addr5.getHostName(),"/zone5/ud1");
            StaticDNSResolver.addNodeToRack(addr6.getHostName(),"/zone6/ud1");
            StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud1");
            StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone1/ud1");
            StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone1/ud1");
            StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone1/ud1");
            StaticDNSResolver.addNodeToRack(addr11.getHostName(),"/zone1/ud4");
            StaticDNSResolver.addNodeToRack(addr12.getHostName(),"/zone1/ud5");
            StaticDNSResolver.addNodeToRack(addr13.getHostName(), "/zone1/ud2");
            StaticDNSResolver.addNodeToRack(addr14.getHostName(),"/zone2/ud3");
            StaticDNSResolver.addNodeToRack(addr15.getHostName(),"/zone3/ud4");
            StaticDNSResolver.addNodeToRack(addr16.getHostName(), "/zone4/ud5");
            StaticDNSResolver.addNodeToRack(addr17.getHostName(),"/zone5/ud6");
            StaticDNSResolver.addNodeToRack(addr18.getHostName(),NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
            zep =  new ZoneawareEnsemblePlacementPolicyImpl();
            updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
            zep.initialize(clientConfiguration, Optional.<DNSToSwitchMapping> empty(), null, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
            Set<BookieId> initial = new HashSet<>(Arrays.asList(
                    addr1.toBookieId(),
                    addr2.toBookieId(),
                    addr3.toBookieId(),
                    addr4.toBookieId(),
                    addr5.toBookieId(),
                    addr6.toBookieId(),
                    addr7.toBookieId(),
                    addr8.toBookieId(),
                    addr9.toBookieId(),
                    addr10.toBookieId(),
                    addr11.toBookieId(),
                    addr12.toBookieId(),
                    addr13.toBookieId(),
                    addr14.toBookieId(),
                    addr15.toBookieId(),
                    addr16.toBookieId(),
                    addr17.toBookieId(),
                    addr18.toBookieId()
            ));
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
    @Test
    public void IsEnsembleAdheringToPlacementPolicy(){
        try {
            EnsemblePlacementPolicy.PlacementPolicyAdherence result = zep.isEnsembleAdheringToPlacementPolicy(ensambleList,writeQuorumSize,ackQuorumSize);
            if(expectedException){
                Assert.fail();
            }
            Assert.assertEquals(expected,result);
            //Pit
            Field rwLockField = TopologyAwareEnsemblePlacementPolicy.class.getDeclaredField("rwLock");
            rwLockField.setAccessible(true);
            Assert.assertEquals(0,zep.rwLock.getReadLockCount());
        }catch (Exception e){
            if(!expectedException){
                Assert.fail();
            }
        }

    }
}




}
