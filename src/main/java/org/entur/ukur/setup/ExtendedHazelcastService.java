package org.entur.ukur.setup;

import com.hazelcast.core.*;
import org.entur.ukur.subscription.PushMessage;
import org.entur.ukur.subscription.Subscription;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.rutebanken.hazelcasthelper.service.HazelCastService;
import org.rutebanken.hazelcasthelper.service.KubernetesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Service
public class ExtendedHazelcastService extends HazelCastService {

    public ExtendedHazelcastService(@Autowired KubernetesService kubernetesService, @Autowired UkurConfiguration cfg) {
        super(kubernetesService, cfg.getHazelcastManagementUrl());
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }

//    @Override
//    public List<SerializerConfig> getSerializerConfigs() {
//
//        return Arrays.asList(new SerializerConfig()
//                .setTypeClass(Object.class)
//                .setImplementation(new ByteArraySerializer() {
//                    @Override
//                    public byte[] write(Object object) throws IOException {
//                        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
//                            try(ObjectOutputStream o = new ObjectOutputStream(b)){
//                                o.writeObject(object);
//                            }
//                            return b.toByteArray();
//                        }
//                    }
//
//                    @Override
//                    public Object read(byte[] buffer) throws IOException {
//                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
//                        ObjectInputStream in = new ObjectInputStream(byteArrayInputStream);
//                        try {
//                            return in.readObject();
//                        } catch (ClassNotFoundException e) {
//                            e.printStackTrace();
//                        }
//                        return null;
//                    }
//
//                    @Override
//                    public int getTypeId() {
//                        return 1;
//                    }
//
//                    @Override
//                    public void destroy() {
//
//                    }
//                })
//        );
//    }

    @Bean
    public IMap<String, Set<Subscription>> getSubscriptionsPerStopPoint(){
        return hazelcast.getMap("ukur.subscriptionsPerStop");
    }

    @Bean
    public IMap<String, List<PushMessage>> getPushMessagesMemoryStore(){
        return hazelcast.getMap("ukur.pushMessagesMemoryStore");
    }

    public String listNodes(boolean includeStats) {
        JSONObject root = new JSONObject();
        JSONArray clusterMembers = new JSONArray();
        Cluster cluster = hazelcast.getCluster();
        if (cluster != null) {
            Set<Member> members = cluster.getMembers();
            if (members != null && !members.isEmpty()) {
                for (Member member : members) {

                    JSONObject obj = new JSONObject();
                    obj.put("uuid", member.getUuid());
                    obj.put("host", member.getAddress().getHost());
                    obj.put("port", member.getAddress().getPort());
                    obj.put("local", member.localMember());

                    if (includeStats) {
                        JSONObject stats = new JSONObject();
                        Collection<DistributedObject> distributedObjects = hazelcast.getDistributedObjects();
                        for (DistributedObject distributedObject : distributedObjects) {
                            stats.put(distributedObject.getName(), hazelcast.getMap(distributedObject.getName()).getLocalMapStats().toJson());
                        }

                        obj.put("localmapstats", stats);
                    }
                    clusterMembers.add(obj);
                }
            }
        }
        root.put("members", clusterMembers);
        return root.toString();
    }

}
