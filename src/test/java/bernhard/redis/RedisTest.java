package bernhard.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.HyperLogLogOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.data.redis.support.collections.RedisZSet;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private ProductService productService;

    @Autowired
    private CacheManager cacheManager;

    @Test
    void redisTemplate() {
        assertNotNull(redisTemplate);
    }

    @Test
    void string() throws InterruptedException {
        ValueOperations<String, String> operations = redisTemplate.opsForValue();
        operations.set("name", "Bernhard", Duration.ofSeconds(2));

        assertEquals("Bernhard", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        assertNull(operations.get("name"));

    }

    @Test
    void list() {

        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("names", "Bernhard");
        operations.rightPush("names", "Winner");
        operations.rightPush("names", "Manurung");

        assertEquals("Bernhard", operations.leftPop("names"));
        assertEquals("Winner", operations.leftPop("names"));
        assertEquals("Manurung", operations.leftPop("names"));
    }

    @Test
    void set() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();
        operations.add("students", "Bernhard");
        operations.add("students", "Bernhard");
        operations.add("students", "Winner");
        operations.add("students", "Winner");
        operations.add("students", "Manurung");
        operations.add("students", "Manurung");

        assertEquals(3,operations.members("students").size());
        assertThat(operations.members("students"), hasItems("Bernhard", "Winner", "Manurung"));

        redisTemplate.delete("students");
    }

    @Test
    void zSet() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();
        operations.add("score", "Bernhard", 100);
        operations.add("score", "Winner", 85);
        operations.add("score", "Manurung", 95);


        assertEquals("Bernhard", operations.popMax("score").getValue());
        assertEquals("Manurung", operations.popMax("score").getValue());
        assertEquals("Winner", operations.popMax("score").getValue());

    }

    @Test
    void hash() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();
//        operations.put("user:1", "id", "1");
//        operations.put("user:1", "name", "Bernhard");
//        operations.put("user:1", "email", "bernhardwinner@gmail.com");
//
        Map<Object, Object> map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "Bernhard");
        map.put("email", "bernhardwinner@gmail.com");
        operations.putAll("user:1", map);

        assertEquals("1", operations.get("user:1", "id"));
        assertEquals("Bernhard", operations.get("user:1", "name"));
        assertEquals("bernhardwinner@gmail.com", operations.get("user:1", "email"));

        redisTemplate.delete("user:1");
    }

    @Test
    void geo() {
        GeoOperations<String, String> operations = redisTemplate.opsForGeo();
        operations.add("sellers", new Point(106.822702, -6.177590), "Toko A");
        operations.add("sellers", new Point(106.820889, -6.174964), "Toko B");

        Distance distance = operations.distance("sellers", "Toko A", "Toko B", Metrics.KILOMETERS);
        assertEquals(0.3543, distance.getValue());

        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers = operations
                .search("sellers", new Circle(
                        new Point(106.821825, -6.175105),
                        new Distance(5, Metrics.KILOMETERS)
                ));
        assertEquals(2, sellers.getContent().size());
        assertEquals("Toko A", sellers.getContent().get(0).getContent().getName());
        assertEquals("Toko B", sellers.getContent().get(1).getContent().getName());
    }

    @Test
    void hyperLogLog() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();
        operations.add("traffics", "eko", "kurniawan", "khannedy");
        operations.add("traffics,", "eko", "budi", "joko");
        operations.add("traffics", "budi", "joko", "rully");

        assertEquals(6L, operations.size("traffics"));
    }

    @Test
    void transaction() {
        redisTemplate.execute(new SessionCallback<>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForValue().set("test1", "Bernhard", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Winner", Duration.ofSeconds(2));
                operations.exec();
                return null;
            }
        });

        assertEquals("Bernhard", redisTemplate.opsForValue().get("test1"));
        assertEquals("Winner", redisTemplate.opsForValue().get("test2"));
    }

    @Test
    void pipeline() {
        List<Object> list = redisTemplate.executePipelined(new SessionCallback<>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Bernhard");
                operations.opsForValue().set("test2", "Bernhard");
                operations.opsForValue().set("test3", "Bernhard");
                operations.opsForValue().set("test4", "Bernhard");
                return null;
            }
        });
        assertThat(list, hasSize(4));
        assertThat(list, hasItem(true));
        assertThat(list, not(hasItem(false)));
    }

    @Test
    void publishStream() {
        StreamOperations<String, Object, Object> operations = redisTemplate.opsForStream();
        MapRecord<String, String, String> record = MapRecord.create("stream-1", Map.of(
                "name", "Bernhard Winner",
                "address", "Indonesia"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(record);
        }
    }

    @Test
    void subscribeStream() {
        StreamOperations<String, Object, Object> operations = redisTemplate.opsForStream();
        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception) {
            //group already exists
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"),
                StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for(MapRecord<String, Object, Object> record : records ) {
            System.out.println(record);
        }
    }

    @Test
    void pubSub() {
        redisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());
                System.out.println("Receive message : " + event);
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            redisTemplate.convertAndSend("my-channel", "Hello World : " + i);
        }
    }

    @Test
    void redisList() {
        List<String> list = RedisList.create("names", redisTemplate);
        list.add("Bernhard");
        list.add("Winner");
        list.add("Manurung");

        List<String> names = redisTemplate.opsForList().range("names", 0, -1);

        assertThat(list, hasItems("Bernhard", "Winner", "Manurung"));
        assertThat(names, hasItems("Bernhard", "Winner", "Manurung"));
    }

    @Test
    void redisSet() {
        Set<String> set = RedisSet.create("traffic", redisTemplate);
        set.addAll(Set.of("Bernhard", "Winner", "Manurung"));
        set.addAll(Set.of("Bernhard", "Sarah", "Segalciano"));
        assertThat(set, hasItems("Bernhard", "Winner", "Manurung", "Sarah", "Segalciano"));

        Set<String> members = redisTemplate.opsForSet().members("traffic");

        assertThat(members, hasItems("Bernhard", "Winner", "Manurung", "Sarah", "Segalciano"));
    }

    @Test
    void redisZSet() {
        RedisZSet<String> set = RedisZSet.create("winner", redisTemplate);
        set.add("Bernhard", 90);
        set.add("Sarah", 100);
        set.add("Segalciano", 95);
        assertThat(set, hasItems("Bernhard", "Sarah", "Segalciano"));

        Set<String> members = redisTemplate.opsForZSet().range("winner", 0, -1);

        assertThat(members, hasItems("Bernhard", "Sarah", "Segalciano"));
        assertEquals("Sarah", set.popLast());
        assertEquals("Segalciano", set.popLast());
        assertEquals("Bernhard", set.popLast());
    }

    @Test
    void redisMap() {
        Map<String, String> map = new DefaultRedisMap<>("user:1", redisTemplate);
        map.put("name", "Bernhard");
        map.put("address", "Indonesia");

        assertThat(map, hasEntry("name", "Bernhard"));
        assertThat(map, hasEntry("address", "Indonesia"));

        Map<Object, Object> user = redisTemplate.opsForHash().entries("user:1");

        assertThat(user, hasEntry("name", "Bernhard"));
        assertThat(user, hasEntry("address", "Indonesia"));
    }


    @Test
    void repository() {
        Product product = Product
                .builder()
                .id("1")
                .name("Mie Ayam Goreng")
                .price(20_000L)
                .build();
        productRepository.save(product);

        Map<Object, Object> map = redisTemplate.opsForHash().entries("products:1");
        assertEquals(product.getId(), map.get("id"));
        assertEquals(product.getName(), map.get("name"));
        assertEquals(product.getPrice().toString(), map.get("price"));

        Product product2 = productRepository.findById("1").get();
        assertEquals(product, product2);

    }

    @Test
    void ttlRedis() throws InterruptedException {
        Product product = Product
                .builder()
                .id("1")
                .name("Mie Ayam Goreng")
                .price(20_000L)
                .ttl(3L)
                .build();
        productRepository.save(product);

        assertTrue(productRepository.findById("1").isPresent());
        Thread.sleep(Duration.ofSeconds(5));

        assertFalse(productRepository.findById("1").isPresent());
    }

    @Test
    void cache() {
        Cache sample = cacheManager.getCache("scores");
        sample.put("Bernhard", 90);
        sample.put("Sarah", 100);

        assertEquals(100, sample.get("Sarah", Integer.class));
        assertEquals(90, sample.get("Bernhard", Integer.class));

        sample.evict("Sarah");
        sample.evict("Bernhard");
        assertNull(sample.get("Sarah", Integer.class));
        assertNull(sample.get("Bernhard", Integer.class));
    }

    @Test
    void findProduct() {
        Product product = productService.getProduct("P-001");
        assertNotNull(product);
        assertEquals("P-001", product.getId());
        assertEquals("Sample", product.getName());

        Product product2 = productService.getProduct("P-001");
        assertEquals(product, product2);

    }

    @Test
    void saveProduct() {
        Product product = Product.builder().id("P002").name("Sample").build();
        productService.save(product);

        Product product2 = productService.getProduct("P002");
        assertEquals(product, product2);

    }

    @Test
    void removeProduct() {
        Product product = productService.getProduct("P003");
        assertNotNull(product);

        productService.remove("P003");

        Product product2 = productService.getProduct("P003");
        assertEquals(product, product2);

    }
}
