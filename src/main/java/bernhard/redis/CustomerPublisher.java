package bernhard.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class CustomerPublisher {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
    public void publish() {
        redisTemplate.convertAndSend("customers", "Bernhard " + UUID.randomUUID());
    }
}
