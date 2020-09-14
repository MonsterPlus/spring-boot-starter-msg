import com.huake.msg.kafka.Application;
import com.huake.msg.kafka.category.ConsumptionCategory;
import com.huake.msg.kafka.utils.KafkaUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Application.class)
public class kafkaTestConsuner {

	@Test
	public void testForSubscribe() {

		KafkaUtils.consumerClient("test", ConsumptionCategory.SUBSCRIBE);
	}

	@Test
	public void testForAssign() {

		KafkaUtils.consumerClient("test", ConsumptionCategory.ASSIGN);
	}
}
