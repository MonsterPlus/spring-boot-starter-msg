import com.huake.msg.kafka.Application;
import com.huake.msg.kafka.utils.KafkaUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Application.class)
public class kafkaTestConsuner {
	@Test
	public void testConsumer() {

		KafkaUtils.consumerClient("test");
	}

	@Test
	public void testForSubscribe() {

		KafkaUtils.recordForSubscribe("test").forEach(record -> {
			System.out.println("m2 offset : {" + record.offset() + "} , value : {" + record.value()
					+ "},channel :{ test } topic : {" + record.topic() + "}");
		});
	}

	@Test
	public void testForAssign() {

		KafkaUtils.recordForAssign("test").forEach(record -> {
			System.out.println("m2 offset : {" + record.offset() + "} , value : {" + record.value()
					+ "},channel :{ test } topic : {" + record.topic() + "}");
		});
	}
}
