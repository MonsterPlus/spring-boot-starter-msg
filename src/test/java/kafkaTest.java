import com.huake.msg.kafka.Application;
import com.huake.msg.kafka.mode.RetryModel;
import com.huake.msg.kafka.mode.impl.DefaultMessageModel;
import com.huake.msg.kafka.utils.KafkaUtils;
import com.huake.msg.kafka.utils.SpringFactoriesUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class kafkaTest {

	@Test
	public void testRetry() {
		try {
			DefaultMessageModel msg = getDefaultMessageModel();
			KafkaUtils.resend(msg,SpringFactoriesUtils.loadFactories(RetryModel.class),null,null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	@Test
	public void testSend() {
		try {
			DefaultMessageModel msg = getDefaultMessageModel();
			KafkaUtils.send(msg,null,null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private DefaultMessageModel getDefaultMessageModel() {
		DefaultMessageModel msg = new DefaultMessageModel();
		Map<String, Object> headerMap = new HashMap<>();
		Map<String, Object> oj = new HashMap<>();
		Map<String, Object> resume = new HashMap<>();
		Map<String, Object> bodyMap = new HashMap<>();
		oj.put("name", "罗辑");
		oj.put("age", 24);
		oj.put("gender", 1);
		headerMap.put("user", oj);
		resume.put("School", "清华大学");
		resume.put("Record1", "清华大学社会关系学教授 ");
		resume.put("Record2", "宇宙社会学创始人");
		resume.put("Record3", "人类面壁计划亚洲区面壁者");
		resume.put("Record4", "威慑纪元执剑人");
		resume.put("Record5", "守护人类文明，威慑三体文明侵略之先行者");
		bodyMap.put("resume", resume);
		msg.setHeaders(headerMap);
		msg.setBody(bodyMap);
		return msg;
	}
}
