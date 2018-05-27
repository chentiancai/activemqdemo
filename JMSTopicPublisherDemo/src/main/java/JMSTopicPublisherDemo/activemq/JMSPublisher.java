package JMSTopicPublisherDemo.activemq;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * mq主题发布者!
 *
 */
public class JMSPublisher {
	// 发送的消息数量
	private static final int SENDNUM = 1000000;

	// 连接用户名
	private static final String USERNAME = "mq_xiaotiancai";
	// 连接密码
	private static final String PASSWORD = "xiaotiancai12345678";
	// 连接地址
	private static final String BROKEURL = "tcp://193.112.124.124:61616";
	// 连接工厂
	private static ConnectionFactory connectionFactory = null;

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

	public static void main(String[] args) {
		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKEURL);
		// 开启2个生产者线程，不断发送消息
		for (int i = 0; i < 1; i++) {
			String producerName = "JMSPublisher_" + String.valueOf(i);
			Thread t = new Thread(new Runnable() {
				public void run() {
					// 连接
					Connection connection = null;
					// 会话 接受或者发送消息的线程
					Session session;
					// 消息的目的地
					Destination destination;
					// 消息生产者
					MessageProducer messageProducer;
					try {
						// 通过连接工厂获取连接
						connection = connectionFactory.createConnection();
						connection.setClientID(Thread.currentThread().getName());
						// 启动连接
						connection.start();
						// 创建session
						/*
						 * 参数说明: arg0:boolean,是否开启事务 arg1:int,应答模式(1:自动应答,2:手动应答) 当使用事务时,第二个参数无效
						 */
						session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
						// 创建一个名称为HelloWorld的消息队列
						// Destination ：消息的目的地;消息发送给谁.
						// 获取session注意参数值HelloWorld是Queue的名字
						// destination = session.createQueue("HelloWorld");

						// =======================================================
						// 点对点与订阅模式唯一不同的地方，就是这一行代码，点对点创建的是Queue，而订阅模式创建的是Topic
						destination = session.createTopic("myTopic");
						// =======================================================

						// 创建消息生产者
						messageProducer = session.createProducer(destination);
						/**
						 * 设置投递方式 DeliveryMode.NON_PERSISTENT:不持久化
						 * 
						 * DeliveryMode.PERSISTENT:持久化
						 */
						messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
						int count = 0;
						while (count < SENDNUM) {
							String dateString = sdf.format(new Date());
							String text = String.format("%s 说：现在是 %s，数据是：%s ", Thread.currentThread().getName(),
									dateString, String.valueOf(new Random().nextInt(100000)));
							// 创建一条文本消息
							TextMessage message = session.createTextMessage(text);
							System.out.println("Producer:" + text);
							// 通过消息生产者发出消息
							messageProducer.send(message);
							count++;
							Thread.sleep(new Random().nextInt(1000)); // 睡眠一会
						}
						messageProducer.close();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (connection != null) {
							try {
								connection.close();
							} catch (JMSException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}, producerName);
			t.start();
		}
	}
}
