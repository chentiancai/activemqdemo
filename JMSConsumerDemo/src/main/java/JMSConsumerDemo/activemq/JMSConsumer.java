package JMSConsumerDemo.activemq;

import java.text.SimpleDateFormat;
import java.util.Date;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 消息的消费者（接收者）。多线程启动
 *
 */
public class JMSConsumer {
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
		connectionFactory = new ActiveMQConnectionFactory(JMSConsumer.USERNAME, JMSConsumer.PASSWORD,
				JMSConsumer.BROKEURL);
		// 开启5个线程，不断接收消息
		for (int i = 0; i < 2; i++) {
			String consumerName = "JMSConsumer_" + String.valueOf(i);
			Thread t = new Thread(new Runnable() {
				public void run() {
					Connection connection = null;// 连接

					Session session;// 会话 接受或者发送消息的线程
					Destination destination;// 消息的目的地

					MessageConsumer messageConsumer;// 消息的消费者

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
						// 创建一个连接HelloWorld的消息队列
						destination = session.createQueue("HelloWorld");
						// 创建消息消费者
						messageConsumer = session.createConsumer(destination);

						while (true) {
							TextMessage textMessage = (TextMessage) messageConsumer.receive();
							if (textMessage != null) {
								String dateString = sdf.format(new Date());
								String text = String.format("\t%s %s ：收到消息：[%s] ", Thread.currentThread().getName(),
										dateString, textMessage.getText());

								System.out.println(text);
							} else {
								System.out.println(Thread.currentThread().getName() + "没有收到消息，结束循环");
								break;
							}
						}
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
			}, consumerName);
			t.start();
		}
	}
}
