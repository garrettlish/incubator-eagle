package org.apache.eagle.service.app;

import org.apache.eagle.alert.utils.ZookeeperEmbedded;

public class ServiceAppWithZk {

	public static void main(String[] args) {
		ZookeeperEmbedded zkEmbed = new ZookeeperEmbedded(2181);
		try {
			zkEmbed.start();
			Thread.sleep(3000);
			
			new ServiceApp().run(new String[] {"server"});
			
			Thread.currentThread().join();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			zkEmbed.shutdown();
		}
	}

}
