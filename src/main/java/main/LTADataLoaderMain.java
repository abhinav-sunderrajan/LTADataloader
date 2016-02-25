package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.dom4j.Document;

import utils.DatabaseAccess;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlInput;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlSubmitInput;

import dataretrievers.DataRetriever;
import dataretrievers.IncidentDatabaseLoader;
import dataretrievers.TrafficSpeedBandDatabaseLoader;

public class LTADataLoaderMain {
	private static Map<String, Queue<Document>> xmlDocQueueMap;
	private static ScheduledExecutorService executor;
	private static Properties dbConnectionProperties;
	private static Properties configProperties;
	private static final String URLS[] = {
			"http://datamall.mytransport.sg/ltaodataservice.svc/TrafficSpeedBandSet",
			"http://datamall.mytransport.sg/ltaodataservice.svc/IncidentSet" };

	private static final Logger LOGGER = Logger.getLogger(LTADataLoaderMain.class);
	private static final WebClient webClient = new WebClient();

	private static class RefreshUID implements Runnable {

		@Override
		public void run() {
			try {
				HtmlPage page = webClient.getPage("http://datamall.mytransport.sg/tool.aspx");
				HtmlInput accKey = page.getElementByName("tbAccountKey");
				accKey.setValueAttribute(configProperties.getProperty("AccountKey"));

				HtmlSubmitInput generateUIDButton = page.getElementByName("btnGenerateUUID");
				page = generateUIDButton.click();

				HtmlInput uid = page.getElementByName("tbUniqueUserID");
				configProperties.setProperty("UniqueUserID", uid.getValueAttribute());
				LOGGER.info("Set the new value of UID to " + uid.getValueAttribute());

			} catch (FailingHttpStatusCodeException e) {
				LOGGER.error("Error generating new UID", e);
			} catch (MalformedURLException e) {
				LOGGER.error("Error generating new UID", e);
			} catch (IOException e) {
				LOGGER.error("Error generating new UID", e);
			}

		}
	}

	public static void main(String[] args) {
		try {

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

				@Override
				public void run() {
					executor.shutdown();
				}
			}));

			xmlDocQueueMap = new HashMap<String, Queue<Document>>();
			dbConnectionProperties = new Properties();
			configProperties = new Properties();
			if (args.length == 2) {
				configProperties.load(new FileInputStream(args[1]));
				dbConnectionProperties.load(new FileInputStream(args[0]));
			} else {
				configProperties.load(new FileInputStream("src/main/resources/config.properties"));
				dbConnectionProperties.load(new FileInputStream(
						"src/main/resources/connection.properties"));

			}

			executor = Executors.newScheduledThreadPool(URLS.length * 3 + 1);
			executor.scheduleAtFixedRate(new RefreshUID(), 0, 900, TimeUnit.SECONDS);

			for (String url : URLS)
				xmlDocQueueMap.put(url, new ConcurrentLinkedQueue<Document>());

			launchThreads();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void launchThreads() {
		try {
			for (String url : URLS) {
				DatabaseAccess access = new DatabaseAccess(dbConnectionProperties);

				if (url.contains("TrafficSpeedBandSet")) {
					executor.scheduleAtFixedRate(new DataRetriever(url, configProperties,
							xmlDocQueueMap), 1, 300, TimeUnit.SECONDS);
					access.setBlockExecutePS(
							"INSERT INTO  trafficspeedbandset VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
							10);

					executor.schedule(new TrafficSpeedBandDatabaseLoader(xmlDocQueueMap.get(url),
							access), 2, TimeUnit.SECONDS);

				}

				if (url.contains("IncidentSet")) {
					executor.scheduleAtFixedRate(new DataRetriever(url, configProperties,
							xmlDocQueueMap), 2, 120, TimeUnit.SECONDS);
					access.setBlockExecutePS("INSERT INTO  incidentset VALUES (?,?,?,?,?,?,?)", 10);
					executor.schedule(new IncidentDatabaseLoader(xmlDocQueueMap.get(url), access),
							2, TimeUnit.SECONDS);

				}

			}
		} catch (IOException e) {
			LOGGER.error("Error while parsing config properties file", e);
		} catch (SQLException e) {
			LOGGER.error("Error creating prepared statement for block insert", e);
		}

	}
}
