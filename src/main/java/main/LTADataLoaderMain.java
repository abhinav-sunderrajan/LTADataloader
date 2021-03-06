package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.json.simple.JSONObject;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlInput;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlSubmitInput;

import dataretrievers.DataRetriever;
import dataretrievers.TrafficImagesDatabaseLoader;
import dataretrievers.TrafficSpeedBandDatabaseLoader;
import utils.DatabaseAccess;

public class LTADataLoaderMain {
	private static ScheduledExecutorService executor;
	private static Properties dbConnectionProperties;
	private static Properties configProperties;
	private static final String URLS[] = {
			 "http://datamall.mytransport.sg/ltaodataservice.svc/TrafficSpeedBandSet", 
			"http://datamall2.mytransport.sg/ltaodataservice/Traffic-Images" };

	private static final Logger LOGGER = Logger
			.getLogger(LTADataLoaderMain.class);
	private static final WebClient webClient = new WebClient();

	private static class RefreshUID implements Runnable {

		@Override
		public void run() {
			try {
				HtmlPage page = webClient
						.getPage("http://datamall.mytransport.sg/tool.aspx");
				HtmlInput accKey = page.getElementByName("tbAccountKey");
				accKey.setValueAttribute(
						configProperties.getProperty("AccountKey"));

				HtmlSubmitInput generateUIDButton = page
						.getElementByName("btnGenerateUUID");
				page = generateUIDButton.click();

				HtmlInput uid = page.getElementByName("tbUniqueUserID");
				configProperties.setProperty("UniqueUserID",
						uid.getValueAttribute());
				LOGGER.info("Set the new value of UID to "
						+ uid.getValueAttribute());

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

			dbConnectionProperties = new Properties();
			configProperties = new Properties();
			if (args.length == 2) {
				configProperties.load(new FileInputStream(args[1]));
				dbConnectionProperties.load(new FileInputStream(args[0]));
			} else {
				configProperties.load(new FileInputStream(
						"src/main/resources/config.properties"));
				dbConnectionProperties.load(new FileInputStream(
						"src/main/resources/connection.properties"));

			}

			executor = Executors.newScheduledThreadPool(URLS.length * 2 + 1);
			executor.scheduleAtFixedRate(new RefreshUID(), 0, 720,
					TimeUnit.MINUTES);

			// DateTimeParser[] parsers = {
			// DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").getParser(),
			// DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").getParser() };
			// DateTimeFormatter df = new
			// DateTimeFormatterBuilder().append(null, parsers)
			// .toFormatter();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		launchThreads();

	}

	private static void launchThreads() {
		try {
			for (String url : URLS) {
				DatabaseAccess access = new DatabaseAccess(
						dbConnectionProperties);

				if (url.contains("TrafficSpeedBandSet")) {
					Queue<Document> speedbandQueue = new ConcurrentLinkedQueue<Document>();
					executor.scheduleAtFixedRate(
							new DataRetriever<Document>(url, configProperties,
									speedbandQueue),
							1, 300, TimeUnit.SECONDS);
					executor.schedule(new TrafficSpeedBandDatabaseLoader(
							speedbandQueue, access), 2, TimeUnit.SECONDS);

				}
				if (url.contains("Traffic-Images")) {
					Queue<JSONObject> imagesQueue = new ConcurrentLinkedQueue<JSONObject>();
					executor.scheduleAtFixedRate(
							new DataRetriever<JSONObject>(url, configProperties,
									imagesQueue),
							2, 60, TimeUnit.SECONDS);
					final ScheduledFuture<?> future =	executor.schedule(new TrafficImagesDatabaseLoader(
							imagesQueue, access), 2, TimeUnit.SECONDS);
					
					executor.execute(new Runnable() {
						 
					    @Override
					    public void run() {
					        try {
					            future.get();
					        } catch (InterruptedException e) {
					            LOGGER.error("Scheduled execution was interrupted", e);
					        } catch (CancellationException e) {
					        	LOGGER.warn("Watcher thread has been cancelled", e);
					        } catch (ExecutionException e) {
					        	LOGGER.error("Uncaught exception in scheduled execution", e.getCause());
					        }
					    }
					 
					});
					
				}

			}
		} catch (IOException e) {
			LOGGER.error("Error while parsing config properties file", e);
		}

	}
}
