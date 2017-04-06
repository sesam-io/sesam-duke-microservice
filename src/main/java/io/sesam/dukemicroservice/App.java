package io.sesam.dukemicroservice;

import static io.sesam.dukemicroservice.IncrementalRecordLinkageLuceneDatabase.DATASET_ID_PROPERTY_NAME;
import static io.sesam.dukemicroservice.IncrementalRecordLinkageLuceneDatabase.GROUP_NO_PROPERTY_NAME;
import static io.sesam.dukemicroservice.IncrementalRecordLinkageLuceneDatabase.ORIGINAL_ENTITY_ID_PROPERTY_NAME;
import static spark.Spark.get;
import static spark.Spark.halt;
import static spark.Spark.post;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import no.priv.garshol.duke.ConfigLoader;
import no.priv.garshol.duke.Configuration;
import no.priv.garshol.duke.ConfigurationImpl;
import no.priv.garshol.duke.DataSource;
import no.priv.garshol.duke.Database;
import no.priv.garshol.duke.Processor;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.PropertyImpl;
import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.RecordIterator;
import no.priv.garshol.duke.datasources.Column;


public class App {
    private class RecordLinkage {
        final Map<String, IncrementalRecordLinkageDataSource> dataSetId2dataSource;
        final IncrementalRecordLinkageMatchListener matchListener;
        final Processor processor;
        final Configuration config;
        final Lock lock = new ReentrantLock();

        RecordLinkage(Map<String, IncrementalRecordLinkageDataSource> dataSetId2dataSource,
                             IncrementalRecordLinkageMatchListener matchListener, Processor processor,
                             Configuration config) {
            this.dataSetId2dataSource = dataSetId2dataSource;
            this.matchListener = matchListener;
            this.processor = processor;
            this.config = config;
        }
    }

    private Map<String, RecordLinkage> recordLinkages = new HashMap<>();


    private Logger logger = LoggerFactory.getLogger(App.class);


    public App() throws Exception {
        // Create the Duke processor

        // TODO: load the config from an environment variable or something.

        String configFileName = "classpath:testdukeconfig.xml";
        Reader configFileReader;
        if (configFileName.startsWith("classpath:")) {
            String resource = configFileName.substring("classpath:".length());
            ClassLoader cloader = Thread.currentThread().getContextClassLoader();
            InputStream istream = cloader.getResourceAsStream(resource);
            configFileReader = new InputStreamReader(istream);
        } else {
            configFileReader = new FileReader(configFileName);
        }

        logger.info("Parsing the config-file...");

        // TODO: perhaps get the datafolder from config?
        Path rootDataFolder = Paths.get("").toAbsolutePath().resolve("data");

        XPath xPath = XPathFactory.newInstance().newXPath();

        InputSource xml = new InputSource(configFileReader);
        NodeList recordLinkageNodes = (NodeList) xPath.evaluate("//DukeMicroService/RecordLinkage", xml, XPathConstants.NODESET);

        for(int x=0; x<recordLinkageNodes.getLength(); x++) {
            Node recordLinkageNode = recordLinkageNodes.item(x);
            String recordLinkageName = recordLinkageNode.getAttributes().getNamedItem("name").getTextContent();
            logger.info("  Parsing the config for the '{}' recordLinkage.", recordLinkageName);

            Path recordLinkDataFolder = rootDataFolder.resolve(recordLinkageName);

            Node dukeConfigNode = null;
            NodeList childNodes = recordLinkageNode.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node childNode = childNodes.item(i);
                if (childNode.getNodeName().equals("duke")) {
                    dukeConfigNode = childNode;
                    break;
                }
            }
            if (dukeConfigNode == null) {
                throw new RuntimeException(String.format("The recordLinkage '%s' didn't contain a <duke> element!", recordLinkageName));
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            File dukeConfigFile = File.createTempFile("dukeconfig_" + recordLinkageName + "_", ".xml");
            try {
                FileOutputStream resultStream = new FileOutputStream(dukeConfigFile);
                StreamResult result = new StreamResult(resultStream);
                transformer.transform(new DOMSource(dukeConfigNode), result);
                resultStream.close();

                ConfigurationImpl config = (ConfigurationImpl) ConfigLoader.load(dukeConfigFile.getAbsolutePath());

                // Add the special "id", "datasetId" and "groupNo" properties.
                List<Property> properties = config.getProperties();
                for (Property property: properties) {
                    if (property.isIdProperty()) {
                        throw new RuntimeException(String.format("    The schema contained an 'id'-property: '%s'", property.getName()));
                    }
                }

                PropertyImpl IdProperty = new PropertyImpl("ID");
                assert IdProperty.isIdProperty();
                properties.add(IdProperty);

                PropertyImpl datasetIdProperty = new PropertyImpl(DATASET_ID_PROPERTY_NAME, null, 0, 0);
                datasetIdProperty.setIgnoreProperty(true);
                properties.add(datasetIdProperty);

                PropertyImpl originalEntityIdProperty = new PropertyImpl(ORIGINAL_ENTITY_ID_PROPERTY_NAME, null, 0, 0);
                originalEntityIdProperty.setIgnoreProperty(true);
                properties.add(originalEntityIdProperty);

                PropertyImpl groupNoProperty = new PropertyImpl(GROUP_NO_PROPERTY_NAME, null, 0, 0);
                groupNoProperty.setIgnoreProperty(true);
                properties.add(groupNoProperty);

                config.setProperties(properties);

                logger.info("  Created the Duke config for the recordLinkage '{}' ok.", recordLinkageName);

                IncrementalRecordLinkageLuceneDatabase database = new IncrementalRecordLinkageLuceneDatabase();
                Path luceneFolderPath = recordLinkDataFolder.resolve("lucene-index");
                File luceneFolderFile = luceneFolderPath.toFile();
                boolean wasCreated = luceneFolderFile.mkdirs();
                if (wasCreated) {
                    logger.info("  Created the folder '{}'.", luceneFolderPath.toString());
                }
                if (!luceneFolderFile.exists()) {
                    throw new RuntimeException(String.format("Failed to create the folder '%s'!", luceneFolderPath.toString()));
                }
                logger.info("  Using this folder for the lucene index: '{}'.", luceneFolderPath.toString());
                database.setPath(luceneFolderPath.toString());
                config.setDatabase(database);
                Processor processor = new Processor(config, false);


                Path h2DatabasePath = recordLinkDataFolder.resolve("recordlinkdatabase");
                File h2DatabaseFolder = h2DatabasePath.toFile().getParentFile();
                wasCreated = h2DatabaseFolder.mkdirs();
                if (wasCreated) {
                    logger.info("  Created the folder '{}'.", h2DatabaseFolder.getAbsolutePath());
                }
                if (!h2DatabaseFolder.exists()) {
                    throw new RuntimeException(String.format("Failed to create the folder '%s'!", h2DatabasePath.toString()));
                }

                logger.info("  Using this folder for the h2 record-link database: '{}'.", h2DatabasePath.toString());

                IncrementalRecordLinkageMatchListener incrementalRecordLinkageMatchListener = new IncrementalRecordLinkageMatchListener(
                        this,
                        recordLinkageName,
                        h2DatabasePath.toString());
                processor.addMatchListener(incrementalRecordLinkageMatchListener);

                // Load the datasources for the two groups
                Map<String, IncrementalRecordLinkageDataSource> dataSetId2dataSource = new HashMap<>();
                java.util.function.Consumer<Integer> addDataSourcesForGroup = (Integer groupNo) ->
                {
                    Collection<DataSource> dataSources = config.getDataSources(groupNo);

                    if (dataSources.isEmpty()) {
                        throw new RuntimeException(
                                String.format(
                                        "Got zero datasources for group %d in the recordLinkage '%s'!",
                                        groupNo,
                                        recordLinkageName));
                    }

                    for (DataSource dataSource : dataSources) {
                        if (dataSource instanceof IncrementalRecordLinkageDataSource) {
                            IncrementalRecordLinkageDataSource incrementalRecordLinkageDataSource = (IncrementalRecordLinkageDataSource) dataSource;
                            String datasetId = incrementalRecordLinkageDataSource.getDatasetId();
                            if (datasetId == null || datasetId.isEmpty()) {
                                throw new RuntimeException(
                                        String.format("Got a DataSource with no datasetId property in the recordLinkage '%s'!",
                                                      recordLinkageName));
                            }

                            for (Column column: incrementalRecordLinkageDataSource.getColumns()) {
                                if (column.getName().toLowerCase().equals("_id") || column.getName().toLowerCase().equals("id")) {
                                    throw new RuntimeException(
                                            String.format("The DataSource '%s' in the recordLinkage '%s' contained an '%s' column!!",
                                                          datasetId, recordLinkageName, column.getName()));
                                }
                            }

                            incrementalRecordLinkageDataSource.setGroupNo(groupNo);
                            dataSetId2dataSource.put(datasetId, incrementalRecordLinkageDataSource);
                            logger.info("    Added the datasource '{}' to group {}", datasetId, groupNo);
                        } else {
                            throw new RuntimeException(
                                    String.format("Got a DataSource of the unsupported type '%s' in the recordLinkage '%s'!",
                                                  dataSource.getClass().getName(), recordLinkageName));
                        }
                    }
                };
                addDataSourcesForGroup.accept(1);
                addDataSourcesForGroup.accept(2);

                recordLinkages.put(recordLinkageName, new RecordLinkage(dataSetId2dataSource,
                                                                        incrementalRecordLinkageMatchListener,
                                                                        processor,
                                                                        config));

            } finally {
                dukeConfigFile.delete();
            }
        }
        logger.info("Done parsing the config-file.");
    }

    public static void main(String[] args) throws Exception {
        App app = new App();
        Gson gson = new Gson();

        /*
         * This endpoint is used to do record linkage. The results can be read from the
         * GET /recordlinkage/:recordLinkage endpoint.
         */
        post("/recordlinkage/:recordLinkage/:datasetId", (req, res) -> {
            String recordLinkageName = req.params("recordLinkage");
            String datasetId = req.params("datasetId");

            if (recordLinkageName.isEmpty()) {
                halt(400, "The recordLinkage cannot be an empty string!");
            }

            if (datasetId.isEmpty()) {
                halt(400, "The datasetId cannot be an empty string!");
            }

            RecordLinkage recordLinkage = app.recordLinkages.get(recordLinkageName);
            if (recordLinkage == null) {
                halt(400, String.format("Unknown recordLinkage '%s'! (All recordLinkages must be specified in the configuration)",
                                        recordLinkageName));
            }

            recordLinkage.lock.lock();
            try {

                IncrementalRecordLinkageDataSource dataSource = recordLinkage.dataSetId2dataSource.get(datasetId);
                if (dataSource == null) {
                    halt(400, String.format("Unknown dataset-id '%s' for the recordLinkage '%s'!", datasetId, recordLinkageName));
                }

                res.type("application/json");

                JsonArray elementsInBatch;
                // We assume that we get the records in small batches, so that it is ok to load the entire request into memory.
                String requestBody = req.body();
                try {
                    elementsInBatch = gson.fromJson(requestBody, JsonArray.class);
                } catch (JsonSyntaxException e) {
                    // The request can contain either an array of entities, or one single entity.
                    JsonObject singleElement = gson.fromJson(requestBody, JsonObject.class);
                    elementsInBatch = new JsonArray();
                    elementsInBatch.add(singleElement);
                }

                // use the DataSource for the datasetId to convert the JsonObjects into Record objects.
                dataSource.setDatasetEntitiesBatch(elementsInBatch);
                RecordIterator it = dataSource.getRecords();
                List<Record> records = new LinkedList<>();
                List<Record> deletedRecords = new LinkedList<>();
                while (it.hasNext()) {
                    Record record = it.next();
                    if ("true".equals(record.getValue("_deleted"))) {
                        deletedRecords.add(record);
                    } else {
                        records.add(record);
                    }
                }

                Processor processor = recordLinkage.processor;
                IncrementalRecordLinkageLuceneDatabase database = (IncrementalRecordLinkageLuceneDatabase) processor.getDatabase();

                // Note that we run deduplicate(), even though we are actually doing record linkage. This works because we have
                // plugged in our own Database, DataSource and MatchListener classes.
                IncrementalRecordLinkageMatchListener incrementalRecordLinkageMatchListener = recordLinkage.matchListener;

                try {
                    // When we get a record with "_deleted"=True, we must do the following:
                    // 1. Delete the record from the lucene index
                    // 2. If the record appears in the RECORDLINKAGE table:
                    //      Mark that row as "deleted=true".
                    //      Mark the other record in that row as needing a rematching (add a row to the RECORDLINKS_THAT_NEEDS_REMATCH table)
                    // 3. If the record appears in the RECORDLINKS_THAT_NEEDS_REMATCH table:
                    //      Delete the row.
                    Set<String> recordsIdThatNeedsReprocessing = new HashSet<>();
                    for (Record record : deletedRecords) {
                        database.delete(record);

                        recordsIdThatNeedsReprocessing.addAll(incrementalRecordLinkageMatchListener.delete(record));
                    }

                    List<Record> recordsThatNeedsReprocessing = new LinkedList<>();
                    for (String recordId : recordsIdThatNeedsReprocessing) {
                        Record record = processor.getDatabase().findRecordById(recordId);
                        if (record != null) {
                            app.logger.info("Re-processing the record '{}', because of some other record being deleted. ", recordId);
                            recordsThatNeedsReprocessing.add(record);
                        }
                    }
                    if (!recordsThatNeedsReprocessing.isEmpty()) {
                        processor.deduplicate(recordsThatNeedsReprocessing);
                    }

                    // Process the actual records in the batch.
                    if (!records.isEmpty()) {
                        processor.deduplicate(records);
                    }

                    // The call to deduplicate() might have removed the matches for existing records because the new mappings had a higher
                    // confidence-level. In such cases we want to fallback to the second best match for the existing records.
                    int reprocessingRunNumber = 0;
                    while (true) {
                        if (reprocessingRunNumber > 10) {
                            app.logger.warn("Giving up reprocessing after {} attempts. Something is odd here; the matches don't seem to settle down...",
                                          reprocessingRunNumber);
                            break;
                        }
                        reprocessingRunNumber++;
                        recordsThatNeedsReprocessing.clear();
                        for (String recordId : incrementalRecordLinkageMatchListener.getRecordIdsThatNeedReprocessing()) {
                            Record record = processor.getDatabase().findRecordById(recordId);
                            if (record != null) {
                                app.logger.info("Reprocessing-run {} processing the record '{}'", reprocessingRunNumber, recordId);
                                recordsThatNeedsReprocessing.add(record);
                            } else {
                                app.logger.warn("Failed to reprocess the record '{}', since it couldn't be found!", recordId);
                            }
                        }
                        incrementalRecordLinkageMatchListener.clearRecordIdsThatNeedReprocessing();

                        if (recordsThatNeedsReprocessing.isEmpty()) {
                            break;
                        }
                        processor.deduplicate(recordsThatNeedsReprocessing);
                    }

                    incrementalRecordLinkageMatchListener.commitCurrentDatabaseTransaction();
                } catch (Exception e) {
                    // something went wrong, so rollback any changes to the RECORDLINK database table.
                    incrementalRecordLinkageMatchListener.rollbackCurrentDatabaseTransaction();
                    throw e;
                }

                Writer writer = res.raw().getWriter();
                writer.append("{\"success\": true}");
                writer.flush();
                return "";
            } finally {
                recordLinkage.lock.unlock();
            }
        });

        get("/recordlinkage/:recordLinkage", (req, res) -> {
            String recordLinkageName = req.params("recordLinkage");
            if (recordLinkageName.isEmpty()) {
                halt(400, "The recordLinkage cannot be an empty string!");
            }

            RecordLinkage recordLinkage = app.recordLinkages.get(recordLinkageName);
            IncrementalRecordLinkageMatchListener incrementalRecordLinkageMatchListener = recordLinkage.matchListener;
            if (incrementalRecordLinkageMatchListener == null) {
                halt(400, String.format("Unknown recordLinkage '%s'! (All recordLinkages must be specified in the configuration)",
                                        recordLinkageName));
            }

            String since = req.queryParams("since");

            res.status(200);
            res.type("application/json");
            Writer writer = res.raw().getWriter();

            incrementalRecordLinkageMatchListener.streamRecordLinksSince(since, writer);

            writer.flush();
            return "";
        });
    }

}