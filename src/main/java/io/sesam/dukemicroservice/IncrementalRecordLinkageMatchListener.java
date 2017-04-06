package io.sesam.dukemicroservice;

import static io.sesam.dukemicroservice.IncrementalRecordLinkageLuceneDatabase.DATASET_ID_PROPERTY_NAME;
import static io.sesam.dukemicroservice.IncrementalRecordLinkageLuceneDatabase.ORIGINAL_ENTITY_ID_PROPERTY_NAME;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.matchers.MatchListener;


public class IncrementalRecordLinkageMatchListener implements MatchListener {
    private final App app;
    private final String recordLinkageName;
    private final Logger logger;
    private final Connection h2connection;

    public IncrementalRecordLinkageMatchListener(App app,
                                                 String recordLinkageName,
                                                 String h2DatabaseFilename) throws ClassNotFoundException, SQLException {
        this.app = app;
        this.recordLinkageName = recordLinkageName;
        this.logger = LoggerFactory.getLogger("recordLinkageMatchListener-" + recordLinkageName);

        Class.forName("org.h2.Driver");
        h2connection = DriverManager.getConnection("jdbc:h2:" + h2DatabaseFilename, "sa", "");
        h2connection.setAutoCommit(false);
        final double EPSILON = 0.001;

        DatabaseMetaData meta = h2connection.getMetaData();

        try (Statement stmt = h2connection.createStatement();) {
            stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS RECORDLINKS_UPDATED START WITH 1");
        }

        try (ResultSet res = meta.getTables(null, null, "RECORDLINKS",
                                            new String[]{"TABLE"});) {

            if (!res.next()) {
                logger.info("Creating the 'RECORDLINKS' database table.");
                try (Statement stmt = h2connection.createStatement();) {
                    String sql = "CREATE TABLE RECORDLINKS " +
                            "(record1id NVARCHAR not NULL, " +
                            " record2id NVARCHAR not NULL, " +
                            " record1datasetId NVARCHAR not NULL, " +
                            " record2datasetId NVARCHAR not NULL, " +
                            " record1entityId NVARCHAR not NULL, " +
                            " record2entityId NVARCHAR not NULL, " +
                            " confidence NUMBER not NULL, " +
                            " updated BIGINT not NULL, " +
                            " deleted BOOL not NULL default FALSE, " +
                            " PRIMARY KEY (record1id))";
                    stmt.executeUpdate(sql);
                }

                logger.info("Creating the 'RECORDLINKS_THAT_NEEDS_REMATCH' database table.");
                try (Statement stmt = h2connection.createStatement();) {
                    String sql = "CREATE TABLE RECORDLINKS_THAT_NEEDS_REMATCH(" +
                            "recordId NVARCHAR not NULL, " +
                            "PRIMARY KEY (recordId))";
                    stmt.executeUpdate(sql);
                }
            }
        }
    }

    private void _matches(Record r1, Record r2, double confidence) {
        String r1Dataset = r1.getValue(IncrementalRecordLinkageLuceneDatabase.DATASET_ID_PROPERTY_NAME);
        String r2Dataset = r2.getValue(IncrementalRecordLinkageLuceneDatabase.DATASET_ID_PROPERTY_NAME);
        if (r1Dataset.equals(r2Dataset)) {
            throw new RuntimeException(
                    String.format(
                            "The matching records have the same dataset (%s)! This should never happen, since we are not supposed to match the "
                                    + " records from one dataset to other records in the same dataset! r1:%s  r2:%s", r1Dataset, r1, r2));
        }

        String r1GroupNo = r1.getValue(IncrementalRecordLinkageLuceneDatabase.GROUP_NO_PROPERTY_NAME);
        String r2GroupNo = r2.getValue(IncrementalRecordLinkageLuceneDatabase.GROUP_NO_PROPERTY_NAME);
        if (r1GroupNo.equals(r2GroupNo)) {
            throw new RuntimeException(
                    String.format(
                            "The matching records have the same groupNo (%s)! This should never happen, since we are not supposed to match the "
                                    + " records from one group to other records in the same group ! r1:%s  r2:%s", r1GroupNo, r1, r2));
        }

        Record recordFromGroup1, recordFromGroup2;
        if (r1GroupNo.equals("1")) {
            recordFromGroup1 = r1;
            recordFromGroup2 = r2;
        } else {
            recordFromGroup1 = r2;
            recordFromGroup2 = r1;
        }

        String recordFromGroup1Id = recordFromGroup1.getValue("ID");
        String recordFromGroup2Id = recordFromGroup2.getValue("ID");

        try {
            String existingPrimaryRecord2Id = null;
            double existingPrimaryConfidence = -1;
            boolean existingPrimaryDeleted = false;

            String existingSecondaryRecord1Id = null;
            double existingSecondaryConfidence = -1;

            // Check if there is a row for the primary record already
            try (PreparedStatement selectStatement = h2connection.prepareStatement(
                    "SELECT * FROM RECORDLINKS WHERE record1id=?")) {
                selectStatement.setNString(1, recordFromGroup1Id);
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    if (resultSet.next()) {
                        // There already exists a mapping for this entity
                        existingPrimaryRecord2Id = resultSet.getString("record2id");
                        existingPrimaryConfidence = resultSet.getDouble("confidence");
                        existingPrimaryDeleted = resultSet.getBoolean("deleted");
                    }
                }
            }

            // Check if there is a separate (undeleted) row for the secondary record already
            try (PreparedStatement selectStatement = h2connection.prepareStatement(
                    "SELECT * FROM RECORDLINKS WHERE record1id <> ? AND record2id=? and deleted=FALSE")) {
                selectStatement.setNString(1, recordFromGroup1Id);
                selectStatement.setNString(2, recordFromGroup2Id);
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    if (resultSet.next()) {
                        // There already exists a mapping for this entity
                        existingSecondaryRecord1Id = resultSet.getString("record1id");
                        existingSecondaryConfidence = resultSet.getDouble("confidence");
                    }
                }
            }

            // We will only make a change if the confidence is larger than in both the existing rows
            if ( (confidenceIsMeasurablyLarger(confidence, existingPrimaryConfidence) || existingPrimaryDeleted)
               && confidenceIsMeasurablyLarger(confidence, existingSecondaryConfidence)) {
                // We want to make a change.
                if (existingPrimaryRecord2Id != null) {
                    // The primary row already exists. If the existing record2 is a third record we must
                    // mark it for rematching.
                    if (!existingPrimaryRecord2Id.equals(recordFromGroup2Id)) {
                        try (PreparedStatement markAsRematchStatement = h2connection.prepareStatement(
                                "MERGE INTO RECORDLINKS_THAT_NEEDS_REMATCH KEY(recordId) VALUES(?)"
                        )) {
                            markAsRematchStatement.setNString(1, existingSecondaryRecord1Id);
                            markAsRematchStatement.executeUpdate();
                        }
                    }

                    // Update the row
                    try (PreparedStatement updateStatement = h2connection.prepareStatement(
                            "UPDATE RECORDLINKS SET "
                                    + "confidence=?, "       // 1
                                    + "record2id=?, "        // 2
                                    + "record2datasetid=?, " // 3
                                    + "record2entityid=?, "  // 4
                                    + "updated=(select next value for RECORDLINKS_UPDATED), "
                                    + "deleted=FALSE "
                                    + "WHERE record1id=?" // 5
                    )) {
                        updateStatement.setDouble(1, confidence);
                        updateStatement.setNString(2, recordFromGroup2Id);
                        updateStatement.setNString(3, recordFromGroup2.getValue(DATASET_ID_PROPERTY_NAME));
                        updateStatement.setNString(4, recordFromGroup2.getValue(ORIGINAL_ENTITY_ID_PROPERTY_NAME));
                        updateStatement.setNString(5, recordFromGroup1Id);
                        updateStatement.executeUpdate();
                    }

                } else {
                    // We must insert a new primary row.
                    try (PreparedStatement insertStatement = h2connection.prepareStatement(
                            "INSERT INTO RECORDLINKS("
                                    + "record1id,"         // 1
                                    + "record2id, "        // 2
                                    + "record1datasetid, " // 3
                                    + "record2datasetid, " // 4
                                    + "record1entityid, "  // 5
                                    + "record2entityid, "  // 6
                                    + "confidence, "       // 7
                                    + "updated "
                                    + ") VALUES(?, ?, ?, ?, ?, ?, ?, (select next value for RECORDLINKS_UPDATED))")) {
                        insertStatement.setNString(1, recordFromGroup1Id);
                        insertStatement.setNString(2, recordFromGroup2Id);
                        insertStatement.setNString(3, recordFromGroup1.getValue(DATASET_ID_PROPERTY_NAME));
                        insertStatement.setNString(4, recordFromGroup2.getValue(DATASET_ID_PROPERTY_NAME));
                        insertStatement.setNString(5, recordFromGroup1.getValue(ORIGINAL_ENTITY_ID_PROPERTY_NAME));
                        insertStatement.setNString(6, recordFromGroup2.getValue(ORIGINAL_ENTITY_ID_PROPERTY_NAME));
                        insertStatement.setDouble(7, confidence);
                        insertStatement.executeUpdate();
                    }
                }

                if (existingSecondaryRecord1Id != null) {
                    // We have a secondary row, which must be marked as deleted. That existing record1 must be
                    // marked for rematching.
                    try (PreparedStatement updateStatement = h2connection.prepareStatement(
                            "UPDATE RECORDLINKS SET "
                                    + "updated=(select next value for RECORDLINKS_UPDATED), "
                                    + "deleted=TRUE "
                                    + "WHERE record1id=?" // 1
                    )) {
                        updateStatement.setNString(1, existingSecondaryRecord1Id);
                        updateStatement.executeUpdate();
                    }

                    try (PreparedStatement markAsRematchStatement = h2connection.prepareStatement(
                            "MERGE INTO RECORDLINKS_THAT_NEEDS_REMATCH KEY(recordId) VALUES(?)"
                    )) {
                        markAsRematchStatement.setNString(1, existingSecondaryRecord1Id);
                        markAsRematchStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean confidenceIsMeasurablyLarger(double confidence, double existingPrimaryConfidence) {
        if (confidence > existingPrimaryConfidence) {
            double diff = Math.abs(confidence - existingPrimaryConfidence);
            if (diff > 0.001) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void batchReady(int size) {
        logger.info("batchReady(size={})", size);
    }

    @Override
    public void batchDone() {
        logger.debug("batchDone()");
    }

    @Override
    public void matches(Record r1, Record r2, double confidence) {
        logger.debug("matches(r1={}, r2={}, confidence={})", r1, r2, confidence);
        _matches(r1, r2, confidence);
    }

    @Override
    public void matchesPerhaps(Record r1, Record r2, double confidence) {
        logger.debug("matchesPerhaps(r1={}, r2={}, confidence={})", r1, r2, confidence);
        _matches(r1, r2, confidence);
    }

    @Override
    public void noMatchFor(Record record) {
        logger.debug("noMatchFor(record={})", record);
    }

    @Override
    public void startProcessing() {
        logger.info("startProcessing()");
    }

    @Override
    public void endProcessing() {
        logger.info("endProcessing()");
    }

    public void commitCurrentDatabaseTransaction() throws SQLException {
        h2connection.commit();
    }

    public void rollbackCurrentDatabaseTransaction() throws SQLException {
        h2connection.rollback();
    }

    public List<String> getRecordIdsThatNeedReprocessing() throws SQLException {
        List<String> recordIdsThatNeedReprocessing = new LinkedList<>();
        try (PreparedStatement selectStatement = h2connection.prepareStatement("SELECT * FROM RECORDLINKS_THAT_NEEDS_REMATCH")) {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                while (resultSet.next()) {
                    recordIdsThatNeedReprocessing.add(resultSet.getString("recordId"));
                }
            }
        }
        return recordIdsThatNeedReprocessing;
    }

    public void clearRecordIdsThatNeedReprocessing() throws SQLException {
        try (PreparedStatement deleteStatement = h2connection.prepareStatement("DELETE FROM RECORDLINKS_THAT_NEEDS_REMATCH")) {
            deleteStatement.executeUpdate();
        }
    }

    public void streamRecordLinksSince(String since, Writer writer) throws SQLException, IOException {
        PreparedStatement selectStatement;
        if (since != null) {
            int sinceAsInt = Integer.parseInt(since);
            selectStatement = h2connection.prepareStatement("SELECT * FROM RECORDLINKS WHERE UPDATED > ? ORDER BY UPDATED ASC");
            selectStatement.setInt(1, sinceAsInt);
        } else {
            selectStatement = h2connection.prepareStatement("SELECT * FROM RECORDLINKS ORDER BY UPDATED ASC");
        }

        try {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                boolean isFirstEntity = true;
                writer.append("[");
                while (resultSet.next()) {
                    if (isFirstEntity) {
                        isFirstEntity = false;
                    } else {
                        writer.append(",\n");
                    }

                    JsonObject entity = new JsonObject();
                    entity.addProperty("_id", resultSet.getString("record1id"));
                    entity.addProperty("_updated", resultSet.getString("updated"));
                    entity.addProperty("_deleted", resultSet.getBoolean("deleted"));
                    entity.addProperty("entity1", resultSet.getString("record1entityid"));
                    entity.addProperty("entity2", resultSet.getString("record2entityid"));
                    entity.addProperty("dataset1", resultSet.getString("record1datasetid"));
                    entity.addProperty("dataset2", resultSet.getString("record2datasetid"));
                    entity.addProperty("confidence", resultSet.getDouble("confidence"));

                    String entityLinkAsString = entity.toString();
                    writer.append(entityLinkAsString);
                }
                writer.append("]");
            }
        } finally {
            selectStatement.close();
        }
    }

    /*
     * Removes a deleted entry from the database
     */
    Set<String> delete(Record record) throws SQLException {
        String recordId = record.getValue("ID");

        try (PreparedStatement deleteStatement = h2connection.prepareStatement(
                "DELETE FROM RECORDLINKS_THAT_NEEDS_REMATCH WHERE recordId=?")) {
            deleteStatement.setString(1, recordId);
            deleteStatement.executeUpdate();
        }

        Set<String> recordIdsThatNeedRematch = new HashSet<>();
        try (PreparedStatement selectStatement = h2connection.prepareStatement(
                "SELECT * FROM RECORDLINKS WHERE record1id=? OR record2id=?")) {
            selectStatement.setString(1, recordId);
            selectStatement.setString(2, recordId);
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                while (resultSet.next()) {
                    String record1Id = resultSet.getNString("record1Id");
                    String record2Id = resultSet.getNString("record2Id");
                    if (record1Id.equals(recordId)) {
                        recordIdsThatNeedRematch.add(record2Id);
                    } else {
                        recordIdsThatNeedRematch.add(record1Id);
                    }
                }
            }
        }

        try (PreparedStatement markAsDeletedStatement = h2connection.prepareStatement(
                "UPDATE RECORDLINKS SET "
                        + "deleted=TRUE, "
                        + "updated=(select next value for RECORDLINKS_UPDATED) "
                        + "WHERE record1id=? OR record2id=?")) {
            markAsDeletedStatement.setString(1, recordId);
            markAsDeletedStatement.setString(2, recordId);
            markAsDeletedStatement.executeUpdate();
        }

        return recordIdsThatNeedRematch;
    }
}