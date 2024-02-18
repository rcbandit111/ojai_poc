package com.ext.dependency.jar;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.*;

import java.util.*;
import java.util.Map;

// This Java file is located in external jar file
public class MainJsonRepository {
    final Connection connection = DriverManager.getConnection("ojai:mapr:");
    protected DocumentStore jsonStore;
    private String table_path;

    protected MainJsonRepository(String table_path) {
        this.jsonStore = this.connection.getStore("/demo_table");
        this.table_path = table_path;
    }
    private MainJsonRepository(DocumentStore jsonStore, String table_path) {
        this.jsonStore = jsonStore;
        this.table_path = table_path;
    }

    public static MainJsonRepository createInstance(String connectionUrl, String tablePath) {
        Connection connection = DriverManager.getConnection(connectionUrl);
        DocumentStore store = connection.getStore(tablePath);
        return new MainJsonRepository(store, tablePath);
    }

    public Document createDocument(Document document) {
        try{ jsonStore.insertOrReplace(document);
        }
        catch (Exception e){
            throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");}
        return null;
    }

    public DocumentStream getAllDocumentsAsStream() {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    public List<Document> getAllDocumentsAsStream(List<String> personIds) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    protected Document createDocument(String payload) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    public Document createDocument(Object object) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    public MainJsonRepository(String path, boolean useBufferedWrite) {
        this.jsonStore = this.connection.getStore("/demo_table");
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    public List<Document> getAllDocuments(String documentId) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    public List<Document> getAllDocuments(List<String> documentIds) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    protected List<Document> getAllDocuments(Map<String, Object> values) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    protected List<Document> query(QueryCondition queryCondition) {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
    }

    public List<Document> getAllDocuments(String[] ids) {
        List<Document> documents = new ArrayList<>();

        for (String id : ids) {
            Document document = jsonStore.findById(id);
            if (document != null) {
                documents.add(document);
            }
        }

        return documents;
    }

    public List<Document> queryDocuments(QueryCondition condition, String fieldName) {

        List<Document> result = new ArrayList<>();
        Document document = jsonStore.findById(fieldName,condition);
        return result;
    }

    public DocumentStream queryDocumentsList(QueryCondition condition,String[] ids ) {


        Query query = connection.newQuery()
                .where(condition.and().in("_id", Arrays.asList(ids)).close())
                .build();

        return jsonStore.find(query);
    }

}
