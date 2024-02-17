package com.ext.dependency.jar;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.*;

import java.util.List;
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

    public Document createDocument() {
        throw new UnsupportedOperationException("Not implemented yet !!!!!!!!!!!!!");
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

    protected List<Document> getAllDocuments(String documentId) {
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
}
