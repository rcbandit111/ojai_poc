package com.ext.dependency.jar;

import com.google.common.collect.Lists;
import com.test.processor.UsersRepository;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

// This Java file is located in external jar file
public class MainJsonRepository {
    final Connection connection = DriverManager.getConnection("ojai:mapr:");
    protected MainJsonRepository jsonStore = this.getInstant();
    private DocumentStore documentStore = this.connection.getStore("/demo_table");
    private String table_path = null;


    protected MainJsonRepository(String table_path) {
        this.table_path = table_path;
    }

    protected MainJsonRepository(String path, boolean useBufferedWrite) {

    }

    private MainJsonRepository(DocumentStore documentStore, String table_path) {
        this.documentStore = documentStore;
        this.table_path = table_path;
    }

    public static MainJsonRepository getInstant(){
        return new UsersRepository();
    }

    public static MainJsonRepository getInstant(String connectionUrl, String tablePath){
        Connection connection = DriverManager.getConnection(connectionUrl);
        DocumentStore store = connection.getStore(tablePath);
        return new MainJsonRepository(store, tablePath);
    }

    public Document createDocument() {
        return connection.newDocument();
    }

    public DocumentStream getAllDocumentsAsStream() {
        return documentStore.find(connection.newQuery().build());
    }

    public List<Document> getAllDocumentsAsStream(List<String> personIds) {
        return personIds.stream().map(documentStore::findById).collect(Collectors.toList());
    }

    protected Document createDocument(String payload) {
        if (StringUtils.isBlank(payload)){
            return null;
        }
        return connection.newDocument(payload);
    }

    public Document createDocument(Object object) {
        if (Objects.isNull(object)){
            return null;
        }
        return connection.newDocument(object);
    }

    protected List<Document> getAllDocuments(String documentId) {
        if (StringUtils.isBlank(documentId)){
            return Lists.newArrayList();
        }
        return Lists.newArrayList(documentStore.find(documentId));
    }

    public List<Document> getAllDocuments(String[]  documentArrayIds) {
        return this.getAllDocuments(Lists.newArrayList(documentArrayIds));
    }

    public List<Document> getAllDocuments(List<String> documentIds) {
        return documentIds.stream().map(documentStore::findById).collect(Collectors.toList());
    }

    public List<Document> getAllDocuments(Map<String, Object> values) {
        return values.keySet().stream().map(documentStore::findById).collect(Collectors.toList());
    }

    public List<Document> query(QueryCondition queryCondition, String documentId) {
        if (StringUtils.isBlank(documentId)||Objects.isNull(queryCondition)){
            return Lists.newArrayList();
        }
        return Lists.newArrayList(documentStore.find(queryCondition, documentId).iterator());
    }

    public DocumentStream queryAsStream(QueryCondition queryCondition,String[]  documentArrayIds){
        if (ArrayUtils.isEmpty(documentArrayIds)||Objects.isNull(queryCondition)){
            return null;
        }
        return documentStore.find(queryCondition, documentArrayIds);
    }
}
