package com.test.processor;

import com.ext.dependency.jar.MainJsonRepository;
import com.mapr.db.MapRDB;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;
import org.ojai.types.OTimestamp;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Repository
public class UsersRepository extends MainJsonRepository {

    private static final String TABLE_PATH = "groupData";

    private final String[] fields = { "_id" };

    public UsersRepository(String path, boolean useBufferedWrite) {
        super(path, useBufferedWrite);
    }

    public UsersRepository(String path) {
        super(path);
    }

    public UsersRepository() {
        super(TABLE_PATH);
    }

    public List<String> getRegisteredUsersByDate() {
        List<Document> documents = getAllDocuments(fields);
        return documents.stream().filter(Objects::nonNull)
                .map(Document::getIdString)
                .collect(Collectors.toList());
    }

    public List<Document> getRegisteredUsers(List<String> users) {
        QueryCondition condition = MapRDB.newCondition().and().in("documentId", users).close()
                .build();

        return queryDocuments(condition, "documentId");
    }

    public DocumentStream getUsers() {
        QueryCondition queryCondition = MapRDB.newCondition().and()
                .is("username", QueryCondition.Op.EQUAL, new OTimestamp(null))
                .is("year", QueryCondition.Op.EQUAL, new OTimestamp(null))
                .close().build();

        String[] fields = new String[0];
        return queryDocumentsList(queryCondition, fields);
    }

}
