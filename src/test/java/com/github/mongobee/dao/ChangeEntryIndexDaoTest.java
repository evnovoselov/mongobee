package com.github.mongobee.dao;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.github.fakemongo.Fongo;
import com.github.mongobee.changeset.ChangeEntry;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * @author lstolowski
 * @since 10.12.14
 */
@RunWith(Theories.class)
public class ChangeEntryIndexDaoTest {
  private static final String TEST_SERVER = "testServer";
  private static final String DB_NAME = "mongobeetest";
  private static final String CHANGEID_AUTHOR_INDEX_NAME = "changeId_1_author_1";
  private static final String CHANGELOG_COLLECTION_NAME = "dbchangelog";

  @DataPoint
  public static boolean UNIQUE_INDEXES_SUPPORTED = true;
  @DataPoint
  public static boolean UNIQUE_INDEXES_UNSUPPORTED = false;

  private ChangeEntryIndexDao dao = new ChangeEntryIndexDao(CHANGELOG_COLLECTION_NAME);

  @Theory
  public void shouldCreateRequiredUniqueIndex(boolean isUniqueIndexesSupported) {
    // given
    MongoClient mongo = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongo.getDatabase(Mockito.anyString())).thenReturn(db);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);

    // when
    dao.createRequiredIndex(db.getCollection(CHANGELOG_COLLECTION_NAME));

    // then
    Document createdIndex = findIndex(db, CHANGEID_AUTHOR_INDEX_NAME);
    assertNotNull(createdIndex);
    assertEquals(dao.isUnique(createdIndex), isUniqueIndexesSupported);
  }

  @Test
  @Ignore("Fongo has not implemented dropIndex for MongoCollection object (issue with mongo driver 3.x)")
  public void shouldDropWrongIndex() {
    // init
    MongoClient mongo = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongo.getDatabase(Mockito.anyString())).thenReturn(db);

    MongoCollection<Document> collection = db.getCollection(CHANGELOG_COLLECTION_NAME);
    collection.createIndex(new Document()
        .append(ChangeEntry.KEY_CHANGEID, 1)
        .append(ChangeEntry.KEY_AUTHOR, 1));
    Document index = new Document("name", CHANGEID_AUTHOR_INDEX_NAME);

    // given
    Document createdIndex = findIndex(db, CHANGEID_AUTHOR_INDEX_NAME);
    assertNotNull(createdIndex);
    assertFalse(dao.isUnique(createdIndex));

    // when
    dao.dropIndex(db.getCollection(CHANGELOG_COLLECTION_NAME), index);

    // then
    assertNull(findIndex(db, CHANGEID_AUTHOR_INDEX_NAME));
  }

  private Document findIndex(MongoDatabase db, String indexName) {

    for (MongoCursor<Document> iterator = db.getCollection(CHANGELOG_COLLECTION_NAME).listIndexes().iterator(); iterator.hasNext(); ) {
      Document index = iterator.next();
      String name = (String) index.get("name");
      if (indexName.equals(name)) {
        return index;
      }
    }
    return null;
  }

}
