package com.github.mongobee.dao;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.Test;

import com.github.fakemongo.Fongo;
import com.github.mongobee.exception.MongobeeConfigurationException;
import com.mongodb.FongoMongoCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * @author lstolowski
 * @since 10.12.14
 */
@RunWith(Theories.class)
public class ChangeEntryDaoTest {
  private static final String TEST_SERVER = "testServer";
  private static final String DB_NAME = "mongobeetest";
  private static final String CHANGELOG_COLLECTION_NAME = "dbchangelog";
  private static final String LOCK_COLLECTION_NAME = "mongobeelock";

  @DataPoint
  public static boolean UNIQUE_INDEXES_SUPPORTED = true;
  @DataPoint
  public static boolean UNIQUE_INDEXES_UNSUPPORTED = false;

  @Theory
  public void shouldCreateChangeIdAuthorIndexIfNotFound(boolean isUniqueIndexesSupported) throws MongobeeConfigurationException {

    // given
    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);

    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);

    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
    when(indexDaoMock.findRequiredChangeAndAuthorIndex(db)).thenReturn(null);
    dao.setIndexDao(indexDaoMock);

    // when
    dao.connectMongoDb(mongoClient, DB_NAME);

    //then
    verify(indexDaoMock, times(1)).createRequiredIndex(any(FongoMongoCollection.class));
    // and not
    verify(indexDaoMock, times(0)).dropIndex(any(FongoMongoCollection.class), any(Document.class));
  }

  @Theory
  public void shouldNotCreateChangeIdAuthorIndexIfFound(boolean isUniqueIndexesSupported) throws MongobeeConfigurationException {

    // given
    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
    when(indexDaoMock.findRequiredChangeAndAuthorIndex(db)).thenReturn(new Document());
    when(indexDaoMock.isUnique(any(Document.class))).thenReturn(true);
    dao.setIndexDao(indexDaoMock);

    // when
    dao.connectMongoDb(mongoClient, DB_NAME);

    //then
    verify(indexDaoMock, times(0)).createRequiredIndex(db.getCollection(CHANGELOG_COLLECTION_NAME));
    // and not
    verify(indexDaoMock, times(0)).dropIndex(db.getCollection(CHANGELOG_COLLECTION_NAME), new Document());
  }

  @Theory
  public void checkRecreateChangeIdAuthorIndexIfFoundNotUnique(boolean isUniqueIndexesSupported) throws MongobeeConfigurationException {

    // given
    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
    when(indexDaoMock.findRequiredChangeAndAuthorIndex(db)).thenReturn(new Document());
    when(indexDaoMock.isUnique(any(Document.class))).thenReturn(false);
    dao.setIndexDao(indexDaoMock);

    // when
    dao.connectMongoDb(mongoClient, DB_NAME);

    //then
    int shouldDropAndCreateTimes = isUniqueIndexesSupported ? 1 : 0;
    verify(indexDaoMock, times(shouldDropAndCreateTimes)).dropIndex(any(FongoMongoCollection.class), any(Document.class));
    // and
    verify(indexDaoMock, times(shouldDropAndCreateTimes)).createRequiredIndex(any(FongoMongoCollection.class));
  }

  @Theory
  public void shouldInitiateLock(boolean isUniqueIndexesSupported) throws MongobeeConfigurationException {

    // given
    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    ChangeEntryIndexDao indexDaoMock = mock(ChangeEntryIndexDao.class);
    dao.setIndexDao(indexDaoMock);

    LockDao lockDao = mock(LockDao.class);
    dao.setLockDao(lockDao);

    // when
    dao.connectMongoDb(mongoClient, DB_NAME);

    // then
    verify(lockDao).intitializeLock(db);

  }

  @Theory
  public void shouldGetLockWhenLockDaoGetsLock(boolean isUniqueIndexesSupported) throws Exception {

    // given
    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);

    LockDao lockDao = mock(LockDao.class);
    when(lockDao.acquireLock(any(MongoDatabase.class))).thenReturn(true);
    dao.setLockDao(lockDao);

    dao.connectMongoDb(mongoClient, DB_NAME);

    // when
    boolean hasLock = dao.acquireProcessLock();

    // then
    assertTrue(hasLock);
  }

  @Theory
  public void shouldReleaseLockFromLockDao(boolean isUniqueIndexesSupported) throws Exception {

    // given
    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);

    LockDao lockDao = mock(LockDao.class);
    dao.setLockDao(lockDao);

    dao.connectMongoDb(mongoClient, DB_NAME);

    // when
    dao.releaseProcessLock();

    // then
    verify(lockDao).releaseLock(any(MongoDatabase.class));
  }

  @Theory
  public void shouldCheckLockHeldFromFromLockDao(boolean isUniqueIndexesSupported) throws Exception {

    // given
    MongoClient mongoClient = mock(MongoClient.class);
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    when(mongoClient.getDatabase(anyString())).thenReturn(db);

    ChangeEntryDao dao = new ChangeEntryDao(CHANGELOG_COLLECTION_NAME, LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);

    LockDao lockDao = mock(LockDao.class);
    dao.setLockDao(lockDao);

    dao.connectMongoDb(mongoClient, DB_NAME);

    // when
    when(lockDao.isLockHeld(db)).thenReturn(true);

    boolean lockHeld = dao.isProccessLockHeld();

    // then
    assertTrue(lockHeld);
  }

}
