package com.github.mongobee.dao;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.client.MongoDatabase;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.runner.RunWith;

/**
 * @author colsson11
 * @since 13.01.15
 */
@RunWith(Theories.class)
public class LockDaoTest {

  private static final String TEST_SERVER = "testServer";
  private static final String DB_NAME = "mongobeetest";
  private static final String LOCK_COLLECTION_NAME = "mongobeelock";

  @DataPoint
  public static boolean UNIQUE_INDEXES_SUPPORTED = true;
  @DataPoint
  public static boolean UNIQUE_INDEXES_UNSUPPORTED = false;

  @Theory
  public void shouldGetLockWhenNotPreviouslyHeld(boolean isUniqueIndexesSupported) throws Exception {

    // given
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    dao.intitializeLock(db);

    // when
    boolean hasLock = dao.acquireLock(db);

    // then
    assertTrue(hasLock);

  }

  @Theory
  public void shouldNotGetLockWhenPreviouslyHeld(boolean isUniqueIndexesSupported) throws Exception {

    // given
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    dao.intitializeLock(db);

    // when
    dao.acquireLock(db);
    boolean hasLock = dao.acquireLock(db);
    // then
    assertFalse(hasLock);

  }

  @Theory
  public void shouldGetLockWhenPreviouslyHeldAndReleased(boolean isUniqueIndexesSupported) throws Exception {

    // given
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    dao.intitializeLock(db);

    // when
    dao.acquireLock(db);
    dao.releaseLock(db);
    boolean hasLock = dao.acquireLock(db);
    // then
    assertTrue(hasLock);

  }

  @Theory
  public void releaseLockShouldBeIdempotent(boolean isUniqueIndexesSupported) {
    // given
    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    
    dao.intitializeLock(db);

    // when
    dao.releaseLock(db);
    dao.releaseLock(db);
    boolean hasLock = dao.acquireLock(db);
    // then
    assertTrue(hasLock);

  }

  @Theory
  public void whenLockNotHeldCheckReturnsFalse(boolean isUniqueIndexesSupported) {

    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    dao.intitializeLock(db);

    assertFalse(dao.isLockHeld(db));

  }

  @Theory
  public void whenLockHeldCheckReturnsTrue(boolean isUniqueIndexesSupported) {

    MongoDatabase db = new Fongo(TEST_SERVER).getDatabase(DB_NAME);
    LockDao dao = new LockDao(LOCK_COLLECTION_NAME);
    dao.setIsUniqueIndexesSupported(isUniqueIndexesSupported);
    dao.intitializeLock(db);

    dao.acquireLock(db);

    assertTrue(dao.isLockHeld(db));

  }

}
