package com.taxonic.carml.engine.reactivedev.join;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import reactor.core.publisher.Flux;

public class MapDbChildSideJoinFileStore<T1 extends Serializable, T2 extends Serializable>
    implements ChildSideJoinStore<T1, T2> {

  private final String name;

  private final File dbFile;

  private final int commitBatchSize;

  private final AtomicBoolean dbFileUsed;

  private final BlockingQueue<ChildSideJoin<T1, T2>> tmpStorage;

  static <T1 extends Serializable, T2 extends Serializable> MapDbChildSideJoinFileStore<T1, T2> of(String name,
      File dbFile, int commitBatchSize) {
    return new MapDbChildSideJoinFileStore<>(name, dbFile, commitBatchSize);
  }

  private MapDbChildSideJoinFileStore(String name, File dbFile, int commitBatchSize) {
    this.name = name;
    this.dbFile = dbFile;
    this.commitBatchSize = commitBatchSize;
    this.dbFileUsed = new AtomicBoolean();
    this.tmpStorage = new LinkedBlockingDeque<>();
  }

  @Override
  public void addAll(Set<ChildSideJoin<T1, T2>> childSideJoins) {
    tmpStorage.addAll(childSideJoins);
    if (tmpStorage.size() >= commitBatchSize) {
      commit();
    }
  }

  @Override
  public Flux<ChildSideJoin<T1, T2>> clearingFlux() {
    return Flux.using(this::getTransaction, this::createFlux, this::cleanup);
  }

  @SuppressWarnings("unchecked")
  private Set<ChildSideJoin<T1, T2>> openStore(DB transaction) {
    return (Set<ChildSideJoin<T1, T2>>) transaction.hashSet(name, Serializer.JAVA)
        .createOrOpen();
  }

  private void commit() {
    DB transaction = getTransaction();
    commitRunningTransaction(transaction);
  }

  private DB getTransaction() {
    return DBMaker.fileDB(dbFile)
        .closeOnJvmShutdown()
        .make();
  }

  private void commitRunningTransaction(DB transaction) {
    Set<ChildSideJoin<T1, T2>> store = openStore(transaction);
    tmpStorage.drainTo(store);
    transaction.commit();
    transaction.close();
    dbFileUsed.compareAndSet(false, true);
  }

  private Flux<ChildSideJoin<T1, T2>> createFlux(DB transaction) {
    if (dbFileUsed.get()) {
      return Flux.merge(Flux.fromIterable(tmpStorage), Flux.fromIterable(openStore(transaction)));
    } else {
      return Flux.fromIterable(tmpStorage);
    }
  }

  private void cleanup(DB transaction) {
    if (dbFileUsed.get()) {
      transaction.close();
      try {
        Files.delete(dbFile.toPath());
      } catch (IOException ioException) {
        throw new MapDbStoreException(String.format("Error deleting temp file %s", dbFile), ioException);
      }
    }

    tmpStorage.clear();
  }
}
