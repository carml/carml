package com.taxonic.carml.engine.reactivedev.join;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.mapdb.DB;
import org.mapdb.Serializer;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MapDbParentSideJoinConditionStoreProvider<T extends Serializable>
    implements ParentSideJoinConditionStoreProvider<T> {

  private static final String DB_FILE_PARENT_PREFIX = "carml-mapdb-parent-";

  private static final String DB_FILE_SUFFIX = ".db";

  private static final int DB_FILE_COMMIT_SIZE = 10_000;

  private static final Path TMP_DIR = new File(System.getProperty("java.io.tmpdir")).toPath();

  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");

  public enum DbType {
    HEAP_DB, MEMORY_DB, MEMORY_DIRECT_DB, FILE_DB
  }

  private final DbType dbType;

  private final Path fileDbDirectory;

  // TODO, how to customize config?

  public static <T extends Serializable> MapDbParentSideJoinConditionStoreProvider<T> of() {
    return of(DbType.FILE_DB);
  }

  public static <T extends Serializable> MapDbParentSideJoinConditionStoreProvider<T> of(DbType dbType) {
    return of(dbType, null);
  }

  public static <T extends Serializable> MapDbParentSideJoinConditionStoreProvider<T> of(DbType dbType,
      Path fileDbDirectory) {
    return new MapDbParentSideJoinConditionStoreProvider<>(dbType, fileDbDirectory);
  }

  @Override
  public ParentSideJoinConditionStore<T> createParentSideJoinConditionStore(@NonNull String name) {

    throw new UnsupportedOperationException("This class is still work in progress and currently not yet supported");

    // String storeName = createStoreName(name);
    // switch (dbType) {
    // case MEMORY_DB:
    // ConcurrentMap<ParentSideJoinKey, Set<T>> memoryStore = getConcurrentFromMemDbMaker(storeName,
    // DBMaker.memoryDB()
    // .closeOnJvmShutdown()
    // .make());
    // return MapDbParentSideJoinConditionMemStore.of(storeName, memoryStore);
    //
    // case HEAP_DB:
    // ConcurrentMap<ParentSideJoinKey, Set<T>> heapStore = getConcurrentFromMemDbMaker(storeName,
    // DBMaker.heapDB()
    // .closeOnJvmShutdown()
    // .make());
    // return MapDbParentSideJoinConditionMemStore.of(storeName, heapStore);
    //
    // case MEMORY_DIRECT_DB:
    // ConcurrentMap<ParentSideJoinKey, Set<T>> memoryDirectStore =
    // getConcurrentFromMemDbMaker(storeName, DBMaker.memoryDirectDB()
    // .closeOnJvmShutdown()
    // .make());
    // return MapDbParentSideJoinConditionMemStore.of(storeName, memoryDirectStore);
    //
    // case FILE_DB:
    // return MapDbParentSideJoinConditionFileStore.of(storeName, getDbFile(storeName,
    // DB_FILE_PARENT_PREFIX), DB_FILE_COMMIT_SIZE);
    //
    // default:
    // throw new IllegalStateException(String.format("Unexpected DbType value passed: %s", dbType));
    // }
  }

  private String createStoreName(String name) {
    long hash = (long) Integer.MAX_VALUE + (long) String.format("%s-%s", name, dateFormat.format(new Date()))
        .hashCode();
    return String.valueOf(hash);
  }

  @SuppressWarnings("unchecked")
  private ConcurrentMap<ParentSideJoinKey, Set<T>> getConcurrentMapFromMemDbMaker(String name, DB db) {
    return db.<ParentSideJoinKey, Set<T>>hashMap(name, Serializer.JAVA, Serializer.JAVA)
        .create();
  }

  private File getDbFile(String name, String filePrefix) {
    Path directory = fileDbDirectory != null ? fileDbDirectory : TMP_DIR;
    if (!directory.toFile()
        .isDirectory()) {
      throw new MapDbStoreException(String.format("Provided directory path is not a directory: %s", directory));
    }

    return directory.resolve(String.format("%s%s%s", filePrefix, name, DB_FILE_SUFFIX))
        .toFile();
  }
}
