package com.taxonic.carml.engine.reactivedev.join;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MapDbChildSideJoinStoreProvider<T1 extends Serializable, T2 extends Serializable>
    implements ChildSideJoinStoreProvider<T1, T2> {

  private static final String DB_FILE_CHILD_PREFIX = "carml-mapdb-child-";

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

  public static <T1 extends Serializable, T2 extends Serializable> MapDbChildSideJoinStoreProvider<T1, T2> of() {
    return of(DbType.FILE_DB);
  }

  public static <T1 extends Serializable, T2 extends Serializable> MapDbChildSideJoinStoreProvider<T1, T2> of(
      DbType dbType) {
    return of(dbType, null);
  }

  public static <T1 extends Serializable, T2 extends Serializable> MapDbChildSideJoinStoreProvider<T1, T2> of(
      DbType dbType, Path fileDbDirectory) {
    return new MapDbChildSideJoinStoreProvider<>(dbType, fileDbDirectory);
  }

  @Override
  public ChildSideJoinStore<T1, T2> createChildSideJoinStore(@NonNull String name) {
    String storeName = createStoreName(name);
    switch (dbType) {
      case MEMORY_DB:
        Set<ChildSideJoin<T1, T2>> memoryStore = getSetFromMemDbMaker(storeName, DBMaker.memoryDB()
            .closeOnJvmShutdown()
            .make());
        return MapDbChildSideJoinMemStore.of(storeName, memoryStore);

      case HEAP_DB:
        Set<ChildSideJoin<T1, T2>> heapStore = getSetFromMemDbMaker(storeName, DBMaker.heapDB()
            .closeOnJvmShutdown()
            .make());
        return MapDbChildSideJoinMemStore.of(storeName, heapStore);

      case MEMORY_DIRECT_DB:
        Set<ChildSideJoin<T1, T2>> memoryDirectStore = getSetFromMemDbMaker(storeName, DBMaker.memoryDirectDB()
            .closeOnJvmShutdown()
            .make());
        return MapDbChildSideJoinMemStore.of(storeName, memoryDirectStore);

      case FILE_DB:
        return MapDbChildSideJoinFileStore.of(storeName, getDbFile(storeName, DB_FILE_CHILD_PREFIX),
            DB_FILE_COMMIT_SIZE);

      default:
        throw new IllegalStateException(String.format("Unexpected DbType value passed: %s", dbType));
    }
  }

  private String createStoreName(String name) {
    long hash = (long) Integer.MAX_VALUE + (long) String.format("%s-%s", name, dateFormat.format(new Date()))
        .hashCode();
    return String.valueOf(hash);
  }

  @SuppressWarnings("unchecked")
  private Set<ChildSideJoin<T1, T2>> getSetFromMemDbMaker(String name, DB db) {
    return (Set<ChildSideJoin<T1, T2>>) db.hashSet(name, Serializer.JAVA)
        .createOrOpen();
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
