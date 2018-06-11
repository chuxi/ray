package org.ray.core;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayApi;
import org.ray.api.RayList;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;
import org.ray.api.WaitResult;
import org.ray.api.internal.RayFunc;
import org.ray.api.funcs.RayFunc_1_1;
import org.ray.api.funcs.RayFunc_3_1;
import org.ray.api.funcs.RayFunc_4_1;
import org.ray.api.internal.Callable;
import org.ray.core.model.RayParameters;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.LocalSchedulerProxy;
import org.ray.spi.ObjectStoreProxy;
import org.ray.spi.ObjectStoreProxy.GetStatus;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.util.config.ConfigReader;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.DynamicLog;
import org.ray.util.logger.DynamicLogManager;
import org.ray.util.logger.RayLog;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class RayRuntime implements RayApi {

  public static ConfigReader configReader;
  protected static RayRuntime ins = null;
  protected static RayParameters params = null;
  private static boolean fromRayInit = false;
  protected Worker worker;
  protected LocalSchedulerProxy localSchedulerProxy;
  protected ObjectStoreProxy objectStoreProxy;
  protected LocalFunctionManager functions;
  protected RemoteFunctionManager remoteFunctionManager;
  protected PathConfig pathConfig;

  // app level Ray.init()
  // make it private so there is no direct usage but only from Ray.init
  private static RayRuntime init() {
    if (ins == null) {
      try {
        fromRayInit = true;
        RayRuntime.init(null, null);
        fromRayInit = false;
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Ray.init failed", e);
      }
    }
    return ins;
  }

  // engine level RayRuntime.init(xx, xx)
  // updateConfigStr is sth like section1.k1=v1;section2.k2=v2
  public static RayRuntime init(String configPath, String updateConfigStr) throws Exception {
    if (ins == null) {
      if (configPath == null) {
        configPath = System.getenv("RAY_CONFIG");
        if (configPath == null) {
          configPath = System.getProperty("ray.config");
        }
        if (configPath == null) {
          throw new Exception(
              "Please set config file path in env RAY_CONFIG or property ray.config");
        }
      }
      configReader = new ConfigReader(configPath, updateConfigStr);

      String loglevel = configReader.getStringValue("ray.java", "log_level", "debug",
          "set the log output level(debug, info, warn, error)");
      DynamicLog.setLogLevel(loglevel);
      RayRuntime.params = new RayParameters(configReader);
      DynamicLogManager.init(params.max_java_log_file_num, params.max_java_log_file_size);
      ins = instantiate(params);
      assert (ins != null);

      if (!fromRayInit) {
        Ray.init(); // assign Ray._impl
      }
    }
    return ins;
  }

  // init with command line args
  // --config=ray.config.ini --overwrite=updateConfigStr
  public static RayRuntime init(String[] args) throws Exception {
    String config = null;
    String updateConfig = null;
    for (String arg : args) {
      if (arg.startsWith("--config=")) {
        config = arg.substring("--config=".length());
      } else if (arg.startsWith("--overwrite=")) {
        updateConfig = arg.substring("--overwrite=".length());
      } else {
        throw new RuntimeException("Input argument " + arg
            + " is not recognized, please use --overwrite to merge it into config file");
      }
    }
    return init(config, updateConfig);
  }

  protected void init(
      LocalSchedulerLink slink,
      ObjectStoreLink plink,
      RemoteFunctionManager remoteLoader,
      PathConfig pathManager
  ) {
    UniqueIdHelper.setThreadRandomSeed(UniqueIdHelper.getUniqueness(params.driver_id));
    remoteFunctionManager = remoteLoader;
    pathConfig = pathManager;

    functions = new LocalFunctionManager(remoteLoader);
    localSchedulerProxy = new LocalSchedulerProxy(slink);
    objectStoreProxy = new ObjectStoreProxy(plink);
    worker = new Worker(localSchedulerProxy, functions);
  }

  private static RayRuntime instantiate(RayParameters params) {
    String className = params.run_mode.isNativeRuntime()
        ? "org.ray.core.impl.RayNativeRuntime" : "org.ray.core.impl.RayDevRuntime";

    RayRuntime runtime;
    try {
      Class<?> cls = Class.forName(className);
      if (cls.getConstructors().length > 0) {
        throw new Error("The RayRuntime final class should not have any public constructor.");
      }
      Constructor<?> cons = cls.getDeclaredConstructor();
      cons.setAccessible(true);
      runtime = (RayRuntime) cons.newInstance();
      cons.setAccessible(false);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException
        | NoSuchMethodException e) {
      RayLog.core
          .error("Load class " + className + " failed for run-mode " + params.run_mode.toString(),
              e);
      throw new Error("RayRuntime not registered for run-mode " + params.run_mode.toString());
    }

    RayLog.core
        .info("Start " + runtime.getClass().getName() + " with " + params.run_mode.toString());
    try {
      runtime.start(params);
    } catch (Exception e) {
      System.err.println("RayRuntime start failed:" + e.getMessage()); //in case of logger not ready
      e.printStackTrace(); //in case of logger not ready
      RayLog.core.error("RayRuntime start failed", e);
      System.exit(-1);
    }

    return runtime;
  }

  /**
   * start runtime.
   */
  public abstract void start(RayParameters params) throws Exception;

  public static RayRuntime getInstance() {
    return ins;
  }

  public static RayParameters getParams() {
    return params;
  }

  public abstract void cleanUp();

  public <T> void putRaw(UniqueID taskId, UniqueID objectId, T obj) {
    putRaw(taskId, objectId, obj, null);
  }

  /***********
   * RayApi methods.
   ***********/

  public <T, TMT> void putRaw(UniqueID taskId, UniqueID objectId, T obj, TMT metadata) {
    RayLog.core.info("Task " + taskId.toString() + " Object " + objectId.toString() + " put");
    localSchedulerProxy.markTaskPutDependency(taskId, objectId);
    objectStoreProxy.put(objectId, obj, metadata);
  }

  public <T> void putRaw(UniqueID objectId, T obj) {
    UniqueID taskId = getCurrentTaskId();
    putRaw(taskId, objectId, obj, null);
  }

  public <T> void putRaw(T obj) {
    UniqueID taskId = getCurrentTaskId();
    UniqueID objectId = getCurrentTaskNextPutId();
    putRaw(taskId, objectId, obj, null);
  }

  /**
   * get the task identity of the currently running task, UniqueID.Nil if not inside any
   */
  public UniqueID getCurrentTaskId() {
    return worker.getCurrentTaskId();
  }

  /**
   * get the to-be-returned objects identities of the currently running task, empty array if not
   * inside any.
   */
  public UniqueID getCurrentTaskNextPutId() {
    return worker.getCurrentTaskNextPutId();
  }

  @Override
  public <T> RayObject<T> put(T obj) {
    return put(obj, null);
  }

  @Override
  public <T, TMT> RayObject<T> put(T obj, TMT metadata) {
    UniqueID taskId = getCurrentTaskId();
    UniqueID objectId = getCurrentTaskNextPutId();
    putRaw(taskId, objectId, obj, metadata);
    return new RayObject<>(objectId);
  }

  @Override
  public <T> T get(UniqueID objectId) throws TaskExecutionException {
    return doGet(objectId, false);
  }

  @Override
  public <T> List<T> get(List<UniqueID> objectIds) throws TaskExecutionException {
    return doGet(objectIds, false);
  }

  @Override
  public <T> T getMeta(UniqueID objectId) throws TaskExecutionException {
    return doGet(objectId, true);
  }

  @Override
  public <T> List<T> getMeta(List<UniqueID> objectIds) throws TaskExecutionException {
    return doGet(objectIds, true);
  }

  @Override
  public <T> WaitResult<T> wait(RayList<T> waitfor, int numReturns, int timeout) {
    return objectStoreProxy.wait(waitfor, numReturns, timeout);
  }

  @Override
  public RayObjects call(UniqueID taskId, Class<?> funcCls, RayFunc lambda, int returnCount,
      Object... args) {
    return worker.rpc(taskId, funcCls, lambda, returnCount, args);
  }

  @Override
  public <R, RIDT> RayMap<RIDT, R> callWithReturnLabels(UniqueID taskId, Class<?> funcCls,
      RayFunc lambda, Collection<RIDT> returnids, Object... args) {
    return worker.rpcWithReturnLabels(taskId, funcCls, lambda, returnids, args);
  }

  @Override
  public <R> RayList<R> callWithReturnIndices(UniqueID taskId, Class<?> funcCls,
      RayFunc lambda, Integer returnCount, Object... args) {
    return worker.rpcWithReturnIndices(taskId, funcCls, lambda, returnCount, args);
  }

  private <T> List<T> doGet(List<UniqueID> objectIds, boolean isMetadata)
      throws TaskExecutionException {
    boolean wasBlocked = false;
    UniqueID taskId = getCurrentTaskId();
    try {
      int numObjectIds = objectIds.size();

      // Do an initial fetch for remote objects.
      dividedFetch(objectIds);

      // Get the objects. We initially try to get the objects immediately.
      List<Pair<T, GetStatus>> ret = objectStoreProxy
          .get(objectIds, params.default_first_check_timeout_ms, isMetadata);
      assert ret.size() == numObjectIds;

      // mapping the object IDs that we haven't gotten yet to their original index in objectIds
      Map<UniqueID, Integer> unreadys = new HashMap<>();
      for (int i = 0; i < numObjectIds; i++) {
        if (ret.get(i).getRight() != GetStatus.SUCCESS) {
          unreadys.put(objectIds.get(i), i);
        }
      }
      wasBlocked = (unreadys.size() > 0);

      // Try reconstructing any objects we haven't gotten yet. Try to get them
      // until at least PlasmaLink.GET_TIMEOUT_MS milliseconds passes, then repeat.
      while (unreadys.size() > 0) {
        for (UniqueID id : unreadys.keySet()) {
          localSchedulerProxy.reconstructObject(id);
        }

        // Do another fetch for objects that aren't available locally yet, in case
        // they were evicted since the last fetch.
        List<UniqueID> unreadyList = new ArrayList<>(unreadys.keySet());

        dividedFetch(unreadyList);

        List<Pair<T, GetStatus>> results = objectStoreProxy
            .get(unreadyList, params.default_get_check_interval_ms, isMetadata);

        // Remove any entries for objects we received during this iteration so we
        // don't retrieve the same object twice.
        for (int i = 0; i < results.size(); i++) {
          Pair<T, GetStatus> value = results.get(i);
          if (value.getRight() == GetStatus.SUCCESS) {
            UniqueID id = unreadyList.get(i);
            ret.set(unreadys.get(id), value);
            unreadys.remove(id);
          }
        }
      }

      RayLog.core
          .debug("Task " + taskId + " Objects " + Arrays.toString(objectIds.toArray()) + " get");
      List<T> finalRet = new ArrayList<>();
      for (Pair<T, GetStatus> value : ret) {
        finalRet.add(value.getLeft());
      }
      return finalRet;
    } catch (TaskExecutionException e) {
      RayLog.core.error("Task " + taskId + " Objects " + Arrays.toString(objectIds.toArray())
          + " get with Exception", e);
      throw e;
    } finally {
      // If there were objects that we weren't able to get locally, let the local
      // scheduler know that we're now unblocked.
      if (wasBlocked) {
        localSchedulerProxy.notifyUnblocked();
      }
    }

  }

  private <T> T doGet(UniqueID objectId, boolean isMetadata) throws TaskExecutionException {

    boolean wasBlocked = false;
    UniqueID taskId = getCurrentTaskId();
    try {
      // Do an initial fetch.
      objectStoreProxy.fetch(objectId);

      // Get the object. We initially try to get the object immediately.
      Pair<T, GetStatus> ret = objectStoreProxy
          .get(objectId, params.default_first_check_timeout_ms, isMetadata);

      wasBlocked = (ret.getRight() != GetStatus.SUCCESS);

      // Try reconstructing the object. Try to get it until at least PlasmaLink.GET_TIMEOUT_MS
      // milliseconds passes, then repeat.
      while (ret.getRight() != GetStatus.SUCCESS) {
        RayLog.core.warn(
            "Task " + taskId + " Object " + objectId.toString() + " get failed, reconstruct ...");
        localSchedulerProxy.reconstructObject(objectId);

        // Do another fetch
        objectStoreProxy.fetch(objectId);

        ret = objectStoreProxy.get(objectId, params.default_get_check_interval_ms,
            isMetadata);//check the result every 5s, but it will return once available
      }
      RayLog.core.debug(
          "Task " + taskId + " Object " + objectId.toString() + " get" + ", the result " + ret
              .getLeft());
      return ret.getLeft();
    } catch (TaskExecutionException e) {
      RayLog.core
          .error("Task " + taskId + " Object " + objectId.toString() + " get with Exception", e);
      throw e;
    } finally {
      // If the object was not able to get locally, let the local scheduler
      // know that we're now unblocked.
      if (wasBlocked) {
        localSchedulerProxy.notifyUnblocked();
      }
    }

  }

  // We divide the fetch into smaller fetches so as to not block the manager
  // for a prolonged period of time in a single call.
  private void dividedFetch(List<UniqueID> objectIds) {
    int fetchSize = objectStoreProxy.getFetchSize();

    int numObjectIds = objectIds.size();
    for (int i = 0; i < numObjectIds; i += fetchSize) {
      int endIndex = i + fetchSize;
      if (endIndex < numObjectIds) {
        objectStoreProxy.fetch(objectIds.subList(i, endIndex));
      } else {
        objectStoreProxy.fetch(objectIds.subList(i, numObjectIds));
      }
    }
  }

  /**
   * get the object put identity of the currently running task, UniqueID.Nil if not inside any
   */
  public UniqueID[] getCurrentTaskReturnIDs() {
    return worker.getCurrentTaskReturnIDs();
  }

  /***********
   * Internal Methods.
   ***********/

  public void loop() {
    worker.loop();
  }

  /**
   * get actor with given id.
   */
  public abstract Object getLocalActor(UniqueID id);

  public PathConfig getPaths() {
    return pathConfig;
  }

  public RemoteFunctionManager getRemoteFunctionManager() {
    return remoteFunctionManager;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <ContextT, ResultT> RayObject<Boolean> startBatch(
      long batchId,
      RayFunc_1_1<ContextT, Boolean> starter,
      RayFunc_3_1<Long, ContextT, ResultT, Boolean> completionHandler,
      ContextT context) {
    UniqueID taskId = UniqueIdHelper.getBatchRootTaskId(batchId);
    RayObject<Boolean> ret = null;
    if (completionHandler != null) {
      RayObject<ResultT> result = UniqueIdHelper.batchResultObject(batchId);
      UniqueID endTaskId = UniqueIdHelper.getBatchEndTaskId(taskId, batchId);

      if (Ray.isRemoteLambda()) {
        ret = this.worker.rpc(endTaskId, UniqueID.nil, RayFunc_3_1.class, completionHandler, 1,
            new Object[]{batchId, context, result}).getObjs()[0];
      } else {
        ret = this.worker.rpc(endTaskId, () -> completionHandler.apply(null, null, null), 1,
            new Object[]{batchId, context, result}).getObjs()[0];
      }
    }

    if (Ray.isRemoteLambda()) {
      this.call(taskId, RayFunc_1_1.class, starter, 1, context);
    } else {
      this.call(taskId, () -> starter.apply(null), 1, context);
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <ContextT, ResultT, CompletionHostT> RayObject<Boolean> startBatch(
      long batchId,
      RayFunc_1_1<ContextT, Boolean> starter,
      RayActor<CompletionHostT> completionHost,
      RayFunc_4_1<CompletionHostT, Long, ContextT, ResultT, Boolean> completionHandler,
      ContextT context) {
    UniqueID taskId = UniqueIdHelper.getBatchRootTaskId(batchId);
    RayObject<Boolean> ret = null;
    if (completionHandler != null) {
      RayObject<ResultT> result = UniqueIdHelper.batchResultObject(batchId);
      UniqueID endTaskId = UniqueIdHelper.getBatchEndTaskId(taskId, batchId);

      if (Ray.isRemoteLambda()) {
        ret = this.worker.rpc(endTaskId, UniqueID.nil, RayFunc_4_1.class, completionHandler, 1,
            new Object[]{completionHost, batchId, context, result}).getObjs()[0];
      } else {
        ret = this.worker.rpc(endTaskId, () -> completionHandler.apply(null, null, null, null), 1,
            new Object[]{completionHost, batchId, context, result}).getObjs()[0];
      }
    }

    if (Ray.isRemoteLambda()) {
      this.call(taskId, RayFunc_1_1.class, starter, 1, context);
    } else {
      this.call(taskId, () -> starter.apply(null), 1, context);
    }
    return ret;
  }

  @Override
  public <TResult> void endBatch(TResult r) {
    long batchId = UniqueIdHelper.getBatchId(this.getCurrentTaskId());
    Ray.getRappLogger().debug("end batch with id " + batchId);
    this.putRaw(UniqueIdHelper.batchResultObject(batchId).getId(), r);
  }
}
