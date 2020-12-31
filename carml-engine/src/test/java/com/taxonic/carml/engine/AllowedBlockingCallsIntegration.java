package com.taxonic.carml.engine;

import com.google.auto.service.AutoService;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@AutoService(BlockHoundIntegration.class)
public class AllowedBlockingCallsIntegration implements BlockHoundIntegration {

  @Override
  public void applyTo(BlockHound.Builder builder) {
    allowBlockingCallsInside("com.taxonic.carml.engine.rdf.ModelResult", "from", builder);
    allowBlockingCallsInside("com.taxonic.carml.engine.rdf.RdfTermGeneratorFactory", "mapExecution", builder);
  }

  private void allowBlockingCallsInside(String className, String methodName, BlockHound.Builder builder) {
    try {
      Class.forName(className);
      builder.allowBlockingCallsInside(className, methodName);
    } catch (ClassNotFoundException classNotFoundException) {
      throw new IllegalStateException(
          String.format("BlockHound integration incorrectly configured. Could not find class %s", className),
          classNotFoundException);
    }
  }
}
