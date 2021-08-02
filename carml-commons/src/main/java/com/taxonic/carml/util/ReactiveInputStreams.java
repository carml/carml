package com.taxonic.carml.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Slf4j
public final class ReactiveInputStreams {

  private static final int DATA_BUFFER_SIZE = 4096;

  private ReactiveInputStreams() {}

  public static Flux<DataBuffer> fluxInputStream(@NonNull InputStream inputStream) {
    DataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();
    return DataBufferUtils.readInputStream(() -> inputStream, dataBufferFactory, DATA_BUFFER_SIZE)
        .onErrorMap(error -> new ReactiveInputstreamsException(
            "Exception occurred while creating Flux form input stream.", error));
  }

  public static InputStream inputStreamFrom(Flux<DataBuffer> dataBufferFlux) throws IOException {
    var osPipe = new PipedOutputStream();
    var isPipe = new PipedInputStream(osPipe);

    DataBufferUtils.write(dataBufferFlux, osPipe)
        .subscribeOn(Schedulers.boundedElastic())
        .doFinally(onFinally -> {
          try {
            osPipe.close();
          } catch (IOException triggerWarning) {
            LOG.warn("An exception occurred while closing a PipedOutputStream:{}{}", System.lineSeparator(),
                triggerWarning);
          }
        })
        .onErrorMap(error -> new ReactiveInputstreamsException(
            "Exception occurred while creating input stream form Flux.", error))
        .subscribe(DataBufferUtils.releaseConsumer());
    return isPipe;
  }

}
