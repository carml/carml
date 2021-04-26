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

  private ReactiveInputStreams(){}

  public static Flux<DataBuffer> fluxInputStream(@NonNull InputStream inputStream) {
    DataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();
    return DataBufferUtils.readInputStream(() -> inputStream, dataBufferFactory, 4096);
  }

  public static InputStream inputStreamFrom(Flux<DataBuffer> dataBufferFlux) throws IOException {
    PipedOutputStream osPipe = new PipedOutputStream();
    PipedInputStream isPipe = new PipedInputStream(osPipe);

    DataBufferUtils.write(dataBufferFlux, osPipe)
        .subscribeOn(Schedulers.boundedElastic())
        .doOnComplete(() -> {
          try {
            osPipe.close();
          } catch (IOException ignored) {
          }
        })
        // TODO : what to do on error?
        .doOnError(error -> LOG.error("Something went wrong"))
        .subscribe(DataBufferUtils.releaseConsumer(), error -> LOG.error("ERROR: {}", error));
    return isPipe;
  }

}
