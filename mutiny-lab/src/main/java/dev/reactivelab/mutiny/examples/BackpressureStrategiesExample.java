package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 27: Backpressure y estrategias de control de demanda en Multi.
 *
 *   productor --(eventos)--> [onOverflow()] --(solo lo pedido)--> consumidor request(n)
 *
 * Backpressure es el mecanismo por el que un consumidor lento le indica al productor
 * cuántos elementos puede procesar (request(n)). Multi implementa Reactive Streams, así
 * que se comporta como Flowable. Cuando el productor no puede frenar, onOverflow() elige:
 *
 *   sin estrategia                -> BackPressureFailure
 *   onOverflow().buffer(n)        -> guarda hasta n elementos (falla si se supera)
 *   onOverflow().bufferUnconditionally() -> buffer sin límite (cuidado con la memoria)
 *   onOverflow().drop()           -> descarta lo que llega sin demanda
 *   onOverflow().dropPreviousItems() -> conserva solo el más reciente
 *
 * Diferencias con RxJava: Multi es el único tipo de secuencia (no hay Observable sin
 * backpressure); la estrategia se llama onOverflow() y no existen operadores con prefijo
 * onBackpressure*. Multi.createFrom().emitter() también acepta estas estrategias.
 *
 * 1. Se crea un BroadcastProcessor (fuente caliente) con un consumidor con demanda 0.
 * 2. Se emiten 1..5 y se piden elementos después.
 * 3. Compara con {@code BackpressureStrategiesExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   sin estrategia: fallo BackPressureFailure=true, items=[]
 *   buffer: [1, 2, 3, 4, 5]
 *   drop: [1, 2]
 *   latest: [1, 5]
 *   fuente fría con request(n): [1, 2, 3] luego [1, 2, 3, 4, 5]
 */
public final class BackpressureStrategiesExample {
    private BackpressureStrategiesExample() {}

    public static String withoutStrategy() {
        var processor = BroadcastProcessor.<Integer>create();
        var subscriber = processor.subscribe().withSubscriber(AssertSubscriber.<Integer>create(0));
        processor.onNext(1); // sin demanda y sin estrategia
        var failed = subscriber.getFailure() instanceof BackPressureFailure;
        return "fallo BackPressureFailure=" + failed + ", items=" + subscriber.getItems();
    }

    private static List<Integer> emitFiveWith(
            Function<Multi<Integer>, Multi<Integer>> strategy, long initialDemand, long laterDemand) {
        var processor = BroadcastProcessor.<Integer>create();
        var subscriber = strategy.apply(processor).subscribe()
                .withSubscriber(AssertSubscriber.<Integer>create(initialDemand));
        for (int i = 1; i <= 5; i++) processor.onNext(i);
        if (laterDemand > 0) subscriber.request(laterDemand);
        subscriber.cancel();
        return subscriber.getItems();
    }

    public static String coldSourceRequests() {
        var subscriber = Multi.createFrom().range(1, 6).subscribe()
                .withSubscriber(AssertSubscriber.<Integer>create(0));
        subscriber.request(3);
        var first = new ArrayList<>(subscriber.getItems());
        subscriber.request(10);
        return first + " luego " + subscriber.getItems();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("sin estrategia: " + withoutStrategy());
        log.add("buffer: " + emitFiveWith(multi -> multi.onOverflow().buffer(16), 0, 10));
        // drop: con demanda inicial 2 solo se entregan 1 y 2; 3, 4 y 5 se descartan.
        log.add("drop: " + emitFiveWith(multi -> multi.onOverflow().drop(), 2, 10));
        // latest: se entrega el 1 y, al pedir de nuevo, solo el último guardado (5).
        log.add("latest: " + emitFiveWith(multi -> multi.onOverflow().dropPreviousItems(), 1, 10));
        log.add("fuente fría con request(n): " + coldSourceRequests());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
