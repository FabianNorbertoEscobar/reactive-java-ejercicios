package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.exceptions.MissingBackpressureException;
import io.reactivex.rxjava3.processors.PublishProcessor;
import io.reactivex.rxjava3.core.Flowable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 27: Backpressure y estrategias de control de demanda en Flowable.
 *
 *   productor --(eventos)--> [estrategia] --(solo lo pedido)--> consumidor request(n)
 *
 * Backpressure es el mecanismo por el que un consumidor lento le indica al productor
 * cuántos elementos puede procesar (request(n)). Cuando el productor no puede frenar
 * (una fuente caliente como un procesador), se elige qué hacer con el exceso:
 *
 *   sin estrategia           -> MissingBackpressureException
 *   onBackpressureBuffer()   -> guarda todo hasta que haya demanda (cuidado con la memoria)
 *   onBackpressureDrop()     -> descarta lo que llega sin demanda
 *   onBackpressureLatest()   -> conserva solo el más reciente
 *
 * Observable NO tiene backpressure: use Flowable cuando la fuente pueda superar al consumidor.
 *
 * 1. Se crea un PublishProcessor (fuente caliente) con un consumidor con demanda 0.
 * 2. Se emiten 1..5 y se piden elementos después.
 * 3. Compara con {@code BackpressureStrategiesExample} del módulo Mutiny.
 *
 * Resultado esperado:
 *   sin estrategia: fallo MissingBackpressureException, items=[]
 *   buffer: [1, 2, 3, 4, 5]
 *   drop: [1, 2]
 *   latest: [1, 5]
 *   fuente fría con request(n): [1, 2, 3] luego [1, 2, 3, 4, 5]
 */
public final class BackpressureStrategiesExample {
    private BackpressureStrategiesExample() {}

    public static String withoutStrategy() {
        var processor = PublishProcessor.<Integer>create();
        var subscriber = processor.test(0);
        processor.onNext(1); // sin demanda y sin estrategia
        subscriber.assertError(MissingBackpressureException.class);
        return "fallo MissingBackpressureException=true, items=" + subscriber.values();
    }

    private static List<Integer> emitFiveWith(
            Function<Flowable<Integer>, Flowable<Integer>> strategy, long initialDemand, long laterDemand) {
        var processor = PublishProcessor.<Integer>create();
        var subscriber = strategy.apply(processor).test(initialDemand);
        for (int i = 1; i <= 5; i++) processor.onNext(i);
        if (laterDemand > 0) subscriber.request(laterDemand);
        subscriber.cancel();
        return subscriber.values();
    }

    public static String coldSourceRequests() {
        var subscriber = Flowable.range(1, 5).test(0);
        subscriber.request(3);
        var first = new ArrayList<>(subscriber.values());
        subscriber.request(10);
        return first + " luego " + subscriber.values();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("sin estrategia: " + withoutStrategy());
        log.add("buffer: " + emitFiveWith(Flowable::onBackpressureBuffer, 0, 10));
        // drop: con demanda inicial 2 solo se entregan 1 y 2; 3, 4 y 5 se descartan.
        log.add("drop: " + emitFiveWith(Flowable::onBackpressureDrop, 2, 10));
        // latest: se entrega el 1 y, al pedir de nuevo, solo el último guardado (5).
        log.add("latest: " + emitFiveWith(Flowable::onBackpressureLatest, 1, 10));
        log.add("fuente fría con request(n): " + coldSourceRequests());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
