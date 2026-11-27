package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.ArrayList;
import java.util.List;

/**
 * 20: Operadores de creación: items, iterable, range, empty, failure y emitter.
 *
 *   items(1,2,3) | iterable(lista) | range(1,6) | empty() | failure(e) | emitter(...)
 *
 * 1. Cada línea crea una fuente distinta y la recoge en una lista para imprimirla.
 * 2. {@code emitter} es el equivalente de {@code create}; emite manualmente.
 * 3. Compara con {@code CreationOperatorsExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   items: [1, 2, 3]
 *   iterable: [a, b]
 *   range: [1, 2, 3, 4, 5]
 *   empty: []
 *   failure: error capturado: boom
 *   emitter: [10, 20]
 *   deferred: perezoso (se evalúa al suscribirse)
 */
public final class CreationOperatorsExample {
    private CreationOperatorsExample() {}

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("items: " + list(Multi.createFrom().items(1, 2, 3)));
        log.add("iterable: " + list(Multi.createFrom().iterable(List.of("a", "b"))));
        // range(inicio inclusivo, fin exclusivo): difiere de RxJava, que recibe la cantidad.
        log.add("range: " + list(Multi.createFrom().range(1, 6)));
        log.add("empty: " + list(Multi.createFrom().empty()));
        log.add("failure: " + Multi.createFrom().<String>failure(new IllegalStateException("boom"))
                .onFailure().recoverWithItem(t -> "error capturado: " + t.getMessage())
                .collect().first().await().indefinitely());
        log.add("emitter: " + list(Multi.createFrom().<Integer>emitter(emitter -> {
            emitter.emit(10);
            emitter.emit(20);
            emitter.complete();
        })));
        // Uni.deferred difiere la creación hasta la suscripción.
        var deferred = Uni.createFrom().deferred(() -> Uni.createFrom().item("perezoso (se evalúa al suscribirse)"));
        log.add("deferred: " + deferred.await().indefinitely());
        return log;
    }

    private static <T> List<T> list(Multi<T> multi) {
        return multi.collect().asList().await().indefinitely();
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
