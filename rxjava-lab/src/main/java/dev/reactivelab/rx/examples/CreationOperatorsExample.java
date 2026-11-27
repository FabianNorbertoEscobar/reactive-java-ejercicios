package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 20: Operadores de creación: just, fromIterable, range, empty, error y create.
 *
 *   just(1,2,3) | fromIterable(lista) | range(1,5) | empty() | error(e) | create(emitter)
 *
 * 1. Cada línea crea una fuente distinta y la recoge en una lista para imprimirla.
 * 2. {@code create} permite emitir manualmente; siempre termina con onComplete u onError.
 * 3. Compara con {@code CreationOperatorsExample} del módulo Mutiny.
 *
 * Resultado esperado:
 *   just: [1, 2, 3]
 *   fromIterable: [a, b]
 *   range: [1, 2, 3, 4, 5]
 *   empty: []
 *   error: error capturado: boom
 *   create: [10, 20]
 *   defer: perezoso (se evalúa al suscribirse)
 */
public final class CreationOperatorsExample {
    private CreationOperatorsExample() {}

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("just: " + Observable.just(1, 2, 3).toList().blockingGet());
        log.add("fromIterable: " + Observable.fromIterable(List.of("a", "b")).toList().blockingGet());
        // range(inicio, cantidad): en RxJava el segundo argumento es la cantidad.
        log.add("range: " + Flowable.range(1, 5).toList().blockingGet());
        log.add("empty: " + Observable.empty().toList().blockingGet());
        log.add("error: " + Observable.<String>error(new IllegalStateException("boom"))
                .onErrorReturn(t -> "error capturado: " + t.getMessage()).blockingFirst());
        log.add("create: " + Observable.<Integer>create(emitter -> {
            emitter.onNext(10);
            emitter.onNext(20);
            emitter.onComplete();
        }).toList().blockingGet());
        // defer difiere la creación hasta la suscripción.
        var deferred = Single.defer(() -> Single.just("perezoso (se evalúa al suscribirse)"));
        log.add("defer: " + deferred.blockingGet());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
