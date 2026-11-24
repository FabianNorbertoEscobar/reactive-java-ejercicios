package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;

/**
 * 10: Mezclar eventos.
 *
 *   fuente A --+--> eventos intercalados
 *   fuente B --+
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class MergeExample {
    private MergeExample() {}

    public static void main(String[] args) {
        Observable.merge(Observable.just("A1", "A2"), Observable.just("B1", "B2"))
                .blockingSubscribe(System.out::println);
    }
}
