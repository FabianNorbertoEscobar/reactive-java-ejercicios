package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;

/**
 * 11: Concatenar eventos.
 *
 *   fuente A -> fin -> fuente B -> fin
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class ConcatExample {
    private ConcatExample() {}

    public static void main(String[] args) {
        Observable.concat(Observable.just("A1", "A2"), Observable.just("B1", "B2"))
                .blockingSubscribe(System.out::println);
    }
}
