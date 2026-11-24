package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;

/**
 * 05: Solicitar dos eventos.
 *
 *   fuente -- request(2) --> dos eventos
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class FlowableExample {
    private FlowableExample() {}

    public static void main(String[] args) {
        var subscriber = Flowable.range(1, 5).test(0);
        subscriber.request(2);
        System.out.println(subscriber.values());
        subscriber.cancel();
    }
}
