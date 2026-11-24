package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;

/**
 * 15: Consumir solo tres eventos.
 *
 *   fuente -- demanda(3) --> 1, 2, 3 -- cancelar
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class BackpressureExample {
    private BackpressureExample() {}

    public static void main(String[] args) {
        var subscriber = Flowable.range(1, 100).test(0);
        subscriber.request(3);
        System.out.println(subscriber.values());
        subscriber.cancel();
    }
}
