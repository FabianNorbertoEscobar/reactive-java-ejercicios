package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 06: Transformar nombres.
 *
 *   usuarios -- map/transform --> nombres
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class MapExample {
    private MapExample() {}

    public static void main(String[] args) {
        Observable.fromIterable(FakeDatabase.USERS).map(User::name)
                .blockingSubscribe(System.out::println);
    }
}
