package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 19: Transformar cada id mediante una consulta.
 *
 *   ids -> consultas ordenadas -> nombres
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class MultiTransformationsExample {
    private MultiTransformationsExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Observable.fromArray(10L, 20L)
                .concatMapSingle(id -> Single.fromCallable(() -> repo.findProduct(id)))
                .map(Product::name).blockingSubscribe(System.out::println);
    }
}
