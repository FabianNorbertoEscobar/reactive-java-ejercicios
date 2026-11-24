package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.*;
import io.smallrye.mutiny.*;

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
        Multi.createFrom().items(10L, 20L)
                .onItem().transformToUniAndConcatenate(id -> Uni.createFrom().item(() -> repo.findProduct(id)))
                .onItem().transform(Product::name)
                .collect().asList().await().indefinitely().forEach(System.out::println);
    }
}
