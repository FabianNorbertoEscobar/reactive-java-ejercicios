package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.*;
import io.smallrye.mutiny.*;

/**
 * 18: Obtener productos de un pedido.
 *
 *   pedido -> flujo de ids -> productos
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class TransformToMultiExample {
    private TransformToMultiExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Uni.createFrom().item(() -> repo.findOrders(1).getFirst())
                .onItem().transformToMulti(order -> Multi.createFrom().iterable(order.productIds()))
                .onItem().transform(repo::findProduct)
                .collect().asList().await().indefinitely().forEach(System.out::println);
    }
}
