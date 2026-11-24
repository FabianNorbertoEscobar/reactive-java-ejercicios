package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.*;
import io.smallrye.mutiny.*;
import java.math.BigDecimal;

/**
 * 08: Seleccionar productos económicos.
 *
 *   productos -- filtro --> precio <= 20
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class FilterExample {
    private FilterExample() {}

    public static void main(String[] args) {
        Multi.createFrom().iterable(FakeDatabase.PRODUCTS)
                .select().where(p -> p.price().compareTo(new BigDecimal("20")) <= 0)
                .collect().asList().await().indefinitely().forEach(System.out::println);
    }
}
