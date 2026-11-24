package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 02: Buscar un usuario inexistente.
 *
 *   consulta -> usuario o ausencia
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class MaybeExample {
    private MaybeExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Maybe.fromOptional(repo.findUser(999)).subscribe(
                System.out::println, Throwable::printStackTrace, () -> System.out.println("No encontrado"));
    }
}
