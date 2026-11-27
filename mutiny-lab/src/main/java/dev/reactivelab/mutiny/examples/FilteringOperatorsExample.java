package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import java.util.ArrayList;
import java.util.List;

/**
 * 22: Operadores de filtrado: select().where, first, skip, distinct, last y equivalentes.
 *
 *   1 2 2 3 4 5 --select().where(par)--> 2 2 4
 *   1 2 2 3 4 5 --select().distinct()--> 1 2 3 4 5
 *
 * 1. Se aplica cada operador a la misma secuencia de partida.
 * 2. {@code collect().first()} devuelve un {@code Uni} que emite {@code null} si no hay valores.
 * 3. Compara con {@code FilteringOperatorsExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   filter: [2, 2, 4]
 *   take: [1, 2]
 *   skip: [2, 2, 3, 4, 5]
 *   distinct: [1, 2, 3, 4, 5]
 *   first: 1
 *   first de vacío: sin valor
 *   last: 5
 *   takeWhile: [1, 2, 2]
 *   skipWhile: [3, 4, 5]
 */
public final class FilteringOperatorsExample {
    private FilteringOperatorsExample() {}

    private static Multi<Integer> source() {
        return Multi.createFrom().items(1, 2, 2, 3, 4, 5);
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("filter: " + list(source().select().where(n -> n % 2 == 0)));
        log.add("take: " + list(source().select().first(2)));
        log.add("skip: " + list(source().skip().first(1)));
        log.add("distinct: " + list(source().select().distinct()));
        log.add("first: " + source().collect().first().await().indefinitely());
        var firstOfEmpty = Multi.createFrom().<Integer>empty().collect().first()
                .map(n -> n == null ? "sin valor" : String.valueOf(n));
        log.add("first de vacío: " + firstOfEmpty.await().indefinitely());
        log.add("last: " + list(source().select().last()).get(0));
        log.add("takeWhile: " + list(source().select().first(n -> n < 3)));
        log.add("skipWhile: " + list(source().skip().first(n -> n < 3)));
        return log;
    }

    private static <T> List<T> list(Multi<T> multi) {
        return multi.collect().asList().await().indefinitely();
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
