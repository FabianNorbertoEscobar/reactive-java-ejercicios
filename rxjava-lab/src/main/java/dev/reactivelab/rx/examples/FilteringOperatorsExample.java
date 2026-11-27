package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 22: Operadores de filtrado: filter, take, skip, distinct, first y equivalentes.
 *
 *   1 2 2 3 4 5 --filter(par)--> 2 2 4
 *   1 2 2 3 4 5 --distinct-----> 1 2 3 4 5
 *
 * 1. Se aplica cada operador a la misma secuencia de partida.
 * 2. {@code firstElement} devuelve {@code Maybe} (puede estar vacío); {@code first(default)}
 *    devuelve {@code Single}.
 * 3. Compara con {@code FilteringOperatorsExample} del módulo Mutiny.
 *
 * Resultado esperado:
 *   filter: [2, 2, 4]
 *   take: [1, 2]
 *   skip: [2, 2, 3, 4, 5]
 *   distinct: [1, 2, 3, 4, 5]
 *   first: 1
 *   firstElement de vacío: sin valor
 *   last: 5
 *   takeWhile: [1, 2, 2]
 *   skipWhile: [3, 4, 5]
 */
public final class FilteringOperatorsExample {
    private FilteringOperatorsExample() {}

    private static Observable<Integer> source() {
        return Observable.just(1, 2, 2, 3, 4, 5);
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("filter: " + source().filter(n -> n % 2 == 0).toList().blockingGet());
        log.add("take: " + source().take(2).toList().blockingGet());
        log.add("skip: " + source().skip(1).toList().blockingGet());
        log.add("distinct: " + source().distinct().toList().blockingGet());
        log.add("first: " + source().firstOrError().blockingGet());
        log.add("firstElement de vacío: "
                + Observable.<Integer>empty().firstElement().map(String::valueOf).defaultIfEmpty("sin valor").blockingGet());
        log.add("last: " + source().lastOrError().blockingGet());
        log.add("takeWhile: " + source().takeWhile(n -> n < 3).toList().blockingGet());
        log.add("skipWhile: " + source().skipWhile(n -> n < 3).toList().blockingGet());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
