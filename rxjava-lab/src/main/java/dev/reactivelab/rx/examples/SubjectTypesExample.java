package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.subjects.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 25: Tipos de Subject: PublishSubject, BehaviorSubject, ReplaySubject y AsyncSubject.
 *
 *   Un Subject es a la vez Observer (recibe onNext) y Observable (reenvía a sus suscriptores).
 *   Es una fuente caliente: la emisión no depende de quién esté suscrito.
 *
 *   Publish:  solo eventos posteriores a la suscripción
 *   Behavior: el último valor (o el inicial) y los posteriores
 *   Replay:   todo el historial (o los últimos N) y los posteriores
 *   Async:    solo el último valor, y únicamente cuando se completa
 *
 * 1. A suscribe al inicio; B suscribe después de emitir el valor 1.
 * 2. Se emite 2 y se completa el subject.
 * 3. Compara con {@code SubjectTypesExample} del módulo Mutiny (procesadores y replaying).
 *
 * Resultado esperado:
 *   Publish  -> A=[1, 2] B=[2]
 *   Behavior -> A=[0, 1, 2] B=[1, 2]
 *   Replay   -> A=[1, 2] B=[1, 2]
 *   Replay(1)-> A=[1, 2] B=[1, 2]
 *   Async    -> A=[2] B=[2]
 */
public final class SubjectTypesExample {
    private SubjectTypesExample() {}

    private static String drive(Subject<Integer> subject) {
        var a = new ArrayList<Integer>();
        var b = new ArrayList<Integer>();
        subject.subscribe(a::add);
        subject.onNext(1);
        subject.subscribe(b::add);
        subject.onNext(2);
        subject.onComplete();
        return "A=" + a + " B=" + b;
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("Publish  -> " + drive(PublishSubject.create()));
        log.add("Behavior -> " + drive(BehaviorSubject.createDefault(0)));
        log.add("Replay   -> " + drive(ReplaySubject.create()));
        // createWithSize(1) solo conserva el último valor para los suscriptores tardíos.
        log.add("Replay(1)-> " + drive(ReplaySubject.createWithSize(1)));
        log.add("Async    -> " + drive(AsyncSubject.create()));
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
