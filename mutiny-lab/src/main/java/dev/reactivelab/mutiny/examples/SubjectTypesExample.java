package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 25: Equivalentes de los Subjects en Mutiny: BroadcastProcessor y Multi.createBy().replaying().
 *
 *   Mutiny no tiene Subjects. El equivalente de PublishSubject es BroadcastProcessor.
 *   Los demás comportamientos se obtienen componiendo un Multi con replaying():
 *
 *   Publish  -> BroadcastProcessor
 *   Behavior -> replaying().upTo(1).ofSeedAndMulti(inicial, processor)
 *   Replay   -> replaying().ofMulti(processor)  /  upTo(N) para los últimos N
 *   Async    -> replaying().upTo(1).ofMulti(processor.select().last())
 *
 * 1. A suscribe al inicio; B suscribe después de emitir el valor 1.
 * 2. Se emite 2 y se completa el procesador.
 * 3. Los Multi con replaying() arrancan al primer suscriptor: por eso A suscribe antes de emitir.
 * 4. Compara con {@code SubjectTypesExample} del módulo RxJava.
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

    /** Construye la vista del procesador que corresponde a cada tipo de Subject. */
    private static String drive(Function<BroadcastProcessor<Integer>, Multi<Integer>> view) {
        var processor = BroadcastProcessor.<Integer>create();
        var multi = view.apply(processor);
        var a = new ArrayList<Integer>();
        var b = new ArrayList<Integer>();
        multi.subscribe().with(a::add);
        processor.onNext(1);
        multi.subscribe().with(b::add);
        processor.onNext(2);
        processor.onComplete();
        return "A=" + a + " B=" + b;
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("Publish  -> " + drive(processor -> processor));
        log.add("Behavior -> " + drive(processor -> Multi.createBy().replaying().upTo(1)
                .ofSeedAndMulti(List.of(0), processor)));
        log.add("Replay   -> " + drive(processor -> Multi.createBy().replaying().ofMulti(processor)));
        log.add("Replay(1)-> " + drive(processor -> Multi.createBy().replaying().upTo(1).ofMulti(processor)));
        log.add("Async    -> " + drive(processor -> Multi.createBy().replaying().upTo(1)
                .ofMulti(processor.select().last())));
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
