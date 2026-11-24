package dev.reactivelab.mutiny;

import dev.reactivelab.domain.FakeRepository;
import dev.reactivelab.domain.Summary;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class OrderSummaryExampleTest {
    private final OrderSummaryExample example = new OrderSummaryExample(new FakeRepository());

    @Test
    void resumeElPedidoConUniAssertSubscriber() {
        example.summarize(1).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5))
                .assertCompleted()
                .assertItem(new Summary("Ana", List.of("Teclado", "Ratón"),
                        new BigDecimal("60.00"), "CONFIRMADO"));
    }

    @Test
    void usuarioDesconocidoPropagaError() {
        example.summarize(999).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure(Duration.ofSeconds(5)).assertFailedWith(NoSuchElementException.class, "Usuario no encontrado");
    }

    @Test
    void transformYSelectProducenLosValoresEsperados() {
        Multi.createFrom().items(1, 2, 3).select().where(n -> n > 1).onItem().transform(n -> n * 10)
                .subscribe().withSubscriber(AssertSubscriber.create(2))
                .assertCompleted().assertItems(20, 30);
    }
}
