package dev.reactivelab.domain;

import java.util.List;

/** Pedido inmutable: se copia la lista para evitar modificaciones externas. */
public record Order(long id, long userId, List<Long> productIds) {
    public Order {
        productIds = List.copyOf(productIds);
    }
}
