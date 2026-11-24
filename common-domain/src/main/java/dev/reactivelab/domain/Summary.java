package dev.reactivelab.domain;

import java.math.BigDecimal;
import java.util.List;

/** Resultado común para contrastar ambas implementaciones del caso de uso. */
public record Summary(String user, List<String> products, BigDecimal total, String status) {
    public Summary {
        products = List.copyOf(products);
    }

    public static Summary of(User user, List<Product> products, String status) {
        return new Summary(user.name(), products.stream().map(Product::name).toList(),
                products.stream().map(Product::price).reduce(BigDecimal.ZERO, BigDecimal::add), status);
    }
}
