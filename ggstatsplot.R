.centrality_ggrepel <- function(plot,
                                data,
                                x,
                                y,
                                centrality.path = FALSE,
                                centrality.path.args = list(linewidth = 1, color = "red", alpha = 0.5),
                                centrality.point.args = list(size = 5, color = "darkred"),
                                centrality.label.args = list(size = 3, nudge_x = 0.4, segment.linetype = 4),
                                ...) {
  centrality_df <- suppressWarnings(centrality_description(data, {{ x }}, {{ y }}, ...))

  maximum <- max(centrality_df[y])
  centrality_df %<>% mutate(expression = glue("list(
    M=='{insight::format_value(get(y), digits = ifelse(maximum <= 1, 3, 0), protect_integers = TRUE, zap_small = TRUE)}')"))

  # if there should be lines connecting mean values across groups
  if (isTRUE(centrality.path)) {
    plot <- plot +
      exec(
        geom_path,
        data = centrality_df,
        mapping = aes({{ x }}, {{ y }}, group = 1L),
        inherit.aes = FALSE,
        !!!centrality.path.args
      )
  }

  plot + # highlight the mean of each group
    exec(
      geom_point,
      mapping = aes({{ x }}, {{ y }}),
      data = centrality_df,
      inherit.aes = FALSE,
      !!!centrality.point.args
    ) + # attach the labels with means to the plot
    exec(
      ggrepel::geom_label_repel,
      data = centrality_df,
      mapping = aes({{ x }}, {{ y }}, label = expression),
      inherit.aes = FALSE,
      parse = TRUE,
      !!!centrality.label.args
    ) + # adding sample size labels to the x axes
    scale_x_discrete(labels = unique(centrality_df$n.expression))
}

environment(.centrality_ggrepel) <- asNamespace("ggstatsplot")
assignInNamespace(".centrality_ggrepel", .centrality_ggrepel, "ggstatsplot")
