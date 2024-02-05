library(magrittr)
library(glue)
library(lmerTest)
library(stargazer)
library(scales)
library(cowplot)
library(ggpubr)
library(ggstatsplot)
library(easystats)
library(tidyverse)
options(scipen = 999)
path <- "~/Desktop/stale-bot"
source(glue("{path}/statsExpressions.R"))
source(glue("{path}/ggstatsplot.R"))
setwd(glue("{path}/data"))
sink("analytics.html", split = TRUE)
projects <- c(
  "nixos/nixpkgs",
  "homebrew/homebrew-core",
  "ceph/ceph",
  "automattic/wp-calypso",
  "home-assistant/core",
  "cleverraven/cataclysm-dda",
  "homebrew/linuxbrew-core",
  "istio/istio",
  "qgis/qgis",
  "devexpress/devextreme",
  "grafana/grafana",
  "wikia/app",
  "helm/charts",
  "grpc/grpc",
  "solana-labs/solana",
  "home-assistant/home-assistant.io",
  "conda-forge/staged-recipes",
  "apache/beam",
  "frappe/erpnext",
  "riot-os/riot"
)
tibble(
  backlog = c(27, 139, 151, 174, 423, 14, 40, 23, 72, 543, 228, 332, 742, 127, 65, 555, 31, 53, 2054, 293),
  open_prs = c(28, 109, 160, 58, 366, 44, 31, 24, 82, 226, 270, 207, 637, 95, 95, 454, 31, 65, 2324, 349),
  intervened_prs = c(
    70.8, 61.3, 60.7, 58.4, 54.8, 51.0, 48.0, 46.1, 32.5, 29.7, 29.0, 22.2, 20.2, 15.4, 13.2, 11.0, 10.6, 7.6, 6.9, 2.9
  ),
  warned_prs = c(
    65.5, 48.4, 47.3, 51.8, 43.8, 46.9, 41.9, 41.6, 28.7, 16.7, 26.8, 17.3, 15.0, 13.2, 10.3, 7.0, 9.5, 6.5, 5.3, 2.5
  ),
  closed_prs = c(
    30.5, 11.0, 12.1, 37.3, 23.0, 13.7, 18.4, 19.8, 9.0, 13.0, 7.9, 11.5, 3.7, 7.7, 1.6, 3.7, 3.9, 3.7, 0.0, 1.8
  ),
  days_to_stale = c(15, 14, 14, 21, 30, 22, 14, 30, 21, 150, 65, 150, 60, 60, 30, 185, 30, 60, 180, 270),
  days_to_close = c(7, 30, 27, 7, 14, 7, 7, 7, 7, 30, 7, 6, 90, 7, 30, 31, 5, 7, NA, 7)
) %>%
  correlation(method = "spearman", p_adjust = "none") %>%
  summary()
features <- tibble()
indicators <- tibble()
for (project in projects) {
  print(glue("Importing data for project {project}"))
  pattern <- str_replace(project, "/", "_")
  features %<>% bind_rows(read_csv(glue("{pattern}/{pattern}_features_fixed.csv"), guess_max = Inf))
  indicators %<>% bind_rows(read_csv(glue("{pattern}/{pattern}_indicators.csv"), guess_max = Inf))
}
features %<>% filter(between(resolved_month, 0, 11))
features$is_staled %<>%
  as_factor() %>%
  fct_recode(Intervened = "TRUE", `Not Intervened` = "FALSE") %>%
  fct_relevel("Intervened")
features %<>% rename(
  description = pr_description,
  initial_commits = pr_initial_commits,
  followup_commits = pr_followup_commits,
  initial_changed_lines = pr_initial_changed_lines,
  followup_changed_lines = pr_followup_changed_lines,
  initial_changed_files = pr_initial_changed_files,
  followup_changed_files = pr_followup_changed_files,
  submitted_pulls = contributor_pulls,
  acceptance_rate = contributor_acceptance_rate,
  contribution_period = contributor_contribution_period,
  participants = review_participants,
  participant_comments = review_participant_comments,
  contributor_comments = review_contributor_comments,
  first_latency = review_first_latency,
  mean_latency = review_mean_latency,
  resolution_time = review_resolution_time
)
print(glue("Dataset={count(features)}, Intervened={count(filter(features, is_staled=='Intervened'))}"))
closed <- filter(features, is_closed == TRUE)
unprogressed <- filter(closed, followup_commits == 0, participant_comments == 0, contributor_comments == 0)
print(glue("Closed={count(closed)}, Unprogressed={count(unprogressed)}"))
closed_3m <- filter(closed, between(resolved_month, 0, 2))
stale_closed_3m <- filter(closed_3m, is_stale_closed == TRUE)
print(glue("First 3 months: Closed={count(closed_3m)}, Stale Closed={count(stale_closed_3m)}"))
characteristics <- c(
  "description", "initial_commits", "followup_commits", "initial_changed_lines", "followup_changed_lines",
  "initial_changed_files", "followup_changed_files", "submitted_pulls", "acceptance_rate", "contribution_period",
  "participants", "participant_comments", "contributor_comments", "first_latency", "mean_latency", "resolution_time"
)
for (feature in characteristics) {
  print(glue("Creating plots for feature {feature}"))
  grouped_ggbetweenstats(
    features,
    x = is_staled,
    y = !!feature,
    grouping.var = project,
    type = "nonparametric",
    k = 3,
    centrality.point.args = list(size = 0),
    centrality.label.args = list(size = 2.5, nudge_x = 0.4, min.segment.length = 0),
    point.args = list(alpha = 0),
    boxplot.args = list(width = 0.3, alpha = 0),
    violin.args = list(width = 0.5, alpha = 0),
    plotgrid.args = list(ncol = 4),
    ggplot.component = list(
      scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())),
      theme(
        axis.title = element_blank(),
        plot.margin = margin(2.5, 2.5, 5, 2.5),
        plot.subtitle = element_text(hjust = 0.5),
        plot.title = element_text(size = 10, hjust = 0.5)
      )
    )
  ) %>%
    as_grob() %>%
    annotate_figure(left = text_grob(feature, rot = 90, face = "bold", size = 11)) %>%
    save_plot(glue("{feature}_stats.png"), plot = ., base_height = 13.25, base_width = 10.25)
}
grouped_ggbetweenstats(
  filter(features, submitted_pulls != 0),
  x = is_staled,
  y = acceptance_rate,
  grouping.var = project,
  type = "nonparametric",
  k = 3,
  centrality.point.args = list(size = 0),
  centrality.label.args = list(size = 2.5, nudge_x = 0.4, min.segment.length = 0),
  point.args = list(alpha = 0),
  boxplot.args = list(width = 0.3, alpha = 0),
  violin.args = list(width = 0.5, alpha = 0),
  plotgrid.args = list(ncol = 4),
  ggplot.component = list(
    scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())),
    theme(
      axis.title = element_blank(),
      plot.margin = margin(2.5, 2.5, 5, 2.5),
      plot.subtitle = element_text(hjust = 0.5),
      plot.title = element_text(size = 10, hjust = 0.5)
    )
  )
) %>%
  as_grob() %>%
  annotate_figure(left = text_grob("acceptance_rate", rot = 90, face = "bold", size = 11)) %>%
  save_plot(glue("acceptance_rate2_stats.png"), plot = ., base_height = 13.25, base_width = 10.25)
differences <- data.frame()
for (project in projects) {
  data <- filter(features, project == !!project)
  staled <- filter(data, is_staled == "Intervened")
  nonstaled <- filter(data, is_staled == "Not Intervened")
  for (feature in characteristics) {
    differences[project, feature] <- median(staled[[feature]]) - median(nonstaled[[feature]])
  }
}
write_csv(rownames_to_column(differences, "project"), "differences.csv")
data <- filter(features, project == "cleverraven/cataclysm-dda")
for (feature in c("followup_commits", "followup_changed_lines", "followup_changed_files")) {
  ggbetweenstats(
    data,
    x = is_staled,
    y = !!feature,
    type = "nonparametric",
    k = 3,
    centrality.point.args = list(size = 0),
    centrality.label.args = list(size = 2.5, nudge_x = 0.4, min.segment.length = 0),
    point.args = list(alpha = 0),
    boxplot.args = list(width = 0.3, alpha = 0),
    violin.args = list(width = 0.5, alpha = 0),
    ggplot.component = list(
      scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())),
      coord_trans(y = if_else(max(data[[feature]]) > 100, "log1p", "identity")),
      theme(
        axis.title = element_blank(),
        plot.margin = margin(2.5, 2.5, 5, 2.5),
        plot.subtitle = element_text(hjust = 0.5),
        plot.title = element_text(size = 10, hjust = 0.5)
      )
    )
  ) %>%
    annotate_figure(left = text_grob(feature, rot = 90, size = 11)) %>%
    save_plot(glue("{feature}_stats_cleverraven.png"), plot = ., base_height = 4.5, base_width = 2.5)
}
data <- filter(features, project == "homebrew/homebrew-core")
for (feature in c(
  "submitted_pulls", "acceptance_rate", "contribution_period", "participants", "participant_comments",
  "contributor_comments", "first_latency", "mean_latency", "resolution_time"
)) {
  ggbetweenstats(
    data,
    x = is_staled,
    y = !!feature,
    type = "nonparametric",
    k = 3,
    centrality.point.args = list(size = 0),
    centrality.label.args = list(size = 2.5, nudge_x = 0.4, min.segment.length = 0),
    point.args = list(alpha = 0),
    boxplot.args = list(width = 0.3, alpha = 0),
    violin.args = list(width = 0.5, alpha = 0),
    ggplot.component = list(
      scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())),
      coord_trans(y = if_else(max(data[[feature]]) > 100, "log1p", "identity")),
      theme(
        axis.title = element_blank(),
        plot.margin = margin(2.5, 2.5, 5, 2.5),
        plot.subtitle = element_text(hjust = 0.5),
        plot.title = element_text(size = 10, hjust = 0.5)
      )
    )
  ) %>%
    annotate_figure(left = text_grob(feature, rot = 90, size = 11)) %>%
    save_plot(glue("{feature}_stats_homebrew.png"), plot = ., base_height = 4.5, base_width = 2.5)
}
indicators %<>%
  filter(between(resolved_month, -12, 11)) %>%
  group_by(project) %>%
  mutate(time = time - min(time) + 1) %>%
  ungroup() %>%
  mutate(adoption = if_else(adoption == TRUE, 1, 0)) %>%
  rename(
    first_latency = review_first_latency,
    mean_latency = review_mean_latency,
    resolution_time = review_resolution_time,
    comments = review_comments,
    commits = pr_commits,
    contributors = active_contributors
  )
indicators_merged <- filter(indicators, is_merged == TRUE)
indicators_closed <- filter(indicators, is_merged == FALSE)
torename <- c("first_latency", "mean_latency", "resolution_time", "comments", "commits")
indicators_merged %<>% rename_with(function(name) paste0(name, "_m"), all_of(torename))
indicators_closed %<>% rename_with(function(name) paste0(name, "_c"), all_of(torename))
explain <- function(model) {
  print(performance(model))
  statistics <- anova(model)
  statistics$`Sum Sq` %<>% round(digits = 3)
  print(statistics)
  print(parameters(model, ci_method = "satterthwaite"))
  class(model) <- "lmerMod"
  stargazer(model, type = "text", star.cutoffs = c(0.05, 0.01, 0.001))
}
print("merged_pulls:")
merged_pulls_model <- lmer(
  log(merged_pulls) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(merged_pulls_model)
print("closed_pulls:")
closed_pulls_model <- lmer(
  log(closed_pulls) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_closed
)
explain(closed_pulls_model)
print("first_latency_m:")
first_latency_m_model <- lmer(
  log(first_latency_m) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(first_latency_m_model)
print("first_latency_c:")
first_latency_c_model <- lmer(
  log(first_latency_c) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_closed
)
explain(first_latency_c_model)
print("mean_latency_m:")
mean_latency_m_model <- lmer(
  log(mean_latency_m) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(mean_latency_m_model)
print("mean_latency_c:")
mean_latency_c_model <- lmer(
  log(mean_latency_c) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_closed
)
explain(mean_latency_c_model)
print("resolution_time_m:")
resolution_time_m_model <- lmer(
  log(resolution_time_m) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(resolution_time_m_model)
print("resolution_time_c:")
resolution_time_c_model <- lmer(
  log(resolution_time_c) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_closed
)
explain(resolution_time_c_model)
print("comments_m:")
comments_m_model <- lmer(
  log(comments_m) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(comments_m_model)
print("comments_c:")
comments_c_model <- lmer(
  log(comments_c) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_closed
)
explain(comments_c_model)
print("commits_m:")
commits_m_model <- lmer(
  log(commits_m) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(commits_m_model)
print("commits_c:")
commits_c_model <- lmer(
  log(commits_c) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_closed
)
explain(commits_c_model)
print("contributors:")
contributors_model <- lmer(
  log(contributors) ~
    time
    + adoption
    + time_since_adoption
    + log(age_at_adoption)
    + log(pulls_at_adoption)
    + log(contributors_at_adoption)
    + log(maintainers_at_adoption)
    + (1 | project),
  data = indicators_merged
)
explain(contributors_model)
for (indicator in c(
  "merged_pulls", "first_latency_m", "mean_latency_m", "resolution_time_m", "comments_m", "commits_m", "contributors"
)) {
  values <- list()
  predictions <- vector()
  counterfactual <- vector()
  for (time in 1:24) {
    data <- filter(indicators_merged, time == !!time)
    values[[time]] <- data[[indicator]]
    predictions[time] <- mean(exp(predict(get(glue("{indicator}_model")), newdata = data)))
    counterfactual[time] <- mean(
      exp(predict(get(glue("{indicator}_model")), newdata = mutate(data, adoption = 0, time_since_adoption = 0)))
    )
  }
  predictions <- tibble(time = 1:24, predictions = predictions, counterfactual = counterfactual)
  plot <- ggplot(indicators_merged, aes(x = factor(time), y = get(indicator))) +
    geom_boxplot(outlier.shape = NA, lwd = 0.25) +
    geom_vline(xintercept = 12.5, lwd = 0.25, col = "blue") +
    labs(x = "time", y = indicator) +
    scale_x_discrete(breaks = c(1, 6, 12, 18, 24)) +
    coord_cartesian(ylim = c(0, max(boxplot(values)$stat))) +
    theme_classic()
  if (indicator %in% c("merged_pulls", "first_latency_m", "commits_m", "contributors")) {
    mutate(predictions, change = (predictions - counterfactual) / abs(counterfactual)) %>%
      write_csv(glue("{indicator}.csv"))
    plot <- plot +
      geom_line(aes(x = time, y = predictions), data = filter(predictions, time < 13), lwd = 0.5, col = "red") +
      geom_line(aes(x = time, y = predictions), data = filter(predictions, time >= 13), lwd = 0.5, col = "red") +
      geom_line(
        aes(x = time, y = counterfactual),
        data = filter(predictions, time >= 13),
        linetype = "dotted", lwd = 0.5, col = "red"
      )
  }
  ggsave(glue("{indicator}.png"), plot = plot, width = 3, height = 2.5)
}
for (indicator in c(
  "closed_pulls", "first_latency_c", "mean_latency_c", "resolution_time_c", "comments_c", "commits_c"
)) {
  values <- list()
  predictions <- vector()
  counterfactual <- vector()
  for (time in 1:24) {
    data <- filter(indicators_closed, time == !!time)
    values[[time]] <- data[[indicator]]
    predictions[time] <- mean(exp(predict(get(glue("{indicator}_model")), newdata = data)))
    counterfactual[time] <- mean(
      exp(predict(get(glue("{indicator}_model")), newdata = mutate(data, adoption = 0, time_since_adoption = 0)))
    )
  }
  predictions <- tibble(time = 1:24, predictions = predictions, counterfactual = counterfactual)
  plot <- ggplot(indicators_closed, aes(x = factor(time), y = get(indicator))) +
    geom_boxplot(outlier.shape = NA, lwd = 0.25) +
    geom_vline(xintercept = 12.5, lwd = 0.25, col = "blue") +
    labs(x = "time", y = indicator) +
    scale_x_discrete(breaks = c(1, 6, 12, 18, 24)) +
    coord_cartesian(ylim = c(0, max(boxplot(values)$stat))) +
    theme_classic()
  if (indicator %in% c("closed_pulls", "resolution_time_c")) {
    mutate(predictions, change = (predictions - counterfactual) / abs(counterfactual)) %>%
      write_csv(glue("{indicator}.csv"))
    plot <- plot +
      geom_line(aes(x = time, y = predictions), data = filter(predictions, time < 13), lwd = 0.5, col = "red") +
      geom_line(aes(x = time, y = predictions), data = filter(predictions, time >= 13), lwd = 0.5, col = "red") +
      geom_line(
        aes(x = time, y = counterfactual),
        data = filter(predictions, time >= 13),
        linetype = "dotted", lwd = 0.5, col = "red"
      )
  }
  ggsave(glue("{indicator}.png"), plot = plot, width = 3, height = 2.5)
}
