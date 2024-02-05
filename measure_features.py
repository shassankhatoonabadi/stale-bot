import joblib
import numpy as np
import pandas as pd

from common import (
    DATE,
    cleanup_files,
    force_refresh,
    get_logger,
    get_path,
    import_dataset,
    import_patches,
    import_pulls,
    initialize,
    postprocessed,
)

initialize()


def measure_pull(project, dataset, pulls, patches, pull_number):
    timeline = dataset.query("pull_number == @pull_number")
    pulled = timeline.query("event == 'pulled'")
    contributor = pulled["actor"].iat[0]
    is_core = pulled["is_core"].iat[0]
    is_open = pulled["is_open"].iat[0]
    is_closed = pulled["is_closed"].iat[0]
    is_merged = pulled["is_merged"].iat[0]
    is_staled = pulled["is_staled"].iat[0]
    is_stale_closed = pulled["is_stale_closed"].iat[0]
    opened_at = pulled["opened_at"].iat[0]
    closed_at = pulled["closed_at"].iat[0]
    merged_at = pulled["merged_at"].iat[0]
    resolved_at = pulled["resolved_at"].iat[0]
    resolved_by = pulled["resolved_by"].iat[0]
    first_staled_at = pulled["first_staled_at"].iat[0]
    last_staled_at = pulled["last_staled_at"].iat[0]
    first_stale_closed_at = pulled["first_stale_closed_at"].iat[0]
    last_stale_closed_at = pulled["last_stale_closed_at"].iat[0]
    if pd.notna(resolved_at):
        timeline = timeline.query("time <= @resolved_at")
    title = pulls.loc[pull_number, "title"]
    body = pulls.loc[pull_number, "body"]
    pr_description = (len(title.split()) if pd.notna(title) else 0) + (len(body.split()) if pd.notna(body) else 0)
    commits = timeline.query("event == 'committed'")
    initial_commits = commits.query("time <= @opened_at")
    followup_commits = commits.query("time > @opened_at")
    pr_commits = len(commits)
    pr_initial_commits = len(initial_commits)
    pr_followup_commits = len(followup_commits)
    patches = patches.query("pull_number == @pull_number")
    pr_initial_changed_lines = 0
    pr_followup_changed_lines = 0
    pr_initial_changed_files = 0
    pr_followup_changed_files = 0
    changed_files = set()
    for sha in initial_commits["sha"]:
        if not (patch := patches.query("sha == @sha")).empty:
            pr_initial_changed_lines += patch["added_lines"].iat[0] + patch["deleted_lines"].iat[0]
            if files := [file for file in eval(patch["files"].iat[0]) if file not in changed_files]:
                pr_initial_changed_files += len(files)
                changed_files.update(files)
    for sha in followup_commits["sha"]:
        if not (patch := patches.query("sha == @sha")).empty:
            pr_followup_changed_lines += patch["added_lines"].iat[0] + patch["deleted_lines"].iat[0]
            if files := [file for file in eval(patch["files"].iat[0]) if file not in changed_files]:
                pr_followup_changed_files += len(files)
                changed_files.update(files)
    pr_changed_lines = pr_initial_changed_lines + pr_followup_changed_lines
    pr_changed_files = pr_initial_changed_files + pr_followup_changed_files
    contributor_pulled = dataset.query("pull_number < @pull_number and event == 'pulled' and actor == @contributor")
    contributor_pulls = len(contributor_pulled)
    contributor_acceptance_rate = (
        len(contributor_pulled.query("merged_at < @opened_at")) / contributor_pulls if contributor_pulls else 0
    )
    contributor_contribution_period = (
        (opened_at - contributor_pulled["opened_at"].min()) / np.timedelta64(1, "M")
        if not contributor_pulled.empty
        else 0
    )
    updates = timeline.query("time > opened_at and event not in ['mentioned', 'subscribed'] and not is_stale")
    comments = updates.query("event in ['commented', 'reviewed', 'line-commented', 'commit-commented']")
    review_participants = updates.query("not is_contributor")["actor"].nunique()
    review_comments = len(comments)
    review_contributor_comments = len(comments.query("is_contributor"))
    participant_comments = comments.query("not is_contributor")
    review_participant_comments = len(participant_comments)
    review_resolution_time = (resolved_at - opened_at if pd.notna(resolved_at) else DATE - opened_at) / np.timedelta64(
        1, "h"
    )
    review_first_latency = (
        (participant_comments["time"].min() - opened_at) / np.timedelta64(1, "h")
        if not participant_comments.empty
        else review_resolution_time
    )
    review_mean_latency = (
        pd.concat([pulled, participant_comments])["time"].diff().mean() / np.timedelta64(1, "h")
        if not participant_comments.empty
        else review_resolution_time
    )
    return {
        # Identifiers
        "project": project,
        "pull_number": pull_number,
        "contributor": contributor,
        "is_core": is_core,
        "is_open": is_open,
        "is_closed": is_closed,
        "is_merged": is_merged,
        "is_staled": is_staled,
        "is_stale_closed": is_stale_closed,
        "opened_at": opened_at,
        "closed_at": closed_at,
        "merged_at": merged_at,
        "resolved_at": resolved_at,
        "resolved_by": resolved_by,
        "first_staled_at": first_staled_at,
        "last_staled_at": last_staled_at,
        "first_stale_closed_at": first_stale_closed_at,
        "last_stale_closed_at": last_stale_closed_at,
        # PR Features
        "pr_description": pr_description,
        "pr_commits": pr_commits,
        "pr_initial_commits": pr_initial_commits,
        "pr_followup_commits": pr_followup_commits,
        "pr_changed_lines": pr_changed_lines,
        "pr_initial_changed_lines": pr_initial_changed_lines,
        "pr_followup_changed_lines": pr_followup_changed_lines,
        "pr_changed_files": pr_changed_files,
        "pr_initial_changed_files": pr_initial_changed_files,
        "pr_followup_changed_files": pr_followup_changed_files,
        # Contributor Features
        "contributor_pulls": contributor_pulls,
        "contributor_acceptance_rate": contributor_acceptance_rate,
        "contributor_contribution_period": contributor_contribution_period,
        # Review Process Features
        "review_participants": review_participants,
        "review_comments": review_comments,
        "review_contributor_comments": review_contributor_comments,
        "review_participant_comments": review_participant_comments,
        "review_first_latency": review_first_latency,
        "review_mean_latency": review_mean_latency,
        "review_resolution_time": review_resolution_time,
    }


def export_features(project, features):
    pd.DataFrame(features).sort_values(["pull_number"]).to_csv(get_path("features", project), index=False)


def measure_features(project):
    logger = get_logger(__file__)
    logger.info(f"{project}: Measuring features")
    dataset = import_dataset(project)
    pulls = import_pulls(project)
    patches = import_patches(project)
    with joblib.Parallel(n_jobs=-1, prefer="threads", verbose=1) as parallel:
        export_features(
            project,
            parallel(
                joblib.delayed(measure_pull)(project, dataset, pulls, patches, pull_number)
                for pull_number in dataset.index.unique("pull_number")
            ),
        )


def main():
    projects = []
    for project in postprocessed():
        if cleanup_files("features", force_refresh(), project):
            projects.append(project)
        else:
            print(f"Skip measuring features for project {project}")
    for project in projects:
        measure_features(project)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop measuring features")
        exit(1)
