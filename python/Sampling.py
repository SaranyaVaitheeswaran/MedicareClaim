import pandas as pd

# keep original index
work_df = analysis_df.copy()

# -------- Part 1: balanced sample across provider type --------
target_balanced_n = 30000
group_col = "Rndrng_Prvdr_Type"

group_sizes = work_df[group_col].value_counts()
num_groups = group_sizes.shape[0]
base_n_per_group = target_balanced_n // num_groups

balanced_parts = []

for provider_type, group in work_df.groupby(group_col):
    take_n = min(base_n_per_group, len(group))
    balanced_parts.append(group.sample(n=take_n, random_state=42))

balanced_df = pd.concat(balanced_parts)

remaining_needed = target_balanced_n - len(balanced_df)

if remaining_needed > 0:
    remaining_pool = work_df.loc[~work_df.index.isin(balanced_df.index)]
    extra_balanced = remaining_pool.sample(n=remaining_needed, random_state=42)
    balanced_df = pd.concat([balanced_df, extra_balanced])

# -------- Part 2: anomaly-focused high-gap sample --------
remaining_df = work_df.loc[~work_df.index.isin(balanced_df.index)].copy()

high_gap_df = remaining_df.sort_values("Gap_Ratio", ascending=False).head(10000)

# -------- Final combine --------
final_40k_df = pd.concat([balanced_df, high_gap_df]).drop_duplicates()

print("Balanced sample:", balanced_df.shape)
print("High-gap sample:", high_gap_df.shape)
print("Final sample:", final_40k_df.shape)

final_40k_df.to_csv("medical_final_40k_stratified_anomaly.csv", index=False)
