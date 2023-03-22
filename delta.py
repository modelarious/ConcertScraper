import pandas as pd

new_groupings = pd.read_csv("groupings.csv")
old_groupings = pd.read_csv("groupings_old.csv")

groupings_delta = new_groupings[
    ~new_groupings.apply(tuple, 1).isin(old_groupings.apply(tuple, 1))
].copy()


groupings_delta.to_csv("groupings_delta.csv", index=False)
print(groupings_delta)
