import pandas as pd

new_groupings = pd.read_csv("groupings.csv")
try:
    old_groupings = pd.read_csv("groupings_old.csv")
except:
    print("no old groupings found")
    exit(-1)

groupings_delta = new_groupings[
    ~new_groupings.apply(tuple, 1).isin(old_groupings.apply(tuple, 1))
].copy()


groupings_delta.to_csv("groupings_delta.csv", index=False)
print(groupings_delta)
