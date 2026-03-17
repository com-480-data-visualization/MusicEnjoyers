import pandas as pd

df = pd.read_csv("billboard_hot100_2024_monthly_enriched.csv")

avg_ms = df["duration_ms"].mean()
avg_minutes = avg_ms / 60000

print(f"Average duration: {avg_ms:.2f} ms")
print(f"Average duration: {avg_minutes:.2f} minutes")

"""
2025:
# Average duration: 196732.28 ms
# Average duration: 3.28 minutes

#2015:
# Average duration: 219241.67 ms
# Average duration: 3.65 minutes

"""
