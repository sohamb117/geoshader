
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

folder = Path("data/processed/rq1_pilot")
df = pd.read_csv(folder / "pilot_car_results.csv")

df = df.set_index("ticker").loc[["LMT", "NOC", "GD"]]

x = np.arange(len(df))
width = 0.35

fig, ax = plt.subplots(figsize=(8, 5))

bars1 = ax.bar(
    x - width / 2,
    df["CAR_0_1_pct"],
    width,
    label="CAR [0,+1]"
)

bars2 = ax.bar(
    x + width / 2,
    df["CAR_minus1_1_pct"],
    width,
    label="CAR [-1,+1]"
)

ax.bar_label(bars1, fmt="%.2f%%", padding=3)
ax.bar_label(bars2, fmt="%.2f%%", padding=3)

ax.set_xticks(x)
ax.set_xticklabels(df.index)
ax.set_ylabel("Cumulative Abnormal Log Return (%)")
ax.set_title(
    "Cross-Firm Abnormal Returns\n"
    "Russia-Ukraine Invasion, February 24, 2022"
)

ax.axhline(0, linewidth=0.8, color="gray")
ax.set_ylim(0, max(
    df["CAR_0_1_pct"].max(),
    df["CAR_minus1_1_pct"].max()
) * 1.25)

ax.legend()
fig.tight_layout()

output = folder / "rq1_pilot_car_comparison.png"
fig.savefig(output, dpi=300, bbox_inches="tight")

print(f"Figure saved to: {output}")
plt.show()
