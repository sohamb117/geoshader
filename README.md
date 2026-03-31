# Geopolitical Shocks and Defense Equity Returns

A Neural Event Study on how geopolitical events affect US defense company stock prices.

## Project Overview

This project uses machine learning to study how geopolitical events (wars, sanctions, arms deals, defense budget decisions) affect the stock prices of US defense companies. It employs a neural network instead of traditional OLS regression to find patterns that classical event study methods miss.

## Project Structure

```
geoshader/
├── data/
│   ├── raw/              # Raw downloaded data
│   ├── processed/        # Cleaned and processed data
│   ├── fetch_prices.py   # Download stock prices
│   ├── fetch_gdelt.py    # Download GDELT events
│   ├── fetch_macro.py    # Download macro indicators
│   └── compute_cars.py   # Compute Cumulative Abnormal Returns
├── models/
│   ├── mlp.py           # MLP architecture
│   ├── transformer.py   # Transformer architecture (optional)
│   ├── train.py         # Training loop
│   └── evaluate.py      # Evaluation metrics
├── analysis/
│   ├── eda.py           # Exploratory data analysis
│   ├── shap_analysis.py # SHAP value computation
│   └── visualizations.py # Plotting functions
├── notebooks/           # Jupyter notebooks for exploration
├── tests/              # Test suite
├── config/             # Configuration files
├── paper/              # LaTeX paper draft
└── requirements.txt    # Python dependencies
```

## Setup

1. Create environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Set up API keys:
   - Get FRED API key from fred.stlouisfed.org
   - Create `config/secrets.yaml` with your API key
   - Set up Google Cloud for BigQuery access

3. Run the data pipeline:
```bash
python data/fetch_prices.py
python data/fetch_gdelt.py
python data/fetch_macro.py
python data/compute_cars.py
```

## Defense Companies Tracked

- LMT (Lockheed Martin)
- RTX (Raytheon Technologies)
- NOC (Northrop Grumman)
- GD (General Dynamics)
- BA (Boeing)
- HII (Huntington Ingalls)
- LHX (L3Harris Technologies)
- KTOS (Kratos Defense)
- PLTR (Palantir Technologies)

## Timeline

- Weeks 1-3: Data Pipeline
- Weeks 4-5: Exploratory Data Analysis
- Weeks 6-10: Model Build and Training
- Weeks 11-12: SHAP Analysis
- Weeks 13-15: Paper Writing

## Citation

If you use this code, please cite:
```
[To be added upon publication]
```
