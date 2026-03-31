"""
Person A - Week 2: Fetch geopolitical events from GDELT via Google BigQuery

Downloads GDELT events filtered by CAMEO codes for geopolitical conflicts,
threats, sanctions, and military actions.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from utils import load_config, load_secrets, setup_logging, save_dataset, create_event_id

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    logging.warning("google-cloud-bigquery not installed. Install with: pip install google-cloud-bigquery")


def build_gdelt_query(cameo_codes: list, start_date: str, end_date: str, min_mentions: int = 5) -> str:
    """
    Build SQL query for GDELT BigQuery.

    Args:
        cameo_codes: List of CAMEO root codes (e.g., [13, 16, 17, 18, 19])
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_mentions: Minimum number of mentions to filter noise

    Returns:
        SQL query string
    """
    # Convert codes to strings for SQL
    codes_str = ', '.join([f"'{code}'" for code in cameo_codes])

    # GDELT date format is YYYYMMDD
    start_date_int = int(start_date.replace('-', ''))
    end_date_int = int(end_date.replace('-', ''))

    query = f"""
    SELECT
        SQLDATE as date,
        Actor1Name as actor1,
        Actor2Name as actor2,
        EventCode as event_code,
        EventRootCode as event_root_code,
        GoldsteinScale as goldstein_scale,
        NumMentions as num_mentions,
        AvgTone as avg_tone,
        SOURCEURL as source_url
    FROM
        `gdelt-bq.gdeltv2.events`
    WHERE
        SQLDATE >= {start_date_int}
        AND SQLDATE <= {end_date_int}
        AND EventRootCode IN ({codes_str})
        AND NumMentions >= {min_mentions}
        AND Actor1Name IS NOT NULL
        AND Actor2Name IS NOT NULL
    ORDER BY
        SQLDATE DESC
    """

    return query


def fetch_gdelt_bigquery(project_id: str, cameo_codes: list, start_date: str,
                         end_date: str = None, min_mentions: int = 5) -> pd.DataFrame:
    """
    Fetch GDELT events from Google BigQuery.

    Args:
        project_id: Google Cloud project ID
        cameo_codes: List of CAMEO root codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (default: today)
        min_mentions: Minimum mentions threshold

    Returns:
        DataFrame with GDELT events
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required. Install with: pip install google-cloud-bigquery")

    if end_date is None:
        end_date = datetime.today().strftime('%Y-%m-%d')

    logging.info("Fetching GDELT events from BigQuery...")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"CAMEO codes: {cameo_codes}")
    logging.info(f"Min mentions: {min_mentions}")

    # Create BigQuery client
    client = bigquery.Client(project=project_id)

    # Build and execute query
    query = build_gdelt_query(cameo_codes, start_date, end_date, min_mentions)

    logging.info("Executing BigQuery query...")
    logging.info("This may take a few minutes for large date ranges...")

    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        logging.error(f"BigQuery error: {e}")
        logging.error("\nPlease ensure:")
        logging.error("1. You have a Google Cloud project set up")
        logging.error("2. BigQuery API is enabled")
        logging.error("3. You have authentication configured (gcloud auth application-default login)")
        raise

    logging.info(f"Retrieved {len(df)} events from GDELT")

    return df


def create_sample_gdelt_data(cameo_codes: list, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Create sample GDELT data for testing when BigQuery is not available.

    Args:
        cameo_codes: List of CAMEO root codes
        start_date: Start date
        end_date: End date

    Returns:
        Sample DataFrame
    """
    logging.warning("Creating sample GDELT data (BigQuery not configured)")
    logging.warning("To use real data:")
    logging.warning("1. Set up Google Cloud project")
    logging.warning("2. Enable BigQuery API")
    logging.warning("3. Run: gcloud auth application-default login")
    logging.warning("4. Add project_id to config/secrets.yaml")

    # Create sample events for major geopolitical incidents
    sample_events = [
        # Russia-Ukraine conflict
        {'date': '20220224', 'actor1': 'RUSSIA', 'actor2': 'UKRAINE', 'event_code': '190',
         'event_root_code': '19', 'goldstein_scale': -10.0, 'num_mentions': 1000, 'avg_tone': -5.0,
         'source_url': 'https://example.com/russia-ukraine'},

        # US-China tensions
        {'date': '20220801', 'actor1': 'UNITED STATES', 'actor2': 'CHINA', 'event_code': '160',
         'event_root_code': '16', 'goldstein_scale': -5.0, 'num_mentions': 200, 'avg_tone': -3.0,
         'source_url': 'https://example.com/us-china'},

        # Iran sanctions
        {'date': '20200103', 'actor1': 'UNITED STATES', 'actor2': 'IRAN', 'event_code': '163',
         'event_root_code': '16', 'goldstein_scale': -7.0, 'num_mentions': 150, 'avg_tone': -4.0,
         'source_url': 'https://example.com/iran-sanctions'},

        # Afghanistan withdrawal
        {'date': '20210815', 'actor1': 'UNITED STATES', 'actor2': 'AFGHANISTAN', 'event_code': '171',
         'event_root_code': '17', 'goldstein_scale': -5.0, 'num_mentions': 300, 'avg_tone': -4.5,
         'source_url': 'https://example.com/afghanistan'},

        # North Korea missile tests
        {'date': '20220305', 'actor1': 'NORTH KOREA', 'actor2': 'SOUTH KOREA', 'event_code': '130',
         'event_root_code': '13', 'goldstein_scale': -5.0, 'num_mentions': 100, 'avg_tone': -3.0,
         'source_url': 'https://example.com/north-korea'},
    ]

    df = pd.DataFrame(sample_events)
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

    return df


def deduplicate_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate GDELT events by (date, actor1, actor2, event_code).

    Multiple news sources report the same event, creating duplicates.

    Args:
        df: GDELT events DataFrame

    Returns:
        Deduplicated DataFrame
    """
    logging.info(f"Deduplicating events... (before: {len(df)} events)")

    # Sort by num_mentions (keep highest mention count)
    df = df.sort_values('num_mentions', ascending=False)

    # Drop duplicates
    df = df.drop_duplicates(
        subset=['date', 'actor1', 'actor2', 'event_code'],
        keep='first'
    )

    logging.info(f"After deduplication: {len(df)} events")

    return df


def add_event_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add metadata and derived fields to events.

    Args:
        df: GDELT events DataFrame

    Returns:
        DataFrame with additional columns
    """
    df = df.copy()

    # Create unique event ID
    df['event_id'] = df.apply(create_event_id, axis=1)

    # Convert date to datetime if needed
    if df['date'].dtype == 'int64':
        df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')

    # Create event description for embedding
    df['event_description'] = (
        df['actor1'].fillna('') + ' ' +
        df['event_code'].astype(str) + ' ' +
        df['actor2'].fillna('')
    )

    # Map CAMEO codes to categories
    cameo_categories = {
        '13': 'Threaten',
        '16': 'Sanctions/Reduce Relations',
        '17': 'Coerce',
        '18': 'Assault',
        '19': 'Fight/Conflict'
    }
    df['event_category'] = df['event_root_code'].map(cameo_categories)

    return df


def validate_gdelt_data(df: pd.DataFrame, config: dict):
    """
    Validate GDELT data quality.

    Args:
        df: GDELT DataFrame
        config: Configuration dict
    """
    logging.info("\n=== GDELT Data Validation ===")
    logging.info(f"Total events: {len(df)}")
    logging.info(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    # Events by category
    logging.info("\nEvents by category:")
    category_counts = df.groupby('event_category').size().sort_values(ascending=False)
    for category, count in category_counts.items():
        logging.info(f"  {category}: {count}")

    # Check for minimum sample size
    min_threshold = 50
    low_count_categories = category_counts[category_counts < min_threshold]
    if len(low_count_categories) > 0:
        logging.warning(f"\nCategories with < {min_threshold} events:")
        for category, count in low_count_categories.items():
            logging.warning(f"  {category}: {count} (may be insufficient for ML)")

    # Events by year
    df['year'] = df['date'].dt.year
    yearly_counts = df.groupby('year').size()
    logging.info("\nEvents by year:")
    for year, count in yearly_counts.items():
        logging.info(f"  {year}: {count}")

    # Top actors
    logging.info("\nTop 10 Actor1:")
    for actor, count in df['actor1'].value_counts().head(10).items():
        logging.info(f"  {actor}: {count}")


def main():
    """Main execution function."""
    # Setup
    setup_logging()
    config = load_config()
    secrets = load_secrets()

    # Get BigQuery project ID
    project_id = secrets.get('google_cloud_project_id')
    if project_id and BIGQUERY_AVAILABLE:
        try:
            # Fetch from BigQuery
            events_df = fetch_gdelt_bigquery(
                project_id=project_id,
                cameo_codes=config['gdelt']['cameo_root_codes'],
                start_date=config['start_date'],
                end_date=config['end_date'],
                min_mentions=config['gdelt']['min_mentions']
            )
        except Exception as e:
            logging.error(f"BigQuery fetch failed: {e}")
            logging.info("Falling back to sample data")
            events_df = create_sample_gdelt_data(
                config['gdelt']['cameo_root_codes'],
                config['start_date'],
                config['end_date'] or datetime.today().strftime('%Y-%m-%d')
            )
    else:
        # Use sample data
        events_df = create_sample_gdelt_data(
            config['gdelt']['cameo_root_codes'],
            config['start_date'],
            config['end_date'] or datetime.today().strftime('%Y-%m-%d')
        )

    # Process events
    events_df = deduplicate_events(events_df)
    events_df = add_event_metadata(events_df)

    # Validate
    validate_gdelt_data(events_df, config)

    # Save
    save_dataset(events_df, 'gdelt_events', processed=False)

    logging.info("\n=== GDELT data fetch complete ===")
    logging.info(f"Saved {len(events_df)} events to data/raw/gdelt_events.parquet")


if __name__ == "__main__":
    main()
