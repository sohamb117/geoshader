"""
Person A - Week 2: Fetch geopolitical events from GDELT via Google BigQuery

Downloads GDELT events filtered by CAMEO codes for geopolitical conflicts,
threats, sanctions, and military actions.

GDELT VERSIONS:
---------------
This script supports both GDELT 1.0 and GDELT 2.0:

- GDELT 1.0: Historical data from 1979 to February 18, 2015
  - Table: gdelt-bq.full.events
  - Daily updates with ~300 event categories
  - Covers 35+ years of historical geopolitical events

- GDELT 2.0: Modern high-resolution data from February 19, 2015 onwards
  - Table: gdelt-bq.gdeltv2.events
  - 15-minute resolution updates
  - Enhanced coverage and more detailed event tracking

AVAILABLE FUNCTIONS:
-------------------
1. fetch_all_gdelt() - [DEFAULT] Intelligently fetches from both versions
   - Automatically splits queries at Feb 2015 boundary
   - Combines and deduplicates all events
   - Saves to: data/raw/gdelt_events_complete.parquet

2. test_gdelt_versions() - Test small samples from both versions
   - GDELT 1.0: 100 events from 2010-2013
   - GDELT 2.0: 100 events from 2020-2024
   - Saves to: data/raw/gdelt_test_sample.parquet

3. main() - Legacy single-query approach
   - Uses config date range without version splitting
   - Saves to: data/raw/gdelt_events.parquet

USAGE:
------
python data/fetch_gdelt.py  # Runs fetch_all_gdelt() by default
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from utils import load_config, load_secrets, setup_logging, save_dataset, create_event_id
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    logging.warning("google-cloud-bigquery not installed. Install with: pip install google-cloud-bigquery")


def build_gdelt1_query(cameo_codes: list, start_date: str, end_date: str, min_mentions: int = 5, limit: int = None) -> str:
    """
    Build query for GDELT 1.0 (1979 - Feb 2015).
    Uses the full.events table.
    """
    codes_str = ', '.join([f"'{code}'" for code in cameo_codes])
    start_date_int = int(start_date.replace('-', ''))
    end_date_int = int(end_date.replace('-', ''))

    limit_clause = f"LIMIT {limit}" if limit else ""

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
        `gdelt-bq.full.events`
    WHERE
        SQLDATE >= {start_date_int}
        AND SQLDATE <= {end_date_int}
        AND EventRootCode IN ({codes_str})
        AND NumMentions >= {min_mentions}
        AND Actor1Name IS NOT NULL
        AND Actor2Name IS NOT NULL
    ORDER BY
        SQLDATE DESC
    {limit_clause}
    """
    return query


def build_gdelt2_query(cameo_codes: list, start_date: str, end_date: str, min_mentions: int = 5, limit: int = None) -> str:
    """
    Build query for GDELT 2.0 (Feb 2015 onwards).
    Uses the gdeltv2.events table with 15-minute resolution.
    """
    codes_str = ', '.join([f"'{code}'" for code in cameo_codes])
    start_date_int = int(start_date.replace('-', ''))
    end_date_int = int(end_date.replace('-', ''))

    limit_clause = f"LIMIT {limit}" if limit else ""

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
    {limit_clause}
    """
    return query


def build_gdelt_query(cameo_codes: list, start_date: str, end_date: str, min_mentions: int = 5) -> str:
    """Legacy function - kept for backwards compatibility."""
    codes_str = ', '.join([f"'{code}'" for code in cameo_codes])
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
        `gdelt-bq.full.events_partitioned`
    WHERE
        _PARTITIONTIME >= TIMESTAMP('{start_date}')
        AND _PARTITIONTIME <= TIMESTAMP('{end_date}')
        AND EventRootCode IN ({codes_str})
        AND NumMentions >= {min_mentions}
        AND Actor1Name IS NOT NULL
        AND Actor2Name IS NOT NULL
    ORDER BY
        SQLDATE DESC
    """
    return query


def fetch_and_write_streaming(project_id: str, query: str, output_path: Path,
                              chunk_size: int = 50000, gdelt_version: str = None) -> int:
    """
    Fetch BigQuery results in chunks and write incrementally to parquet.

    Args:
        project_id: Google Cloud project ID
        query: SQL query to execute
        output_path: Path to output parquet file
        chunk_size: Number of rows to fetch per chunk
        gdelt_version: GDELT version string to add to data

    Returns:
        Total number of rows written
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required")

    client = bigquery.Client(project=project_id)

    logging.info("Starting streaming query execution...")
    query_job = client.query(query)

    # Use result iterator with page size
    rows_iter = query_job.result(page_size=chunk_size)

    writer = None
    total_rows = 0
    chunk_num = 0

    try:
        for page in rows_iter.pages:
            chunk_num += 1

            # Convert page to DataFrame
            rows_data = [dict(row.items()) for row in page]
            if not rows_data:
                continue

            df_chunk = pd.DataFrame(rows_data)

            # Convert date from int to datetime
            if 'date' in df_chunk.columns and df_chunk['date'].dtype == 'int64':
                df_chunk['date'] = pd.to_datetime(df_chunk['date'].astype(str), format='%Y%m%d')

            # Add GDELT version if specified
            if gdelt_version:
                df_chunk['gdelt_version'] = gdelt_version

            # Add metadata to each chunk immediately
            df_chunk = add_event_metadata(df_chunk)

            # Convert to PyArrow table
            table = pa.Table.from_pandas(df_chunk)

            # Write to parquet (append mode)
            if writer is None:
                # First chunk - create writer
                writer = pq.ParquetWriter(output_path, table.schema)
                logging.info(f"Created parquet file: {output_path}")

            writer.write_table(table)
            total_rows += len(df_chunk)

            if chunk_num % 10 == 0:
                logging.info(f"Processed {chunk_num} chunks, {total_rows:,} rows written...")

        logging.info(f"✓ Streaming complete: {total_rows:,} total rows written")

    finally:
        if writer is not None:
            writer.close()

    return total_rows


def deduplicate_parquet_file(input_path: Path, output_path: Path, chunk_size: int = 100000) -> int:
    """
    Deduplicate a parquet file by reading it, deduplicating, and writing to a new file.

    Note: Deduplication requires loading the full dataset into memory to identify duplicates.
    However, we can read and write in chunks to minimize peak memory usage.

    Args:
        input_path: Path to input parquet file
        output_path: Path to output deduplicated parquet file
        chunk_size: Chunk size for reading

    Returns:
        Number of rows in deduplicated file
    """
    logging.info(f"Deduplicating {input_path.name}...")

    # Read the entire file (needed for deduplication)
    # For very large files, consider using dask or similar for out-of-core processing
    df = pd.read_parquet(input_path)
    logging.info(f"Before deduplication: {len(df):,} rows")

    # Deduplicate
    df = deduplicate_events(df)
    logging.info(f"After deduplication: {len(df):,} rows")

    # Write deduplicated file
    df.to_parquet(output_path, index=False)

    return len(df)


def fetch_gdelt1_full(project_id: str, cameo_codes: list, start_date: str,
                      end_date: str, min_mentions: int = 5) -> pd.DataFrame:
    """
    Fetch full dataset from GDELT 1.0 (1979-2015).

    Args:
        project_id: Google Cloud project ID
        cameo_codes: List of CAMEO root codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_mentions: Minimum mentions threshold

    Returns:
        DataFrame with all GDELT 1.0 events in date range
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required")

    logging.info("\n=== Fetching GDELT 1.0 Full Dataset ===")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"CAMEO codes: {cameo_codes}")
    logging.info(f"Min mentions: {min_mentions}")
    logging.info("⚠️  This may take several minutes and process significant data...")

    client = bigquery.Client(project=project_id)
    query = build_gdelt1_query(cameo_codes, start_date, end_date, min_mentions, limit=None)

    try:
        df = client.query(query).to_dataframe()
        logging.info(f"✓ Retrieved {len(df):,} events from GDELT 1.0")

        # Convert date from int to datetime
        if len(df) > 0 and df['date'].dtype == 'int64':
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')

        return df
    except Exception as e:
        logging.error(f"GDELT 1.0 query failed: {e}")
        raise


def fetch_gdelt1_sample(project_id: str, cameo_codes: list, start_date: str,
                        end_date: str, min_mentions: int = 5, limit: int = 100) -> pd.DataFrame:
    """
    Fetch sample from GDELT 1.0 (pre-2015).

    Args:
        project_id: Google Cloud project ID
        cameo_codes: List of CAMEO root codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_mentions: Minimum mentions threshold
        limit: Maximum number of rows to return

    Returns:
        DataFrame with GDELT 1.0 events
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required")

    logging.info("\n=== Fetching GDELT 1.0 Sample (Pre-2015) ===")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"CAMEO codes: {cameo_codes}")
    logging.info(f"Limit: {limit} events")

    client = bigquery.Client(project=project_id)
    query = build_gdelt1_query(cameo_codes, start_date, end_date, min_mentions, limit)

    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Retrieved {len(df)} events from GDELT 1.0")

        # Convert date from int to datetime
        if len(df) > 0 and df['date'].dtype == 'int64':
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')

        return df
    except Exception as e:
        logging.error(f"GDELT 1.0 query failed: {e}")
        raise


def fetch_gdelt2_full(project_id: str, cameo_codes: list, start_date: str,
                      end_date: str, min_mentions: int = 5) -> pd.DataFrame:
    """
    Fetch full dataset from GDELT 2.0 (2015 onwards).

    Args:
        project_id: Google Cloud project ID
        cameo_codes: List of CAMEO root codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_mentions: Minimum mentions threshold

    Returns:
        DataFrame with all GDELT 2.0 events in date range
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required")

    logging.info("\n=== Fetching GDELT 2.0 Full Dataset ===")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"CAMEO codes: {cameo_codes}")
    logging.info(f"Min mentions: {min_mentions}")
    logging.info("⚠️  This may take several minutes and process significant data...")

    client = bigquery.Client(project=project_id)
    query = build_gdelt2_query(cameo_codes, start_date, end_date, min_mentions, limit=None)

    try:
        df = client.query(query).to_dataframe()
        logging.info(f"✓ Retrieved {len(df):,} events from GDELT 2.0")

        # Convert date from int to datetime
        if len(df) > 0 and df['date'].dtype == 'int64':
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')

        return df
    except Exception as e:
        logging.error(f"GDELT 2.0 query failed: {e}")
        raise


def fetch_gdelt2_sample(project_id: str, cameo_codes: list, start_date: str,
                        end_date: str, min_mentions: int = 5, limit: int = 100) -> pd.DataFrame:
    """
    Fetch sample from GDELT 2.0 (2015 onwards).

    Args:
        project_id: Google Cloud project ID
        cameo_codes: List of CAMEO root codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_mentions: Minimum mentions threshold
        limit: Maximum number of rows to return

    Returns:
        DataFrame with GDELT 2.0 events
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required")

    logging.info("\n=== Fetching GDELT 2.0 Sample (2015+) ===")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"CAMEO codes: {cameo_codes}")
    logging.info(f"Limit: {limit} events")

    client = bigquery.Client(project=project_id)
    query = build_gdelt2_query(cameo_codes, start_date, end_date, min_mentions, limit)

    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Retrieved {len(df)} events from GDELT 2.0")

        # Convert date from int to datetime
        if len(df) > 0 and df['date'].dtype == 'int64':
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')

        return df
    except Exception as e:
        logging.error(f"GDELT 2.0 query failed: {e}")
        raise


def fetch_gdelt_bigquery(project_id: str, cameo_codes: list, start_date: str,
                         end_date: str = None, min_mentions: int = 5) -> pd.DataFrame:
    """
    Fetch GDELT events from Google BigQuery (legacy function).

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


def test_gdelt_versions():
    """Test both GDELT 1.0 and 2.0 with small samples."""
    setup_logging()
    config = load_config()
    secrets = load_secrets()

    project_id = secrets.get('google_cloud_project_id')
    if not project_id or not BIGQUERY_AVAILABLE:
        logging.error("BigQuery not configured. Please set google_cloud_project_id in config/secrets.yaml")
        return

    cameo_codes = config['gdelt']['cameo_root_codes']
    min_mentions = config['gdelt']['min_mentions']

    logging.info("\n" + "="*60)
    logging.info("TESTING GDELT 1.0 vs GDELT 2.0")
    logging.info("="*60)

    all_events = []

    # Test GDELT 1.0 (pre-2014 sample)
    try:
        logging.info("\n1. Testing GDELT 1.0 (Historical Data: 1979-2014)")
        gdelt1_df = fetch_gdelt1_sample(
            project_id=project_id,
            cameo_codes=cameo_codes,
            start_date='2010-01-01',
            end_date='2013-12-31',
            min_mentions=min_mentions,
            limit=100
        )

        if len(gdelt1_df) > 0:
            gdelt1_df['gdelt_version'] = '1.0'
            all_events.append(gdelt1_df)

            logging.info(f"\nGDELT 1.0 Sample Statistics:")
            logging.info(f"  Events retrieved: {len(gdelt1_df)}")
            logging.info(f"  Date range: {gdelt1_df['date'].min()} to {gdelt1_df['date'].max()}")
            logging.info(f"  Top actors: {gdelt1_df['actor1'].value_counts().head(3).to_dict()}")
        else:
            logging.warning("No events found in GDELT 1.0")

    except Exception as e:
        logging.error(f"GDELT 1.0 test failed: {e}")

    # Test GDELT 2.0 (2015+ sample)
    try:
        logging.info("\n2. Testing GDELT 2.0 (Modern Data: 2015+)")
        gdelt2_df = fetch_gdelt2_sample(
            project_id=project_id,
            cameo_codes=cameo_codes,
            start_date='2020-01-01',
            end_date='2024-12-31',
            min_mentions=min_mentions,
            limit=100
        )

        if len(gdelt2_df) > 0:
            gdelt2_df['gdelt_version'] = '2.0'
            all_events.append(gdelt2_df)

            logging.info(f"\nGDELT 2.0 Sample Statistics:")
            logging.info(f"  Events retrieved: {len(gdelt2_df)}")
            logging.info(f"  Date range: {gdelt2_df['date'].min()} to {gdelt2_df['date'].max()}")
            logging.info(f"  Top actors: {gdelt2_df['actor1'].value_counts().head(3).to_dict()}")
        else:
            logging.warning("No events found in GDELT 2.0")

    except Exception as e:
        logging.error(f"GDELT 2.0 test failed: {e}")

    # Combine and analyze
    if all_events:
        combined_df = pd.concat(all_events, ignore_index=True)

        logging.info("\n" + "="*60)
        logging.info("COMPARISON SUMMARY")
        logging.info("="*60)

        version_counts = combined_df.groupby('gdelt_version').size()
        logging.info(f"\nEvents by version:")
        for version, count in version_counts.items():
            logging.info(f"  GDELT {version}: {count} events")

        # Process and save combined sample
        combined_df = deduplicate_events(combined_df)
        combined_df = add_event_metadata(combined_df)

        save_dataset(combined_df, 'gdelt_test_sample', processed=False)
        logging.info(f"\nSaved {len(combined_df)} test events to data/raw/gdelt_test_sample.parquet")

        logging.info("\n" + "="*60)
        logging.info("TEST COMPLETE - Both GDELT versions working!")
        logging.info("="*60)
    else:
        logging.error("Failed to retrieve data from either GDELT version")


def fetch_all_gdelt():
    """Fetch complete GDELT 1.0 and 2.0 datasets and write incrementally to parquet."""
    setup_logging()
    config = load_config()
    secrets = load_secrets()

    project_id = secrets.get('google_cloud_project_id')
    if not project_id or not BIGQUERY_AVAILABLE:
        logging.error("BigQuery not configured. Please set google_cloud_project_id in config/secrets.yaml")
        return

    cameo_codes = config['gdelt']['cameo_root_codes']
    min_mentions = config['gdelt']['min_mentions']
    chunk_size = config['gdelt'].get('chunk_size', 50000)  # Configurable chunk size

    # Get date range from config
    config_start_date = config['start_date']
    config_end_date = config['end_date'] or datetime.today().strftime('%Y-%m-%d')

    logging.info("\n" + "="*80)
    logging.info("FETCHING COMPLETE GDELT 1.0 AND 2.0 DATASETS (STREAMING MODE)")
    logging.info("="*80)
    logging.info(f"Overall date range: {config_start_date} to {config_end_date}")
    logging.info(f"CAMEO codes: {cameo_codes}")
    logging.info(f"Min mentions: {min_mentions}")
    logging.info(f"Chunk size: {chunk_size:,} rows")

    # Output paths
    output_dir = Path(__file__).parent / 'raw'
    output_dir.mkdir(parents=True, exist_ok=True)

    temp_gdelt1_path = output_dir / 'gdelt_events_v1_temp.parquet'
    temp_gdelt2_path = output_dir / 'gdelt_events_v2_temp.parquet'
    final_output_path = output_dir / 'gdelt_events_complete.parquet'

    # GDELT 1.0 cutoff date (GDELT 2.0 starts Feb 19, 2015)
    gdelt1_end = '2015-02-18'
    gdelt2_start = '2015-02-19'

    # Determine which datasets to fetch based on date range
    fetch_gdelt1 = config_start_date < gdelt2_start
    fetch_gdelt2 = config_end_date >= gdelt2_start

    total_rows_gdelt1 = 0
    total_rows_gdelt2 = 0

    # Fetch GDELT 1.0 if needed
    if fetch_gdelt1:
        try:
            gdelt1_start = config_start_date
            gdelt1_end_actual = min(config_end_date, gdelt1_end)

            logging.info(f"\n{'='*80}")
            logging.info(f"STEP 1: Fetching GDELT 1.0 ({gdelt1_start} to {gdelt1_end_actual})")
            logging.info(f"{'='*80}")

            query = build_gdelt1_query(
                cameo_codes=cameo_codes,
                start_date=gdelt1_start,
                end_date=gdelt1_end_actual,
                min_mentions=min_mentions,
                limit=None
            )

            total_rows_gdelt1 = fetch_and_write_streaming(
                project_id=project_id,
                query=query,
                output_path=temp_gdelt1_path,
                chunk_size=chunk_size,
                gdelt_version='1.0'
            )

            logging.info(f"✓ GDELT 1.0: {total_rows_gdelt1:,} events written to {temp_gdelt1_path}")

        except Exception as e:
            logging.error(f"GDELT 1.0 fetch failed: {e}")
            logging.warning("Continuing without GDELT 1.0 data...")

    # Fetch GDELT 2.0 if needed
    if fetch_gdelt2:
        try:
            gdelt2_start_actual = max(config_start_date, gdelt2_start)
            gdelt2_end = config_end_date

            logging.info(f"\n{'='*80}")
            logging.info(f"STEP 2: Fetching GDELT 2.0 ({gdelt2_start_actual} to {gdelt2_end})")
            logging.info(f"{'='*80}")

            query = build_gdelt2_query(
                cameo_codes=cameo_codes,
                start_date=gdelt2_start_actual,
                end_date=gdelt2_end,
                min_mentions=min_mentions,
                limit=None
            )

            total_rows_gdelt2 = fetch_and_write_streaming(
                project_id=project_id,
                query=query,
                output_path=temp_gdelt2_path,
                chunk_size=chunk_size,
                gdelt_version='2.0'
            )

            logging.info(f"✓ GDELT 2.0: {total_rows_gdelt2:,} events written to {temp_gdelt2_path}")

        except Exception as e:
            logging.error(f"GDELT 2.0 fetch failed: {e}")
            logging.warning("Continuing without GDELT 2.0 data...")

    # Combine temporary files if both were created
    if total_rows_gdelt1 > 0 or total_rows_gdelt2 > 0:
        logging.info(f"\n{'='*80}")
        logging.info("COMBINING AND PROCESSING ALL EVENTS")
        logging.info(f"{'='*80}")

        # Deduplicate each version file first
        dedup_files = []

        if temp_gdelt1_path.exists():
            logging.info("\nDeduplicating GDELT 1.0 data...")
            dedup_v1_path = output_dir / 'gdelt_events_v1_dedup.parquet'
            deduplicate_parquet_file(temp_gdelt1_path, dedup_v1_path, chunk_size)
            dedup_files.append(dedup_v1_path)
            temp_gdelt1_path.unlink()  # Remove temp file

        if temp_gdelt2_path.exists():
            logging.info("\nDeduplicating GDELT 2.0 data...")
            dedup_v2_path = output_dir / 'gdelt_events_v2_dedup.parquet'
            deduplicate_parquet_file(temp_gdelt2_path, dedup_v2_path, chunk_size)
            dedup_files.append(dedup_v2_path)
            temp_gdelt2_path.unlink()  # Remove temp file

        # Combine deduplicated files
        if len(dedup_files) == 1:
            # Only one version, just rename it
            logging.info("Only one GDELT version fetched, using as final file...")
            dedup_files[0].rename(final_output_path)
            df_final = pd.read_parquet(final_output_path)

        else:
            # Combine both versions
            logging.info("\nCombining GDELT 1.0 and 2.0 deduplicated files...")
            combined_writer = None
            total_final_rows = 0

            try:
                for pf in dedup_files:
                    logging.info(f"Reading {pf.name}...")
                    parquet_file = pq.ParquetFile(pf)

                    for batch in parquet_file.iter_batches(batch_size=chunk_size):
                        table = batch

                        # Write to final parquet
                        if combined_writer is None:
                            combined_writer = pq.ParquetWriter(final_output_path, table.schema)

                        combined_writer.write_table(table)
                        total_final_rows += len(table)

            finally:
                if combined_writer is not None:
                    combined_writer.close()

            logging.info(f"✓ Combined {total_final_rows:,} rows")

            # Clean up deduplicated temp files
            for pf in dedup_files:
                pf.unlink()

            # Final deduplication across versions (in case same event in both)
            logging.info("\nFinal deduplication across GDELT versions...")
            temp_combined = output_dir / 'gdelt_events_combined_temp.parquet'
            final_output_path.rename(temp_combined)
            deduplicate_parquet_file(temp_combined, final_output_path, chunk_size)
            temp_combined.unlink()

            df_final = pd.read_parquet(final_output_path)

        # Validate
        validate_gdelt_data(df_final, config)

        logging.info(f"\n{'='*80}")
        logging.info("FETCH COMPLETE")
        logging.info(f"{'='*80}")
        logging.info(f"✓ Saved {len(df_final):,} events to {final_output_path}")
        logging.info(f"✓ Date range: {df_final['date'].min()} to {df_final['date'].max()}")

        # Get actual file size
        file_size_mb = final_output_path.stat().st_size / (1024 * 1024)
        logging.info(f"✓ File size: {file_size_mb:.2f} MB")

    else:
        logging.error("Failed to retrieve data from any GDELT version")


def main():
    """Main execution function - legacy compatibility."""
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
    # Fetch complete GDELT 1.0 and 2.0 datasets
    # This will intelligently fetch from both versions based on config date range
    fetch_all_gdelt()

    # Alternative functions:
    # test_gdelt_versions()  # Run small test samples
    # main()                 # Legacy single-query approach
