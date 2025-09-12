"""
Gas Data Downloader for SciGRID_gas IGGIELGN dataset

Downloads the required CSV files for gas network validation from Zenodo.
Based on download_SciGRID_gas_data() from eGon-data.
"""

import zipfile
import logging
from pathlib import Path
from urllib.request import urlretrieve

logger = logging.getLogger(__name__)


def download_scigreid_gas_data(data_dir: Path = None):
    """
    Download SciGRID_gas IGGIELGN data from Zenodo

    Downloads the following CSV files into the data directory:
    * IGGIELGN_Nodes.csv (gas network nodes/buses)
    * IGGIELGN_PipeSegments.csv (gas pipelines)
    * IGGIELGN_Productions.csv (gas production sites)
    * IGGIELGN_Storages.csv (gas storage facilities)
    * IGGIELGN_LNGs.csv (LNG terminals)

    Based on eGon-data download_SciGRID_gas_data() function.
    Source: https://zenodo.org/record/4767098

    Parameters
    ----------
    data_dir : Path, optional
        Directory to download data to. Defaults to ./gas_data/

    Returns
    -------
    Path
        Path to the directory containing extracted CSV files
    """
    if data_dir is None:
        data_dir = Path(".") / "gas_data"

    data_dir.mkdir(parents=True, exist_ok=True)

    basename = "IGGIELGN"
    zip_file = data_dir / f"{basename}.zip"
    extract_dir = data_dir / "data"

    zenodo_zip_file_url = f"https://zenodo.org/record/4767098/files/{basename}.zip"

    # Download if not already present
    if not zip_file.exists():
        logger.info(f"Downloading SciGRID_gas data from {zenodo_zip_file_url}")
        urlretrieve(zenodo_zip_file_url, zip_file)
        logger.info(f"Downloaded {zip_file}")
    else:
        logger.info(f"Using existing download: {zip_file}")

    # Extract CSV files
    extract_dir.mkdir(parents=True, exist_ok=True)

    components = ["Nodes", "PipeSegments", "Productions", "Storages", "LNGs"]

    expected_files = [extract_dir / f"IGGIELGN_{comp}.csv" for comp in components]

    # Check if already extracted
    if all(f.exists() for f in expected_files):
        logger.info("Gas CSV files already extracted")
        return extract_dir

    logger.info(f"Extracting gas data to {extract_dir}")
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        for comp in components:
            csv_filename = (
                f"data/IGGIELGN_{comp}.csv"  # Files are in data/ subdirectory
            )
            try:
                # Extract specific file
                zip_ref.extract(
                    csv_filename, extract_dir.parent
                )  # Extract to parent to preserve structure
                logger.debug(f"Extracted {csv_filename}")
            except KeyError:
                logger.warning(f"File {csv_filename} not found in zip archive")

    # Verify extraction
    extracted_files = []
    for expected_file in expected_files:
        if expected_file.exists():
            extracted_files.append(expected_file.name)
        else:
            logger.warning(f"Expected file not found: {expected_file}")

    logger.info(
        f"Successfully extracted {len(extracted_files)} gas data files: "
        f"{extracted_files}"
    )
    return extract_dir


def get_gas_data_dir():
    """Get the default gas data directory path"""
    return Path(".") / "gas_data" / "data"


def check_gas_data_available():
    """
    Check if gas data CSV files are available

    Returns
    -------
    bool
        True if all required CSV files exist
    """
    data_dir = get_gas_data_dir()

    required_files = ["IGGIELGN_Nodes.csv", "IGGIELGN_Productions.csv"]

    return all((data_dir / filename).exists() for filename in required_files)


def ensure_gas_data():
    """
    Ensure gas data is available, download if necessary

    Returns
    -------
    Path
        Path to gas data directory
    """
    if not check_gas_data_available():
        logger.info("Gas data not found, downloading...")
        return download_scigreid_gas_data()
    else:
        logger.debug("Gas data already available")
        return get_gas_data_dir()
