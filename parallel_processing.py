import argparse
import glob
import os
from os import path
import time
import xml.etree.ElementTree as ET
import re
import multiprocessing
import logging
from functools import partial
import math

from dotenv import load_dotenv
from threadpoolctl import threadpool_limits
from tqdm import tqdm
import pandas as pd
from kraken.lib import models
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import metrics, trace

from readers import build_mets, NewAltoReader
from ocr import ImageOCR
from storage.corpus_manifest import (
    load_manifest,
    get_total_in_corpus,
    default_manifest_file,
)
from storage.processed_manifest import (
    load_processed_manifest,
    append_to_processed_manifest,
    get_processed_count,
    default_processed_manifest_file,
)
from storage.local_data import download_corpus_subset, upload_corpus_results

load_dotenv()

instrument = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
if instrument:
    configure_azure_monitor(logger_name="processing_errors")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [PID: %(process)d] - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# add also a file logger
file_handler = logging.FileHandler("ocr_extraction.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - [PID: %(process)d] - %(levelname)s - %(message)s")
)
logging.getLogger().addHandler(file_handler)
processing_errors_logger = logging.getLogger("processing_errors")
processing_errors_logger.addHandler(file_handler)


# Control verbosity of the specific library
logging.getLogger("kraken").setLevel(logging.ERROR)
logging.getLogger("kraken.binarization").setLevel(logging.CRITICAL)
logging.getLogger("kraken.pageseg").setLevel(logging.CRITICAL)
logging.getLogger("kraken.rpred").setLevel(logging.CRITICAL)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.CRITICAL
)
logging.getLogger("azure").setLevel(logging.CRITICAL)

meter_provider = metrics.get_meter_provider()
ocr_meter = meter_provider.get_meter("ocr")
issue_counter = ocr_meter.create_counter("issue_count")
issue_duration = ocr_meter.create_histogram("issue_duration")
page_counter = ocr_meter.create_counter("page_count")
page_duration = ocr_meter.create_histogram("page_duration")
block_counter = ocr_meter.create_counter("block_count")
block_duration = ocr_meter.create_histogram("block_duration")
corpus_size_gauge = ocr_meter.create_gauge("corpus_size")
processed_gauge = ocr_meter.create_gauge("processed")
processed_pct_gauge = ocr_meter.create_gauge("processed_pct")


def list_mets_files(parent_dir):
    return sorted(
        list(glob.glob(path.join(parent_dir, "**/*-METS.xml"), recursive=True))
    )


def get_alto_pages(mets_path):
    return sorted(list(glob.glob(path.join(path.dirname(mets_path), "ALTO/*.xml"))))


def get_mets_save_name(mets_file):
    """
    Get CSV output file name according to path to METS file, taking the mets file name and prepending the newsletter name,
    assuming the newsletter name is in the alphabetical directory recursively containing the mets file:
    '/path/to/.../Davar/1957/01/01_01/19570101_01-METS.xml' -> 'Davar_19570101_01.csv'
    """
    full_path = path.abspath(path.dirname(mets_file)).replace(
        "\\", "/"
    )  # '/path/to/Davar/1957/01/02/...'
    first_alphabetic_dir = [
        d
        for d in reversed(full_path.split("/"))
        if re.match("[a-zA-Zא-ת]+", d) is not None
    ][
        0
    ]  # 'Davar'
    return (
        first_alphabetic_dir
        + "_"
        + path.basename(mets_file).replace(".xml", ".csv").replace("-METS", "")
    )  # 'Davar_19570101_01.csv'


def process_mets_files(
    mets_files,
    data_dir,
    output_dir,
    disable_tqdm,
    corpus_manifest_file,
    processed_manifest_file,
    checkpoint_processed_every,
    dry_run,
):
    logging.info(f"Started process with {len(mets_files)} files")
    logging.debug(f"files: {mets_files}")
    logging.getLogger("kraken").setLevel(logging.ERROR)
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("ocr_worker"):
        baseline_model = None  # vgsl.TorchVGSLModel.load_model('blla.mlmodel')
        model = models.load_any("model_best_9379_140624.mlmodel")
        ocr = ImageOCR(model=model, bw_threshold=150, baseline_model=baseline_model)

        def process_blocks(blocks, olr, page_file):
            for block in tqdm(
                blocks, disable=disable_tqdm, desc="process blocks of a page"
            ):
                # perf duration
                track_start_time = time.time()
                block["page_file"] = page_file
                try:
                    if dry_run:
                        block["ocr_text"] = "dry_run"
                    else:
                        block["ocr_text"] = "\n".join(
                            ocr.get_text(olr.get_image_for_block(block))
                        )
                except Exception as e:
                    processing_errors_logger.error("Error in text: ")
                    processing_errors_logger.exception(e)
                    block["ocr_text"] = ""

                block_duration.record(time.time() - track_start_time)
                block_counter.add(1)

        def process_alto_page(page):
            olr = NewAltoReader(page)
            blocks = olr.get_text_blocks()
            process_blocks(blocks, olr, page)
            return blocks

        processed_mets_files = []
        results_to_upload = []
        for mets_file in tqdm(mets_files, desc="process issue within a worker"):
            issue_track_start_time = time.time()
            mets_file_on_disk = os.path.join(
                data_dir, mets_file
            )  # where the file actually resides on disk
            try:
                output_file = os.path.join(output_dir, get_mets_save_name(mets_file))
                pages_results = []
                logging.debug(f"Processing mets: {mets_file}")
                pages = get_alto_pages(mets_file_on_disk)

                for pi, page in enumerate(
                    tqdm(pages, disable=disable_tqdm, desc="process pages of an issue")
                ):
                    page_track_start_time = time.time()
                    try:
                        logging.debug(f"Starting page {pi + 1} of {mets_file}")
                        page_results = process_alto_page(page)
                        pages_results.extend(page_results)
                        logging.debug(f"Done with page {pi + 1} of {mets_file}")
                    except Exception as e:
                        processing_errors_logger.error(
                            f"Failed to process page {pi + 1} in file {mets_file}"
                        )
                        processing_errors_logger.exception(e)
                    page_duration.record(time.time() - page_track_start_time)
                    page_counter.add(1)
                results_df = pd.DataFrame(pages_results)
                results_df = results_df.set_index("block_id")

                with open(mets_file_on_disk, "r", encoding="utf-8") as mets:
                    tree = ET.parse(mets)
                    mets_root = tree.getroot()
                    mets_data = build_mets(mets_root)

                    for article in mets_data:
                        ocr_texts = []
                        for begin in article["begins"]:
                            try:
                                block = results_df.loc[begin]
                                if not pd.isna(block["ocr_text"]):
                                    ocr_texts.append(block["ocr_text"])
                            except KeyError:
                                processing_errors_logger.error(
                                    f"Block {begin} was not found in data"
                                )
                        article["ocr_text"] = "\n".join(ocr_texts)

                if not os.path.exists(output_dir):
                    os.makedirs(output_dir, exist_ok=True)
                pd.DataFrame(mets_data).to_csv(output_file)
                results_to_upload.append(output_file)
                processed_mets_files.append(mets_file)

                if hasattr(meter_provider, "force_flush"):
                    metrics.get_meter_provider().force_flush()
            except Exception as e:
                processing_errors_logger.error(f"Failed to process file: {mets_file}")
                processing_errors_logger.exception(e)

            if len(processed_mets_files) >= checkpoint_processed_every:
                append_to_processed_manifest(
                    processed_mets_files, processed_manifest_file
                )
                upload_corpus_results(output_dir, results_to_upload)
                report_general_progress(corpus_manifest_file, processed_manifest_file)
                processed_mets_files = []
                results_to_upload = []

            issue_duration.record(time.time() - issue_track_start_time)
            issue_counter.add(1)

        if len(processed_mets_files) > 0:
            append_to_processed_manifest(processed_mets_files, processed_manifest_file)
            upload_corpus_results(output_dir, results_to_upload)


def chunk_list(data, chunk_size):
    """Divide the data into chunks of the given size."""
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def alto_dir_pipeline(
    data_dir,
    output_dir,
    corpus_manifest,
    corpus_manifest_file,
    processed_manifest,
    processed_manifest_file,
    skip_processed,
    disable_tqdm,
    max_files_to_process,
    num_processes,
    dry_run,
):
    initial_mets_files = corpus_manifest  # All files, including those processed already
    logging.info(
        f"Loading METS files (file => magazine issue with multiple pages) - found {len(initial_mets_files)} files in total."
    )
    mets_files = []  # Actual files to process
    for mets_file in initial_mets_files:
        if skip_processed and mets_file in processed_manifest:
            logging.debug(f"Skipping processed: {mets_file}")
            continue
        mets_files.append(mets_file)

    if not mets_files:
        logging.info("No files to process!")
        return

    logging.info(f"Left to process {len(mets_files)} files in total.")

    logging.info("Uploading any local results from previous unfinished runs")
    upload_corpus_results(output_dir)

    if len(mets_files) > max_files_to_process and max_files_to_process > 0:
        mets_files = mets_files[:max_files_to_process]
        logging.info(f"Limited to only process {len(mets_files)} files in total.")

    logging.info("Downloading required corpus data to process")
    download_corpus_subset(mets_files, data_dir, processed_manifest_file)

    mets_chunks = chunk_list(mets_files, math.ceil(len(mets_files) / num_processes))

    logging.info(f"Running with {num_processes} worker processes.")

    with multiprocessing.Pool(processes=num_processes) as pool:
        # Map the process function to items asynchronously
        process_with_args = partial(
            process_mets_files,
            data_dir=data_dir,
            output_dir=output_dir,
            disable_tqdm=disable_tqdm,
            corpus_manifest_file=corpus_manifest_file,
            processed_manifest_file=processed_manifest_file,
            checkpoint_processed_every=1,
            dry_run=dry_run,
        )

        result_async = pool.map_async(process_with_args, mets_chunks)
        # Block until all finish
        result_async.get()


def report_general_progress(
    corpus_manifest_file: str, processed_manifest_file: str, meters: dict = None
):
    total_in_corpus = get_total_in_corpus(corpus_manifest_file)
    processed_count = get_processed_count(processed_manifest_file)
    pct_done = processed_count * 100 / total_in_corpus

    corpus_size_gauge.set(total_in_corpus)
    processed_gauge.set(processed_count)
    processed_pct_gauge.set(pct_done)

    logging.info(
        f"Total In Corpus: {total_in_corpus} - Processed already {processed_count} ({pct_done:.2f}% done)"
    )


parser = argparse.ArgumentParser()
parser.add_argument("-i", "--data_dir", default="data")
parser.add_argument("-o", "--output_dir", default="output")
parser.add_argument("-l", "--max_files", required=False, default=16)
parser.add_argument("-s", "--skip_processed", default=True)
parser.add_argument("-n", "--num_processes", default=multiprocessing.cpu_count())
parser.add_argument("-c", "--corpus_manifest_file", default=default_manifest_file)
parser.add_argument(
    "-p", "--processed_manifest_file", default=default_processed_manifest_file
)
parser.add_argument("--disable_tqdm", action="store_true")
parser.add_argument("--dry_run", action="store_true")


if __name__ == "__main__":
    args = parser.parse_args()

    logging.info(f"Loading corpus manifest from {args.corpus_manifest_file}")
    corpus_manifest = load_manifest(args.corpus_manifest_file)
    logging.info(f"Loading processed manifest from {args.processed_manifest_file}")
    processed_manifest = load_processed_manifest(args.processed_manifest_file)

    report_general_progress(args.corpus_manifest_file, args.processed_manifest_file)

    with threadpool_limits(limits=1):  # each process will work on a single thred.
        alto_dir_pipeline(
            args.data_dir,
            args.output_dir,
            corpus_manifest,
            args.corpus_manifest_file,
            processed_manifest,
            args.processed_manifest_file,
            skip_processed=args.skip_processed,
            max_files_to_process=int(args.max_files),
            num_processes=args.num_processes,
            disable_tqdm=args.disable_tqdm,
            dry_run=args.dry_run,
        )
