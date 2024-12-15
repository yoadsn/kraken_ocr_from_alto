import os
from argparse import ArgumentParser

from dotenv import load_dotenv

from storage.corpus_manifest import (
    generate_corpus_manifest,
    report_manifest_stats,
    upload_manifest_to_blob_storage,
    default_manifest_file,
)
from storage.processed_manifest import (
    cleanup_processed_manifest,
    upload_processed_manifest,
    default_processed_manifest_file,
)

load_dotenv()

parser = ArgumentParser(
    description="Generate and process the corpus manifest - Based on METS files"
)
# commands are: generate
# flags are: force_refresh (default false), upload (default true)
# allow overriding the default manifest filename (corpus.manifest.txt)
parser.add_argument(
    "command",
    type=str,
    help="Command to run.",
    choices=["generate", "upload", "report", "cleanup-processed"],
)
parser.add_argument(
    "--force_refresh",
    action="store_true",
    help="Force refresh the manifest file. Default: False",
)
parser.add_argument(
    "--skip_upload",
    action="store_true",
    help="Skip uploading newly generated manifest file to Azure Blob Storage. Default: False",
)
parser.add_argument(
    "--corpus_manifest_file",
    type=str,
    default=default_manifest_file,
    help="The name of the manifest file. Default: corpus.manifest.txt",
)
parser.add_argument(
    "--processed_manifest_file",
    type=str,
    default=default_processed_manifest_file,
    help="The name of the manifest file. Default: corpus.manifest.txt",
)

if __name__ == "__main__":
    args = parser.parse_args()

    should_upload = False
    if args.command == "generate":
        # Check if the manifest file already exists
        corpus_manifest_file_name = args.corpus_manifest_file
        if os.path.exists(corpus_manifest_file_name) and not args.force_refresh:
            print(
                f"Manifest file '{corpus_manifest_file_name}' already exists. Skipping generation."
            )
        else:
            print(f"Generating manifest file: {corpus_manifest_file_name}")
            generate_corpus_manifest(corpus_manifest_file_name)
            upload_manifest_to_blob_storage(args.corpus_manifest_file)
    elif args.command == "upload":
        upload_manifest_to_blob_storage(args.corpus_manifest_file)
        upload_processed_manifest(args.processed_manifest_file)
    elif args.command == "report":
        report_manifest_stats(args.corpus_manifest_file)
    elif args.command == "cleanup-processed":
        cleanup_processed_manifest(args.processed_manifest_file)
    else:
        print(f"Unknown command '{args.command}'")
        parser.print_help()
        exit(1)
