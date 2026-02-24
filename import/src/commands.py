#!/usr/bin/env python3

from __future__ import annotations

import argparse
from typing import Iterable

import archive_job
import download_upload_pdfinfo_wrk
import download_pdf_job
import gr_site_job
import import_pdf_job
import pdf_info_job
import readme_status
import sync_hf_job
import validate_ledger
import wayback_job
from command_core import Command, CommandContext, CommandResult
from onetime import append_ledger, backfill_lfs_path, build_baseline_ledger, migrate_infos


class BaselineLedgerCommand(Command):
    name = "baseline-ledger"
    help = "Build baseline ledger from legacy datasets"
    description = (
        "Build import/grinfo yearly JSONL baseline ledger from mahgetGR and mahgetAllGR source datasets."
    )

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return build_baseline_ledger.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = build_baseline_ledger.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "baseline-ledger command failed",
        )


class ValidateLedgerCommand(Command):
    name = "validate-ledger"
    help = "Validate ledger JSONL files"
    description = "Validate split ledger files under import/{urlinfos,uploadinfos,pdfinfos}."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return validate_ledger.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = validate_ledger.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "validate-ledger reported issues",
        )


class MigrateInfosCommand(Command):
    name = "migrate-infos"
    help = "Split grinfo into urlinfos/uploadinfos/pdfinfos"
    description = "One-time migration from import/grinfo into split info ledgers."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return migrate_infos.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = migrate_infos.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "migrate-infos failed",
        )


class AppendLedgerCommand(Command):
    name = "append-ledger"
    help = "Append new records into ledger"
    description = "Append-only incremental ingestion from mahgetGR into existing import/grinfo ledger."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return append_ledger.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = append_ledger.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "append-ledger failed",
        )


class BackfillLfsPathCommand(Command):
    name = "backfill-lfs-path"
    help = "Backfill ledger lfs_path values"
    description = "Scan local LFS PDF roots and backfill top-level lfs_path for each ledger record."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return backfill_lfs_path.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = backfill_lfs_path.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "backfill-lfs-path failed",
        )


class GRSiteJobCommand(Command):
    name = "job-gr-site"
    help = "Run gr-site stage job"
    description = "Process crawled gr-site discovery records and reconcile them into ledger."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return gr_site_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = gr_site_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "job-gr-site failed",
        )


class WaybackJobCommand(Command):
    name = "job-wayback"
    help = "Run wayback stage job"
    description = "Run SPN2 Wayback stage for eligible records."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return wayback_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = wayback_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "job-wayback failed",
        )


class ArchiveJobCommand(Command):
    name = "job-archive"
    help = "Run archive stage job"
    description = "Run Archive upload stage for eligible records."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return archive_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = archive_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "job-archive failed",
        )


class ImportPdfJobCommand(Command):
    name = "job-import-pdf"
    help = "Run import-pdf stage job"
    description = "Upload downloaded PDFs referenced in ledger download.path to Hugging Face."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return import_pdf_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = import_pdf_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "job-import-pdf failed",
        )


class DownloadPdfJobCommand(Command):
    name = "job-download-pdf"
    help = "Run download-pdf stage job"
    description = "Run download-pdf stage for eligible records."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return download_pdf_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = download_pdf_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "job-download-pdf failed",
        )


class PdfInfoJobCommand(Command):
    name = "job-pdf-info"
    help = "Run pdf-info stage job"
    description = "Extract page/image/font/language/file-size metadata from local PDFs into record.pdf_info."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return pdf_info_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = pdf_info_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "job-pdf-info failed",
        )


class DownloadUploadPdfInfoWorkflowCommand(Command):
    name = "wrk-download-upload-pdfinfo"
    help = "Run batch workflow: download -> upload -> pdf-info"
    description = "Run partition-wise workflow: for each year, download PDFs, upload to HF, then compute pdf_info in batches."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return download_upload_pdfinfo_wrk.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = download_upload_pdfinfo_wrk.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "wrk-download-upload-pdfinfo failed",
        )


class SyncHFCommand(Command):
    name = "sync-hf"
    help = "Sync local artifacts with Hugging Face"
    description = "Sync local artifacts with Hugging Face dataset using API-based upload/download."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return sync_hf_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = sync_hf_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "sync-hf failed",
        )


class UpdateReadmeStatusCommand(Command):
    name = "update-readme-status"
    help = "Update README status table"
    description = "Compute repository status from ledger and update the README status section."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return readme_status.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = readme_status.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "update-readme-status failed",
        )


def get_commands() -> Iterable[Command]:
    return [
        BaselineLedgerCommand(),
        MigrateInfosCommand(),
        AppendLedgerCommand(),
        BackfillLfsPathCommand(),
        ValidateLedgerCommand(),
        UpdateReadmeStatusCommand(),
        ImportPdfJobCommand(),
        DownloadPdfJobCommand(),
        PdfInfoJobCommand(),
        DownloadUploadPdfInfoWorkflowCommand(),
        GRSiteJobCommand(),
        WaybackJobCommand(),
        ArchiveJobCommand(),
        SyncHFCommand(),
    ]
