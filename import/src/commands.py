#!/usr/bin/env python3

from __future__ import annotations

import argparse
from typing import Iterable

import archive_job
import append_ledger
import backfill_lfs_path
import build_baseline_ledger
import download_pdfs
import gr_site_job
import import_pdfs_job
import pdf_info_job
import readme_status
import sync_hf_job
import validate_ledger
import wayback_job
import workflow_commands
from command_core import Command, CommandContext, CommandResult


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
    description = "Validate import/grinfo ledger files for schema, state, and partition consistency."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return validate_ledger.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = validate_ledger.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "validate-ledger reported issues",
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


class ImportPdfsCommand(Command):
    name = "import-pdfs"
    help = "Import local PDFs into LFS/pdfs"
    description = "Import PDFs from a local directory into LFS/pdfs and optionally sync to Hugging Face."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return import_pdfs_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = import_pdfs_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "import-pdfs failed",
        )


class DownloadPdfsCommand(Command):
    name = "download-pdfs"
    help = "Download missing PDFs and batch-import"
    description = "Download ledger records with null lfs_path, import in batches, and sync to Hugging Face."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return download_pdfs.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = download_pdfs.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "download-pdfs failed",
        )


class PdfInfoCommand(Command):
    name = "pdf-info"
    help = "Extract PDF metadata into ledger"
    description = "Extract page/image/font/language/file-size metadata from local PDFs into record.pdf_info."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return pdf_info_job.configure_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = pdf_info_job.run_from_args(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "pdf-info failed",
        )


class DailyWorkflowCommand(Command):
    name = "daily"
    help = "Run daily workflow"
    description = "Run daily workflow: gr-site + wayback + archive."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return workflow_commands.configure_daily_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = workflow_commands.run_daily_workflow(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "daily workflow failed",
        )


class WeeklyWorkflowCommand(Command):
    name = "weekly"
    help = "Run weekly workflow"
    description = "Run weekly workflow: retry incomplete stages."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return workflow_commands.configure_weekly_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = workflow_commands.run_weekly_workflow(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "weekly workflow failed",
        )


class MonthlyWorkflowCommand(Command):
    name = "monthly"
    help = "Run monthly workflow"
    description = "Run monthly workflow: full discovery reconciliation only."

    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return workflow_commands.configure_monthly_parser(parser)

    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        exit_code = workflow_commands.run_monthly_workflow(args)
        return CommandResult(
            name=self.name,
            exit_code=exit_code,
            message="" if exit_code == 0 else "monthly workflow failed",
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
        AppendLedgerCommand(),
        BackfillLfsPathCommand(),
        ValidateLedgerCommand(),
        UpdateReadmeStatusCommand(),
        ImportPdfsCommand(),
        DownloadPdfsCommand(),
        PdfInfoCommand(),
        GRSiteJobCommand(),
        WaybackJobCommand(),
        ArchiveJobCommand(),
        DailyWorkflowCommand(),
        WeeklyWorkflowCommand(),
        MonthlyWorkflowCommand(),
        SyncHFCommand(),
    ]
