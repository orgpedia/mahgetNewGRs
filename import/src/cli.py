#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

from command_core import CommandResult, build_default_context
from commands import get_commands
from local_env import load_local_env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mahget-newgr",
        description="Unified Maharashtra GR pipeline CLI.",
    )
    subparsers = parser.add_subparsers(dest="command_name", required=True)

    for command in get_commands():
        subparser = subparsers.add_parser(
            command.name,
            help=command.help,
            description=command.description or command.help,
        )
        command.configure_parser(subparser)
        subparser.set_defaults(_command=command)

    return parser


def run_from_args(args: argparse.Namespace) -> CommandResult:
    context = build_default_context()
    command = getattr(args, "_command")
    result = command.run(args=args, context=context)
    return result


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_from_args(args)

    output_stream = sys.stdout if result.success else sys.stderr
    if result.message:
        print(result.message, file=output_stream)

    if result.metrics:
        for key, value in sorted(result.metrics.items()):
            print(f"{key}: {value}", file=output_stream)

    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
