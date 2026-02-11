#!/usr/bin/env python3

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import argparse


@dataclass(frozen=True)
class CommandContext:
    cwd: Path
    env: Mapping[str, str]
    now_utc: datetime


@dataclass
class CommandResult:
    name: str
    exit_code: int
    message: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.exit_code == 0


class Command(ABC):
    name: str = ""
    help: str = ""
    description: str = ""

    @abstractmethod
    def configure_parser(self, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        raise NotImplementedError

    @abstractmethod
    def run(self, args: argparse.Namespace, context: CommandContext) -> CommandResult:
        raise NotImplementedError


def build_default_context() -> CommandContext:
    return CommandContext(
        cwd=Path.cwd(),
        env=dict(os.environ),
        now_utc=datetime.now(timezone.utc),
    )
