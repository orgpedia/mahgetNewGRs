.DEFAULT_GOAL := help

CLI := python3 import/src/cli.py

LEDGER_DIR ?= import/grinfo
SOURCE_DIR ?= import/websites/gr.maharashtra.gov.in
HF_REPO_PATH ?= $(HF_DATASET_REPO_PATH)
IMPORT_DIR ?=

ifeq ($(strip $(HF_REPO_PATH)),)
HF_REPO_PATH := LFS/mahGRs
endif

.PHONY: help baseline-ledger append-ledger backfill-lfs-path download-pdfs pdf-info validate status-readme import-pdfs daily weekly monthly job-gr-site job-wayback job-archive sync-hf

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  baseline-ledger  Build baseline ledger from mahgetGR + mahgetAllGR"
	@echo "  append-ledger    Append only new records from mahgetGR into ledger"
	@echo "  backfill-lfs-path Backfill ledger lfs_path from local LFS PDFs"
	@echo "  download-pdfs    Download null lfs_path PDFs, import in batches, sync HF"
	@echo "  pdf-info         Extract PDF metadata into ledger pdf_info"
	@echo "  validate         Validate yearly JSONL ledgers"
	@echo "  status-readme    Refresh README status table from ledger"
	@echo "  import-pdfs      Import local PDFs into LFS/pdfs and sync HF"
	@echo "  daily            Run daily workflow"
	@echo "  weekly           Run weekly workflow"
	@echo "  monthly          Run monthly workflow"
	@echo "  job-gr-site      Run gr-site job (set MODE=daily|weekly|monthly)"
	@echo "  job-wayback      Run wayback stage job"
	@echo "  job-archive      Run archive stage job"
	@echo "  sync-hf          Sync local artifacts with HF dataset (API upload/download)"

baseline-ledger:
	$(CLI) baseline-ledger --clean-output
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

append-ledger:
	$(CLI) append-ledger --mahgetgr-root mahgetGR --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

backfill-lfs-path:
	$(CLI) backfill-lfs-path --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

download-pdfs:
	$(CLI) download-pdfs --ledger-dir $(LEDGER_DIR) --hf-repo-path "$(HF_REPO_PATH)" --lfs-pdf-root "$(HF_REPO_PATH)/pdfs"
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

pdf-info:
	$(CLI) pdf-info --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

validate:
	$(CLI) validate-ledger --ledger-dir $(LEDGER_DIR)

status-readme:
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

import-pdfs:
	@if [ -z "$(IMPORT_DIR)" ] || [ -z "$(HF_REPO_PATH)" ]; then \
		echo "Usage: make import-pdfs IMPORT_DIR=/path/to/pdfs HF_REPO_PATH=/path/to/local/lfs-data-root"; \
		exit 2; \
	fi
	$(CLI) import-pdfs --source-dir "$(IMPORT_DIR)" --ledger-dir $(LEDGER_DIR) --hf-repo-path "$(HF_REPO_PATH)"
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

daily:
	$(CLI) daily --ledger-dir $(LEDGER_DIR) --source-dir $(SOURCE_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

weekly:
	$(CLI) weekly --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

monthly:
	$(CLI) monthly --ledger-dir $(LEDGER_DIR) --source-dir $(SOURCE_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

MODE ?= daily
job-gr-site:
	$(CLI) job-gr-site --mode $(MODE) --ledger-dir $(LEDGER_DIR) --source-dir $(SOURCE_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

job-wayback:
	$(CLI) job-wayback --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

job-archive:
	$(CLI) job-archive --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

sync-hf:
	$(CLI) sync-hf --source-root . --hf-repo-path "$(HF_REPO_PATH)"
