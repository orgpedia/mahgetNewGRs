.DEFAULT_GOAL := help

CLI := python3 import/src/cli.py
HF_REPO_PATH_DEFAULT := $(shell python3 -c "import sys; sys.path.insert(0, 'import/src'); from import_config import load_import_config; print(load_import_config().hf.dataset_repo_path)")

LEDGER_DIR ?= import/grinfo
SOURCE_DIR ?= import/websites/gr.maharashtra.gov.in
HF_REPO_PATH ?= $(HF_REPO_PATH_DEFAULT)
IMPORT_DIR ?=

ifeq ($(strip $(HF_REPO_PATH)),)
HF_REPO_PATH := LFS/mahGRs
endif

.PHONY: help baseline-ledger migrate-infos append-ledger backfill-lfs-path validate status-readme job-gr-site job-wayback job-archive job-download-pdf job-import-pdf job-pdf-info wrk-download-upload-pdfinfo sync-hf

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  baseline-ledger  Build baseline ledger from mahgetGR + mahgetAllGR"
	@echo "  migrate-infos    Split import/grinfo into urlinfos/uploadinfos/pdfinfos"
	@echo "  append-ledger    Append only new records from mahgetGR into ledger"
	@echo "  backfill-lfs-path Backfill ledger lfs_path from local LFS PDFs"
	@echo "  job-download-pdf Run download-pdf stage job"
	@echo "  job-import-pdf   Import local PDFs into LFS/pdfs and optionally sync HF"
	@echo "  job-pdf-info     Extract PDF metadata into ledger pdf_info"
	@echo "  wrk-download-upload-pdfinfo Run one batch: download -> upload -> pdf-info"
	@echo "  validate         Validate yearly JSONL ledgers"
	@echo "  status-readme    Refresh README status table from ledger"
	@echo "  job-gr-site      Run gr-site job (set MODE=daily|weekly|monthly)"
	@echo "  job-wayback      Run wayback stage job"
	@echo "  job-archive      Run archive stage job"
	@echo "  sync-hf          Sync local artifacts with HF dataset (API upload/download)"

baseline-ledger:
	$(CLI) baseline-ledger --clean-output
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

migrate-infos:
	$(CLI) migrate-infos --source-ledger-dir $(LEDGER_DIR)

append-ledger:
	$(CLI) append-ledger --mahgetgr-root mahgetGR --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

backfill-lfs-path:
	$(CLI) backfill-lfs-path --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

job-pdf-info:
	$(CLI) job-pdf-info --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

validate:
	$(CLI) validate-ledger --ledger-dir $(LEDGER_DIR)

status-readme:
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

job-import-pdf:
	@if [ -z "$(IMPORT_DIR)" ] || [ -z "$(HF_REPO_PATH)" ]; then \
		echo "Usage: make job-import-pdf IMPORT_DIR=/path/to/pdfs HF_REPO_PATH=/path/to/local/lfs-data-root"; \
		exit 2; \
	fi
	$(CLI) job-import-pdf --source-dir "$(IMPORT_DIR)" --ledger-dir $(LEDGER_DIR) --hf-repo-path "$(HF_REPO_PATH)"
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

job-download-pdf:
	$(CLI) job-download-pdf --ledger-dir $(LEDGER_DIR)
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

wrk-download-upload-pdfinfo:
	$(CLI) wrk-download-upload-pdfinfo --ledger-dir $(LEDGER_DIR) --hf-repo-path "$(HF_REPO_PATH)" --verbose
	$(CLI) update-readme-status --ledger-dir $(LEDGER_DIR) --readme-path README.md

sync-hf:
	$(CLI) sync-hf --source-root . --hf-repo-path "$(HF_REPO_PATH)"
