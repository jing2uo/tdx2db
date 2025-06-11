# Makefile for Go CLI tool with TDX data integration and installation targets

# Configuration
TDX_URL       := https://www.tdx.com.cn/products/autoup/cyb/datatool.rar
TMP_DIR       := .tmp
RAR_FILE      := $(TMP_DIR)/datatool.rar
EXTRACT_DIR   := $(TMP_DIR)/extracted
TDX_EMBED_DIR := tdx/embed
BIN_NAME      := tdx2db  # 二进制名称变量化
INSTALL_DIR   := /usr/local/bin
LOCAL_BIN     := $(HOME)/.local/bin

.PHONY: all build check-unrar download extract move_datatool clean sudo-install local-install

all: build

build: check-unrar download extract move_datatool clean-tmp
	@echo "Building Go binary..."
	go build -o $(BIN_NAME)

sudo-install: build
	@echo "Installing system-wide (requires sudo)"
	sudo mkdir -p $(INSTALL_DIR)
	sudo cp $(BIN_NAME) $(INSTALL_DIR)/
	@echo "Installed to $(INSTALL_DIR)/$(BIN_NAME)"

local-install: build
	@echo "Installing to user directory"
	mkdir -p $(LOCAL_BIN)
	cp $(BIN_NAME) $(LOCAL_BIN)/
	@echo "Installed to $(LOCAL_BIN)/$(BIN_NAME)"
	@echo "NOTE: Ensure $(LOCAL_BIN) is in your PATH"

check-unrar:
	@command -v unrar >/dev/null 2>&1 || { echo >&2 "Error: unrar required..."; exit 1; }

download:
	@echo "Downloading TDX data tool..."
	mkdir -p $(TMP_DIR)
	curl -s -L -o $(RAR_FILE) $(TDX_URL) || (echo "Download failed"; exit 1)

extract:
	@echo "Extracting RAR archive..."
	mkdir -p $(EXTRACT_DIR)
	unrar x -o+ $(RAR_FILE) $(EXTRACT_DIR)/ > /dev/null

move_datatool:
	@echo "Moving data tool to embed directory..."
	mkdir -p $(TDX_EMBED_DIR)
	cp $(EXTRACT_DIR)/v4/datatool $(TDX_EMBED_DIR)/

clean-tmp:
	@echo "Cleaning temporary files..."
	rm -rf $(TMP_DIR)

clean:
	@echo "Full cleanup..."
	rm -rf $(TMP_DIR)
	rm -rf $(TDX_EMBED_DIR)/datatool
	rm -f $(BIN_NAME)
