#!/bin/sh

# Default macOS capabilities; the Python application remains on its locked venv.
dev_sync_documents() {
    if "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/document-tools.py" >/dev/null 2>&1; then
        dev_note "Office/PDF/media/OCR toolchain: ready"
        return 0
    fi
    [ "${DEV_OFFLINE:-0}" -eq 0 ] || dev_fail 14 "document toolchain is incomplete offline; run ./dev bootstrap"
    DEV_BREW=$(dev_find_brew) || {
        dev_fail 11 "Homebrew is required; run ./dev bootstrap to review and authorize its installation"
    }
    DEV_DOCUMENT_FORMULAS=$(dev_toml_string "$ROOT_DIR/configuration/dev-toolchain.toml" document_formulas) || dev_fail 2 "missing document_formulas"
    DEV_DOCUMENT_CASKS=$(dev_toml_string "$ROOT_DIR/configuration/dev-toolchain.toml" document_casks) || dev_fail 2 "missing document_casks"
    for DEV_DOCUMENT_FORMULA in $DEV_DOCUMENT_FORMULAS; do
        case "$DEV_DOCUMENT_FORMULA" in *[!a-z0-9-]*) dev_fail 2 "invalid document formula";; esac
        dev_ensure_brew_formula "$DEV_DOCUMENT_FORMULA" || dev_fail 11 "failed to install $DEV_DOCUMENT_FORMULA"
        # Existing formulae can be broken by an ABI change in a shared Homebrew
        # dependency. Preserve working versions, but repair a failed executable.
        case "$DEV_DOCUMENT_FORMULA" in
            ffmpeg) DEV_DOCUMENT_BINARY=ffmpeg; DEV_DOCUMENT_VERSION=-version;;
            imagemagick) DEV_DOCUMENT_BINARY=magick; DEV_DOCUMENT_VERSION=-version;;
            poppler) DEV_DOCUMENT_BINARY=pdftoppm; DEV_DOCUMENT_VERSION=-v;;
            graphviz) DEV_DOCUMENT_BINARY=dot; DEV_DOCUMENT_VERSION=-V;;
            qpdf|pandoc|tesseract) DEV_DOCUMENT_BINARY=$DEV_DOCUMENT_FORMULA; DEV_DOCUMENT_VERSION=--version;;
            *) DEV_DOCUMENT_BINARY=;;
        esac
        if [ -n "$DEV_DOCUMENT_BINARY" ] && ! "$DEV_DOCUMENT_BINARY" "$DEV_DOCUMENT_VERSION" >/dev/null 2>&1; then
            dev_note "Repairing unavailable document executable: $DEV_DOCUMENT_BINARY"
            dev_run_bounded 1800 "Homebrew repair $DEV_DOCUMENT_FORMULA" env HOMEBREW_NO_AUTO_UPDATE=1 "$DEV_BREW" reinstall "$DEV_DOCUMENT_FORMULA" || dev_fail 11 "failed to repair $DEV_DOCUMENT_FORMULA"
        fi
    done
    DEV_DOCUMENT_BREW_PREFIX=$("$DEV_BREW" --prefix) || dev_fail 11 "Homebrew prefix lookup failed"
    case "$DEV_DOCUMENT_BREW_PREFIX" in
        /*) ;;
        *) dev_fail 11 "Homebrew prefix must be a nonempty absolute path";;
    esac
    "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/ocr-models.py" --policy "$ROOT_DIR/configuration/dev-toolchain.toml" --data-dir "$DEV_DOCUMENT_BREW_PREFIX/share/tessdata" || dev_fail 11 "failed to install Chinese OCR models"
    for DEV_DOCUMENT_CASK in $DEV_DOCUMENT_CASKS; do
        case "$DEV_DOCUMENT_CASK" in *[!a-z0-9-]*) dev_fail 2 "invalid document cask";; esac
        if [ "$DEV_DOCUMENT_CASK" = libreoffice ] && command -v soffice >/dev/null 2>&1 && soffice --version >/dev/null 2>&1; then
            dev_note "LibreOffice: preserving healthy existing installation"
            continue
        fi
        "$DEV_BREW" list --cask --versions "$DEV_DOCUMENT_CASK" >/dev/null 2>&1 || \
            dev_run_bounded 1800 "Homebrew $DEV_DOCUMENT_CASK" env HOMEBREW_NO_AUTO_UPDATE=1 "$DEV_BREW" install --cask "$DEV_DOCUMENT_CASK" || dev_fail 11 "failed to install $DEV_DOCUMENT_CASK"
    done
    dev_activate_toolchains
    "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/document-tools.py" || dev_fail 14 "document toolchain failed its native smoke check"
}
