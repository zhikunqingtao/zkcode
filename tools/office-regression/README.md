# Native Office and HTML regression

Run `./dev bootstrap` to install the macOS document, PDF, media, diagram, OCR and Chinese font toolchain, then `./dev test office`. The runner uses the application's locked Python environment and Chromium installation. It runs the complete XLSX/DOCX/PPTX structural and tamper tests, deterministic fixtures, actual LibreOffice-to-PDF conversion, PDF checks and real browser interaction tests.

Evidence is saved to a fresh temporary directory outside the checkout. Set `OFFICE_REGRESSION_OUT` to another empty directory if needed. The runner rejects output inside the repository, symlink aliases into the checkout, occupied directories and existing evidence. It preserves logs, exact exit status and run metadata on failure. No Docker, deployment or publishing service is used. Missing native capabilities fail preflight with actionable diagnostics.

Python versions come from `python-service/requirements.lock`; macOS packages are declared in `configuration/dev-toolchain.toml`. `./dev doctor` reports actual capability readiness. Conversion fixtures run with isolated LibreOffice user profiles and local browser fixtures.
